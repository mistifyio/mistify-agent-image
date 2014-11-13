package imagestore

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"

	"gopkg.in/mistifyio/go-zfs.v1"
)

type (
	fetchRequest struct {
		name     string
		source   string
		dest     string
		tempdir  string
		response chan *fetchResponse
	}

	fetchResponse struct {
		err      error
		dataset  *zfs.Dataset
		snapshot *zfs.Dataset
	}

	fetchWorker struct {
		timeToDie chan struct{}
		requests  chan *fetchRequest
		store     *ImageStore
	}
)

func newFetchWorker(store *ImageStore, requests chan *fetchRequest) *fetchWorker {
	return &fetchWorker{
		timeToDie: make(chan struct{}),
		store:     store,
		requests:  requests,
	}
}

func (f *fetchWorker) Exit() {
	var q struct{}
	f.timeToDie <- q
}

func (f *fetchWorker) Fetch(req *fetchRequest) (*fetchResponse, error) {
	cache := filepath.Join(req.tempdir, req.name)

	_, err := os.Stat(cache)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		temp, err := ioutil.TempFile(req.tempdir, fmt.Sprintf(".image.%s.gz", req.name))
		if err != nil {
			return nil, err
		}
		defer temp.Close()
		defer os.Remove(temp.Name())

		resp, err := http.Get(req.source)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("got %d", resp.StatusCode)
		}

		defer resp.Body.Close()

		_, err = io.Copy(temp, resp.Body)
		if err != nil {
			return nil, err
		}
		if err := temp.Close(); err != nil {
			return nil, err
		}

		if err := os.Rename(temp.Name(), cache); err != nil {
			return nil, err
		}
	}

	// TODO: checksum, etc
	fi, err := os.Open(cache)
	if err != nil {
		return nil, err
	}
	defer fi.Close()

	z, err := gzip.NewReader(fi)

	dataset, err := zfs.ReceiveSnapshot(z, req.dest)
	if err != nil {
		return nil, err
	}

	//we can remove the file now?
	os.Remove(cache)

	snapshots, err := dataset.Snapshots()

	if err != nil {
		return nil, err
	}

	return &fetchResponse{
		dataset:  dataset,
		snapshot: snapshots[0],
	}, nil
}

func (f *fetchWorker) Run() {
	go func() {
		for {
			select {
			case <-f.timeToDie:
				return
			case req := <-f.requests:
				resp, err := f.Fetch(req)
				if err != nil {
					resp = &fetchResponse{err: err}
				}
				req.response <- resp
			}
		}
	}()
}
