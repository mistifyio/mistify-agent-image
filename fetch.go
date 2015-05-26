package imagestore

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/mistifyio/go-zfs"
	"github.com/mistifyio/mistify-agent/rpc"
)

type (
	// fetchRequest contains information needed to fetch and store an image
	fetchRequest struct {
		name     string
		source   string
		dest     string
		tempdir  string
		response chan *fetchResponse
	}

	// fetchResponse contains the results of fetching an image
	fetchResponse struct {
		err      error
		dataset  *zfs.Dataset
		snapshot *zfs.Dataset
	}

	// fetcher fetches images. It shares a response with fetch requests for the
	// same image and handles the maximum concurrent unique image fetch requests
	fetcher struct {
		store           *ImageStore
		concurrentChan  chan struct{}
		quitChan        chan struct{}
		pendingRequests chan *fetchRequest

		lock            sync.Mutex
		currentRequests map[string][]*fetchRequest
	}

	// ErrorHTTPCode should be used for errors resulting from an http response
	// code not matching the expected code
	ErrorHTTPCode struct {
		Expected int
		Code     int
		Source   string
	}
)

// Error returns a string error message
func (e ErrorHTTPCode) Error() string {
	return fmt.Sprintf("unexpected http response code: expected %d, received %d, url: %s", e.Expected, e.Code, e.Source)
}

// newFetcher creates a new fetcher
func newFetcher(store *ImageStore, maxPending, concurrency uint) *fetcher {
	if concurrency <= 0 {
		concurrency = 5
	}

	f := &fetcher{
		store:           store,
		concurrentChan:  make(chan struct{}, concurrency),
		quitChan:        make(chan struct{}),
		pendingRequests: make(chan *fetchRequest, maxPending),
		currentRequests: make(map[string][]*fetchRequest),
	}

	// Fill concurrencyChan
	for i := uint(0); i < concurrency; i++ {
		f.concurrentChan <- struct{}{}
	}

	return f
}

// download fetches an external image to the local machine
func (f *fetcher) download(req *fetchRequest, dest string) error {
	temp, err := ioutil.TempFile(req.tempdir, req.name)
	if err != nil {
		return err
	}
	// In case of a failure, remove the temp file
	successfulDownload := false
	defer func() {
		if !successfulDownload {
			if err := os.Remove(temp.Name()); err != nil {
				log.WithFields(log.Fields{
					"error":    err,
					"filename": temp.Name(),
				}).Error("could not remove temp file")
			}
		}
	}()
	defer temp.Close()

	resp, err := http.Get(req.source)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ErrorHTTPCode{
			Expected: http.StatusOK,
			Code:     resp.StatusCode,
			Source:   req.source,
		}
	}

	if _, err = io.Copy(temp, resp.Body); err != nil {
		return err
	}

	if err := temp.Close(); err != nil {
		return err
	}

	if err := os.Rename(temp.Name(), dest); err != nil {
		return err
	}
	successfulDownload = true
	return nil
}

// importImage takes an image snapshot and imports it to zfs
func (f *fetcher) importImage(req *fetchRequest) *fetchResponse {
	fetchResp := &fetchResponse{}

	filename := filepath.Join(req.tempdir, req.name)

	// Open and unzip the image
	cachedFile, err := os.Open(filename)
	if err != nil {
		fetchResp.err = err
		return fetchResp
	}
	defer cachedFile.Close()

	// Use a response buffer so the first few bytes can be peeked at for file
	// type detection. Uncompress the image if it is gzipped
	fileBuffer := bufio.NewReader(cachedFile)
	var cacheFileReader io.Reader = fileBuffer

	filetypeBytes, err := fileBuffer.Peek(512)
	if err != nil {
		fetchResp.err = err
		return fetchResp
	}

	if http.DetectContentType(filetypeBytes) == "application/x-gzip" {
		gzipReader, err := gzip.NewReader(fileBuffer)
		if err != nil {
			fetchResp.err = err
			return fetchResp
		}
		defer gzipReader.Close()
		cacheFileReader = gzipReader
	}

	// Import the image
	dataset, err := zfs.ReceiveSnapshot(cacheFileReader, req.dest)
	if err != nil {
		fetchResp.err = err
		return fetchResp
	}

	// Remove the cached file
	if err := os.Remove(filename); err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"filename": filename,
		}).Error("could not remove cache file")
	}

	// Build the response
	fetchResp.dataset = dataset
	snapshots, err := dataset.Snapshots()
	if err != nil {
		fetchResp.err = err
		return fetchResp
	}
	fetchResp.snapshot = snapshots[0]

	return fetchResp
}

// fetchImage downloads and imports an image
func (f *fetcher) fetchImage(req *fetchRequest) {
	log.WithField("req", req).Debug("waiting on concurrent slot")
	// Wait until there's an open request slot to limit concurrent fetches
	select {
	case q := <-f.quitChan:
		f.quitChan <- q
		return
	case <-f.concurrentChan:
	}
	defer func() { f.concurrentChan <- struct{}{} }()
	log.WithField("req", req).Debug("beginning fetch")

	// Check for a cached image
	cachedFilename := filepath.Join(req.tempdir, req.name)
	_, err := os.Stat(cachedFilename)

	// Download the image if a cached file wasn't found
	if err != nil {
		fetchResp := &fetchResponse{}

		if !os.IsNotExist(err) {
			fetchResp.err = err
			f.shareResponse(req.name, fetchResp)
			return
		}

		log.WithField("req", req).Debug("download image")
		if err := f.download(req, cachedFilename); err != nil {
			fetchResp.err = err
			f.shareResponse(req.name, fetchResp)
			return
		}
	}

	// Import the file into zfs
	log.WithField("req", req).Debug("import image")
	fetchResp := f.importImage(req)

	// Save the image information
	if fetchResp.err == nil {
		image := &rpc.Image{
			Id:       req.name,
			Volume:   fetchResp.dataset.Name,
			Snapshot: fetchResp.snapshot.Name,
			Size:     fetchResp.snapshot.Volsize / 1024 / 1024,
			Status:   "complete",
		}

		if err := f.store.saveImage(image); err != nil {
			fetchResp.err = err
		}
	}

	log.WithField("req", req).Debug("return response")
	f.shareResponse(req.name, fetchResp)
}

// shareResponse shares a response with all similar waiting requests and then
// cleans up
func (f *fetcher) shareResponse(name string, resp *fetchResponse) {
	f.lock.Lock()
	defer f.lock.Unlock()

	// Give all related requests the response
	for _, req := range f.currentRequests[name] {
		req.response <- resp
	}

	delete(f.currentRequests, name)
}

// process decides whether a request can share the response of an in-progress
// request for the same image or kicks off a new fetch
func (f *fetcher) process(req *fetchRequest) {
	f.lock.Lock()
	defer f.lock.Unlock()

	requests, ok := f.currentRequests[req.name]
	if ok {
		// A request for this already under way. Wait with the rest for a
		// response
		log.WithField("req", req).Debug("appended to existing request")
		f.currentRequests[req.name] = append(requests, req)
		return
	}
	// Completely new request
	log.WithField("req", req).Debug("new request")
	f.currentRequests[req.name] = []*fetchRequest{req}
	go f.fetchImage(req)
}

// fetch adds a new request to the fetcher
func (f *fetcher) fetch(req *fetchRequest) *fetchResponse {
	req.response = make(chan *fetchResponse, 1)
	log.WithField("req", req).Debug("added to pending request chan")
	f.pendingRequests <- req
	return <-req.response
}

// run starts the processing of fetch requests
func (f *fetcher) run() {
	go func() {
		for {
			select {
			case q := <-f.quitChan:
				// Stick it back in so anything else looking at the quitChan can
				// stop
				f.quitChan <- q
				// Stop taking new requests
				close(f.pendingRequests)
				// Close out pending requests
				for req := range f.pendingRequests {
					req.response <- &fetchResponse{err: errors.New("fetcher quit")}
				}
				return
			case req := <-f.pendingRequests:
				log.WithField("req", req).Debug("pending request received")
				// Process the request
				f.process(req)
			}
		}
	}()
}

// stop halts processing
func (f *fetcher) exit() {
	f.quitChan <- struct{}{}
}
