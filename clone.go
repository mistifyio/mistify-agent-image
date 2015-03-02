package imagestore

import (
	"fmt"

	"gopkg.in/mistifyio/go-zfs.v1"
)

type (
	cloneRequest struct {
		name     string
		source   string
		dest     string
		response chan *cloneResponse
	}

	cloneResponse struct {
		err     error
		dataset *zfs.Dataset
	}

	cloneWorker struct {
		timeToDie chan struct{}
		requests  chan *cloneRequest
		store     *ImageStore
	}
)

func newCloneWorker(store *ImageStore) *cloneWorker {
	return &cloneWorker{
		timeToDie: make(chan struct{}),
		requests:  make(chan *cloneRequest),
		store:     store,
	}
}

func (c *cloneWorker) Exit() {
	var q struct{}
	c.timeToDie <- q
}

var defaultZfsOptions = map[string]string{
	"compression": "lz4",
}

func (c *cloneWorker) Clone(source, dest string) (*zfs.Dataset, error) {
	request := &cloneRequest{
		source:   source,
		dest:     dest,
		response: make(chan *cloneResponse),
	}

	fmt.Printf("Clone: request: %+v\n", request)
	c.requests <- request

	response := <-request.response

	fmt.Printf("Clone: response:  %+v\n", response)

	return response.dataset, response.err
}

func (c *cloneWorker) Run() {
	go func() {
		for {
			select {
			case <-c.timeToDie:
				return
			case req := <-c.requests:
				fmt.Printf("Run:  request: %+v\n", req)
				response := &cloneResponse{}

				s, err := zfs.GetDataset(req.source)
				if err != nil {
					response.err = err
				} else {
					d, err := s.Clone(req.dest, defaultZfsOptions)
					response.err = err
					response.dataset = d
				}
				req.response <- response
			}
		}
	}()
}
