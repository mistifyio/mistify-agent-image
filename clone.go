package imagestore

import (
	log "github.com/Sirupsen/logrus"
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

var defaultZFSOptions = map[string]string{
	"compression": "lz4",
}

func (c *cloneWorker) Clone(source, dest string) (*zfs.Dataset, error) {
	request := &cloneRequest{
		source:   source,
		dest:     dest,
		response: make(chan *cloneResponse),
	}

	log.WithFields(log.Fields{
		"request": request,
	}).Info("clone request")
	c.requests <- request

	response := <-request.response
	log.WithFields(log.Fields{
		"request":  request,
		"response": response,
	}).Info("clone response")

	return response.dataset, response.err
}

func (c *cloneWorker) Run() {
	go func() {
		for {
			select {
			case <-c.timeToDie:
				return
			case req := <-c.requests:
				log.WithFields(log.Fields{
					"request": req,
				}).Info("clone response")

				response := &cloneResponse{}

				s, err := zfs.GetDataset(req.source)
				if err != nil {
					response.err = err
				} else {
					d, err := s.Clone(req.dest, defaultZFSOptions)
					response.err = err
					response.dataset = d
				}
				req.response <- response
			}
		}
	}()
}
