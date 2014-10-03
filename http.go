// Package app is the HTTP application
package imagestore

import (
	"github.com/mistifyio/mistify-agent/rpc"
)

// TODO: the core rpc should have some generic ping, logging, and stats handlers

func (store *ImageStore) RunHTTP(port int) error {
	s, _ := rpc.NewServer(port)
	s.RegisterService(store)
	return s.ListenAndServe()
}
