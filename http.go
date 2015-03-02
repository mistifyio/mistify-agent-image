// Package imagestore is the HTTP application
package imagestore

import (
	log "github.com/Sirupsen/logrus"
	"github.com/mistifyio/mistify-agent/rpc"
)

// TODO: the core rpc should have some generic ping, logging, and stats handlers

// RunHTTP creates and runs the http server
func (store *ImageStore) RunHTTP(port uint) error {
	s, _ := rpc.NewServer(port)
	if err := s.RegisterService(store); err != nil {
		log.WithField("error", err).Error("could not register snapshot download")
	}
	// Snapshot downloads are streaming application/octet-stream and can't be
	// done through the normal RPC handling
	s.HandleFunc("/snapshots/download", store.DownloadSnapshot)
	return s.ListenAndServe()
}
