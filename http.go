// Package app is the HTTP application
package imagestore

import (
	"github.com/mistifyio/mistify-agent/rpc"
)

// TODO: the core rpc should have some generic ping, logging, and stats handlers

func (store *ImageStore) RunHTTP(port uint) error {
	s, _ := rpc.NewServer(port)
	s.RegisterService(store)
	// Snapshot downloads are streaming application/octet-stream and can't be
	// done through the normal RPC handling
	s.HandleFunc("/snapshots/download", store.DownloadSnapshot)
	return s.ListenAndServe()
}
