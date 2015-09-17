package imagestore

import (
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/mistifyio/mistify-agent/rpc"
	"github.com/tylerb/graceful"
)

// TODO: the core rpc should have some generic ping, logging, and stats handlers

// RunHTTP creates and runs the http server
func (store *ImageStore) RunHTTP(port uint) *graceful.Server {
	s, _ := rpc.NewServer(port)
	if err := s.RegisterService(store); err != nil {
		log.WithField("error", err).Error("could not register snapshot download")
	}
	// Snapshot downloads are streaming application/octet-stream and can't be
	// done through the normal RPC handling
	s.HandleFunc("/snapshots/download", store.DownloadSnapshot)

	server := &graceful.Server{
		Timeout: 5 * time.Second,
		Server:  s.HTTPServer,
	}
	go server.ListenAndServe()
	return server
}

func listenAndServe(server *graceful.Server) {
	if err := server.ListenAndServe(); err != nil {
		// Ignore the error from closing the listener, which is involved in the
		// graceful shutdown
		if !strings.Contains(err.Error(), "use of closed network connection") {
			log.WithField("error", err).Fatal("server error")
		}
	}
}
