package main

import (
	"sync"

	log "github.com/Sirupsen/logrus"
	imagestore "github.com/mistifyio/mistify-agent-image"
	logx "github.com/mistifyio/mistify-logrus-ext"
	flag "github.com/spf13/pflag"
)

func main() {
	var zpool, imageService, logLevel string
	var port uint

	flag.UintVarP(&port, "port", "p", 19999, "listen port")
	flag.StringVarP(&zpool, "zpool", "z", "mistify", "zpool")
	flag.StringVarP(&logLevel, "log-level", "l", "warning", "log level: debug/info/warning/error/critical/fatal")
	flag.StringVarP(&imageService, "image-service", "i", "image.services.lochness.local", "image service. srv query used to find port if not specified")
	flag.Parse()

	if err := logx.DefaultSetup(logLevel); err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"func":  "logx.DefaultSetup",
		}).Fatal("failed to set up logrus")
	}

	store, err := imagestore.Create(imagestore.Config{
		ImageServer: imageService,
		Zpool:       zpool,
	})
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"func":  "imagestore.Create",
		}).Fatal(err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		store.Run()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		server := store.RunHTTP(port)
		// Block until the server is stopped
		<-server.StopChan()
	}()

	wg.Wait()
}
