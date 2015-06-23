package main

import (
	"fmt"
	"net"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/mistifyio/lochness/pkg/hostport"
	"github.com/mistifyio/mistify-agent-image"
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

	// Parse image service and do any necessary lookups
	iHost, iPort, err := hostport.Split(imageService)
	if err != nil {
		log.WithFields(log.Fields{
			"error":        err,
			"imageService": imageService,
			"func":         "hostport.Split",
		}).Fatal("host port split failed")
	}

	// Try to lookup port if only host/service is provided
	if iPort == "" {
		_, addrs, err := net.LookupSRV("", "", iHost)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"func":  "net.LookupSRV",
			}).Fatal("srv lookup failed")
		}
		if len(addrs) == 0 {
			log.WithField("imageService", iHost).Fatal("invalid host value")
		}
		iPort = fmt.Sprintf("%d", addrs[0].Port)
	}
	imageService = net.JoinHostPort(iHost, iPort)

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
		if err = store.RunHTTP(port); err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"func":  "imagestore.ImageStore.RunHTTP",
			}).Fatal(err)
		}
	}()

	wg.Wait()
}
