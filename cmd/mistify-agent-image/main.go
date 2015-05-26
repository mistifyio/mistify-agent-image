package main

import (
	"net"
	"strconv"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/mistifyio/mistify-agent-image"
	logx "github.com/mistifyio/mistify-logrus-ext"
	flag "github.com/spf13/pflag"
)

func main() {
	var zpool, imageServer, logLevel string
	var port uint

	flag.UintVarP(&port, "port", "p", 19999, "listen port")
	flag.StringVarP(&zpool, "zpool", "z", "mistify", "zpool")
	flag.StringVarP(&logLevel, "log-level", "l", "warning", "log level: debug/info/warning/error/critical/fatal")
	flag.StringVarP(&imageServer, "image-service", "i", "images.service.lochness.local", "image service. srv query used to find port if not specified")
	flag.Parse()

	if err := logx.DefaultSetup(logLevel); err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"func":  "logx.DefaultSetup",
		}).Fatal("failed to set up logrus")
	}

	// Parse image service and do any necessary lookups
	// Using strings.Split instead of net.SplitHostPort since the latter errors
	// it no port is present and it doesn't provide any error type checking
	// convenience methods
	imageServerParts := strings.Split(imageServer, ":")
	partsLength := len(imageServerParts)
	// Empty or too many colons
	if partsLength == 0 || partsLength > 2 {
		log.WithField("imageServer", imageServer).Fatal("invalid image-service value")
	}

	// Try to lookup port if only host/service is provided
	if partsLength == 1 || imageServerParts[1] == "" {
		_, addrs, err := net.LookupSRV("", "", imageServer)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"func":  "net.LookupSRV",
			}).Fatal("srv lookup failed")
		}
		if len(addrs) == 0 {
			log.WithField("imageServer", imageServer).Fatal("invalid image-service value")
		}
		imageServerParts[1] = strconv.FormatUint(uint64(addrs[0].Port), 10)
	}
	imageServer = net.JoinHostPort(imageServerParts[0], imageServerParts[1])

	store, err := imagestore.Create(imagestore.Config{
		ImageServer: imageServer,
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
