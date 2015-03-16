package main

import (
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/mistifyio/mistify-agent-image"
	logx "github.com/mistifyio/mistify-logrus-ext"
	flag "github.com/spf13/pflag"
)

func main() {
	var zpool, logLevel string
	var port uint

	flag.UintVarP(&port, "port", "p", 19999, "listen port")
	flag.StringVarP(&zpool, "zpool", "z", "mistify", "zpool")
	flag.StringVarP(&logLevel, "log-level", "l", "warning", "log level: debug/info/warning/error/critical/fatal")
	flag.Parse()

	err := logx.DefaultSetup(logLevel)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"func":  "log.ParseLevel",
		}).Fatal("failed to set log level")
	}

	store, err := imagestore.Create(imagestore.Config{
		Zpool: zpool,
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
