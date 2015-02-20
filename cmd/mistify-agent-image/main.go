package main

import (
	"os"
	"sync"

	log "github.com/Sirupsen/logrus"
	flag "github.com/docker/docker/pkg/mflag"
	"github.com/mistifyio/mistify-agent-image"
)

func main() {
	var zpool, logLevel string
	var port uint
	var h bool

	flag.BoolVar(&h, []string{"h", "#help", "-help"}, false, "display the help")
	flag.UintVar(&port, []string{"p", "#port", "-port"}, 19999, "listen port")
	flag.StringVar(&zpool, []string{"z", "#zpool", "-zpool"}, "mistify", "zpool")
	flag.StringVar(&logLevel, []string{"l", "-log-level"}, "warning", "log level: debug/info/warning/error/critical/fatal")
	flag.Parse()

	if h {
		flag.PrintDefaults()
		os.Exit(0)
	}

	log.SetFormatter(&log.JSONFormatter{})
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"func":  "log.ParseLevel",
		}).Fatal(err)
	}
	log.SetLevel(level)

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
		if err = imagestore.store.RunHTTP(port); err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"func":  "imagestore.ImageStore.RunHTTP",
			}).Fatal(err)
		}
	}()

	wg.Wait()
}
