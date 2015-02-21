package main

import (
	"os"
	"sync"

	"github.com/mistifyio/mistify-agent-image"
	"github.com/mistifyio/mistify-agent/log"
	flag "github.com/spf13/pflag"
)

func main() {
	var zpool, logLevel string
	var port uint
	var h bool

	flag.BoolVarP(&h, "help", "h", false, "display the help")
	flag.UintVarP(&port, "port", "p", 19999, "listen port")
	flag.StringVarP(&zpool, "zpool", "z", "mistify", "zpool")
	flag.StringVarP(&logLevel, "log-level", "l", "warning", "log level: debug/info/warning/error/critical/fatal")
	flag.Parse()

	if h {
		flag.PrintDefaults()
		os.Exit(0)
	}

	if err := log.SetLogLevel(logLevel); err != nil {
		log.Fatal(err)
	}

	store, err := imagestore.Create(imagestore.Config{
		Zpool: zpool,
	})
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(store.RunHTTP(port))
	}()

	wg.Wait()
}
