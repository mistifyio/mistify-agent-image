package main

import (
	flag "github.com/docker/docker/pkg/mflag"
	"github.com/mistifyio/mistify-agent-image"
	"github.com/mistifyio/mistify-agent/log"
	"os"
	"sync"
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
