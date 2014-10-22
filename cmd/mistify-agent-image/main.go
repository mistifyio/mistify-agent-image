package main

import (
	flag "github.com/docker/docker/pkg/mflag"
	"github.com/mistifyio/mistify-agent-image"
	"github.com/mistifyio/mistify-agent/log"
	"os"
	"sync"
)

func main() {
	zpool := "guests"
	port := 16000
	h := false

	flag.BoolVar(&h, []string{"h", "#help", "-help"}, false, "display the help")
	flag.IntVar(&port, []string{"p", "#port", "-port"}, 19999, "listen port")
	flag.StringVar(&zpool, []string{"z", "#zpool", "-zpool"}, "mistify", "zpool")
	flag.Parse()

	if h {
		flag.PrintDefaults()
		os.Exit(0)
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
