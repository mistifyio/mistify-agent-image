package main

import (
	"github.com/mistifyio/mistify-agent-image"
	"github.com/mistifyio/mistify-agent/log"
	"sync"
)

func main() {
	// TODO: flags/env set these
	zpool := "guests"
	port := 16000

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
