package main

import (
	"github.com/mistifyio/mistify-agent-image"
	"github.com/mistifyio/mistify-agent/log"
	flag "github.com/docker/docker/pkg/mflag"
	"sync"
	"fmt"
)

const (
	DEFAULT_ZPOOL = "guests"
	DEFAULT_PORT = 16000
	DEFAULT_LOG_LEVEL = "warning"
)

var (
	zpool = DEFAULT_ZPOOL
	port = DEFAULT_PORT
	logLevel = DEFAULT_LOG_LEVEL
	help bool
)

func init() {
	flag.StringVar(&zpool, []string{"z", "-zpool"}, DEFAULT_ZPOOL, "pool name")
	flag.IntVar(&port, []string{"p", "-port"}, DEFAULT_PORT, "port to listen on")
	flag.StringVar(&logLevel, []string{"l", "-log-level"}, DEFAULT_LOG_LEVEL, "log level: debug/info/warning/error/critical/fatal")
	flag.BoolVar(&help, []string{"h", "-help"}, false, "display help/usage")

	flag.Parse()
}

func main() {
	if help {
		// help requested, display and bail out
		flag.PrintDefaults()
	} else {
		fmt.Printf("Using zpool '%s'\n", zpool)
		fmt.Printf("Using port %d\n", port)

		// set log level, or default
		logOk := log.SetLogLevel(logLevel)
		if logOk != nil {
			fmt.Printf("%s, defaulting to '%s'\n", logOk, DEFAULT_LOG_LEVEL)
			log.SetLogLevel(DEFAULT_LOG_LEVEL)
		}

		// start
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
}
