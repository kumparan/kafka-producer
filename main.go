package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kumparan/go-lib/log"
	"github.com/kumparan/go-lib/webserver"
)

func main() {
	server := webserver.New(webserver.Options{
		Address: ":9000",
		Timeout: time.Second * 2,
	})

	fatalChan := make(chan error)
	go func() {
		fatalChan <- server.Run()
	}()

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-fatalChan:
		log.Fatalf("prgoram exited because: %s", err.Error())
	case <-term:
		log.Info("signal termination detected")
	}
}
