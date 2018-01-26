package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kumparan/go-lib/flags"
	"github.com/kumparan/go-lib/log"
	"github.com/kumparan/go-lib/webserver"
	"github.com/kumparan/kafka-producer/kafka"
)

type serviceFlags struct {
	logLevel string
	brokers  string
}

func (sf *serviceFlags) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&sf.logLevel, "log_level", "", "logging level")
	fs.StringVar(&sf.brokers, "brokers", "", "broker addresses")
	return fs.Parse(args)
}

func main() {
	sf := serviceFlags{}
	flags.Parse(&sf)
	log.SetLevelString(sf.logLevel)

	brokers := strings.Split(sf.brokers, ",")
	if len(brokers) == 0 {
		log.Fatal("no brokers found")
	}

	producerConcifg := sarama.NewConfig()
	producerConcifg.Producer.RequiredAcks = sarama.WaitForAll
	producerConcifg.Producer.Retry.Max = 10
	producerConcifg.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, producerConcifg)
	if err != nil {
		log.Fatalf("failed to create producer: %s", err.Error())
	}
	kafka.Init(&producer)

	server := webserver.New(webserver.Options{
		Address: ":9000",
		Timeout: time.Second * 2,
	})

	fatalChan := make(chan error)
	go func() {
		fatalChan <- server.Run()
	}()
	registerServiceEndpoints(server.Router())

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-fatalChan:
		log.Fatalf("prgoram exited because: %s", err.Error())
	case <-term:
		log.Info("signal termination detected")
	}
}
