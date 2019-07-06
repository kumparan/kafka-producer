package kafka

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type (
	// ProducerCallback :nodoc:
	ProducerCallback func(conn *Producer)

	// Producer :nodoc:
	Producer struct {
		conn     sarama.SyncProducer
		callback ProducerCallback
	}
)

// NewProducer returns a new SyncProducer for give brokers addresses.
func NewProducer(broker string, client string, fn ProducerCallback) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Successes = true
	config.ClientID = client

	kafkaProducer, err := sarama.NewSyncProducer(strings.Split(broker, ","), config)
	if err != nil {
		log.WithFields(log.Fields{
			"client":        client,
			"brokerAddress": broker,
		}).Fatal(err)
	}

	log.WithFields(log.Fields{
		"client":        client,
		"brokerAddress": broker,
	}).Info("Kafka Client connection made...")

	prod := &Producer{
		callback: fn,
	}
	prod.SetProducer(kafkaProducer)
	prod.runCallback()
}

// SetProducer :nodoc:
func (p *Producer) SetProducer(syncProducer sarama.SyncProducer) {
	p.conn = syncProducer
}

func (p *Producer) runCallback() {
	if p.callback != nil {
		p.callback(p)
	}
}

// SendMessage :nodoc:
func (p *Producer) SendMessage(topic string, message interface{}) error {
	msg, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("cannot marshal message %v: %v", message, err)
	}
	_, _, err = p.conn.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg),
	})
	if err != nil {
		return fmt.Errorf("cannot send message %v: %v", message, err)
	}

	return nil
}

// ShutDown connection
func (p *Producer) ShutDown() {
	p.conn.Close()
}
