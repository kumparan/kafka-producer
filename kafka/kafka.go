package kafka

import (
	"github.com/Shopify/sarama"
)

var kafkaProducer *sarama.SyncProducer

type Message struct {
	Topic   string `json:"topic"`
	Content string `json:"content"`
}

func Init(producer *sarama.SyncProducer) {
	kafkaProducer = producer
}

func Publish(msg Message) error {
	return nil
}
