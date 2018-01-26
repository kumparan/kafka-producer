package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/kumparan/go-lib/log"
)

var kafkaProducer sarama.SyncProducer

type Message struct {
	Topic   string `json:"topic"`
	Content string `json:"content"`
}

func Init(producer *sarama.SyncProducer) {
	kafkaProducer = *producer
	log.Info("kafka producer init")
}

func Publish(msg Message) error {
	log.Infof("Message receive: %v", msg)
	_,_,err := kafkaProducer.SendMessage(&sarama.ProducerMessage{
		Topic: msg.Topic,
		Value: sarama.StringEncoder(msg.Content),
	})
	log.Infof("Message receive: %v", err)
	return err
}
