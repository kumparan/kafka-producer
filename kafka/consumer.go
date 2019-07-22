package kafka

import (
	"context"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type (
	// ConsumerCallback :nodoc:
	ConsumerCallback func(conn *Consumer)

	// Consumer :nodoc:
	Consumer struct {
		client sarama.Client
		info   *consumerInfo
	}

	// consumerInfo contains informations that will be use info to kafka
	consumerInfo struct {
		callback ConsumerCallback
	}

	// ConsumerGroup :represents a Sarama consumer group consumer:
	ConsumerGroup struct {
		Consumer sarama.ConsumerGroup
		callback MessageCallback
	}

	// MessageCallback :nodoc:
	MessageCallback func(msg *sarama.ConsumerMessage)
)

// NewConsumerClient :nodoc:
func NewConsumerClient(brokerAddress string, fn ConsumerCallback) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Consumer.Return.Errors = true

	client, err := sarama.NewClient(strings.Split(brokerAddress, ","), config)
	if err != nil {
		log.WithFields(log.Fields{
			"brokerAddress": brokerAddress,
		}).Error(err)
		return err
	}

	log.WithFields(log.Fields{
		"brokerAddress": brokerAddress,
	}).Info("Kafka Client connection made...")

	cons := &Consumer{}
	cons.info = &consumerInfo{
		callback: fn,
	}

	cons.SetClient(client)
	cons.runCallback()

	return nil
}

// SetClient :nodoc:
func (c *Consumer) SetClient(clientConn sarama.Client) {
	c.client = clientConn
}

func (c *Consumer) runCallback() {
	if c.info != nil && c.info.callback != nil {
		c.info.callback(c)
	}
}

// NewConsumerGroup runs the process of consuming. It is blocking.
func (c *Consumer) NewConsumerGroup(consumerGroupName string, topic string, fn MessageCallback) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Consumer.Return.Errors = true

	// Start a new consumer group
	group, err := sarama.NewConsumerGroupFromClient(consumerGroupName, c.client)
	if err != nil {
		log.WithFields(log.Fields{
			"consumerGroupName": consumerGroupName,
			"client":            c.client,
		}).Fatal(err)
	}

	// Track errors
	go func() {
		for err := range group.Errors() {
			log.WithFields(log.Fields{
				"consumerGroupName": consumerGroupName,
			}).Fatal(err)
		}
	}()

	runConsumerGroup(topic, group, fn)
	return nil
}

// ConsumeClaim Consumergroup :nodoc:
func (cg ConsumerGroup) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		cg.runCallback(msg)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func (cg ConsumerGroup) runCallback(msg *sarama.ConsumerMessage) {
	if cg.callback != nil {
		cg.callback(msg)
	}
}

func runConsumerGroup(topic string, provider sarama.ConsumerGroup, fn MessageCallback) {
	ctx := context.Background()
	for {
		topics := []string{topic}
		handler := ConsumerGroup{
			callback: fn,
		}
		err := provider.Consume(ctx, topics, handler)
		if err != nil {
			log.WithFields(log.Fields{
				"topics": topics,
			}).Error(err)
		}
	}
}

// Setup Consumergroup :nodoc:
func (cg ConsumerGroup) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup Consumergroup :nodoc:
func (cg ConsumerGroup) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ShutDown :nodoc:
func (cg *ConsumerGroup) ShutDown() error {
	err := cg.Consumer.Close()
	if err != nil {
		return err
	}

	return nil
}
