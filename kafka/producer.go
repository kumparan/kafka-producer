package kafka

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/jasonlvhit/gocron"
	"github.com/kumparan/tapao"
	log "github.com/sirupsen/logrus"
)

type (
	// ProducerCallback :nodoc:
	ProducerCallback func(conn *Producer)

	// Producer :nodoc:
	Producer struct {
		client   sarama.Client
		conn     sarama.SyncProducer
		callback ProducerCallback

		wg     *sync.WaitGroup
		stopCh chan struct{}

		workerStatus bool
		workerLock   *sync.Mutex

		opts Options
	}
)

var defaultOptions = Options{
	redisConn:                             nil,
	failedMessagesRedisKey:                "kafka:failed-messages",
	deadMessagesRedisKey:                  "kafka:dead-messages",
	failedMessagePublishIntervalInSeconds: 120,
}

// NewProducer returns a new SyncProducer for give brokers addresses.
func NewProducer(broker string, client string, fn ProducerCallback) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Producer.Return.Successes = true
	config.ClientID = client

	saramaClient, err := sarama.NewClient(strings.Split(broker, ","), config)
	if err != nil {
		log.WithFields(log.Fields{
			"brokerAddress": broker,
		}).Fatal(err)
	}

	kafkaProducer, err := sarama.NewSyncProducerFromClient(saramaClient)
	if err != nil {
		log.WithFields(log.Fields{
			"client":        client,
			"brokerAddress": broker,
		}).Error(err)
	}

	log.WithFields(log.Fields{
		"client":        client,
		"brokerAddress": broker,
	}).Info("Kafka Client connection made...")

	prod := &Producer{
		callback: fn,
		client:   saramaClient,
	}

	prod.SetProducer(kafkaProducer)
	prod.runCallback()
}

// SetProducer :nodoc:
func (p *Producer) SetProducer(syncProducer sarama.SyncProducer) {
	p.conn = syncProducer

	if p.opts.redisConn != nil {
		s := gocron.NewScheduler()
		s.Every(p.opts.failedMessagePublishIntervalInSeconds).Seconds().Do(p.publishFailedMessageFromRedis)

		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			c := s.Start()
			select {
			case <-p.stopCh:
				close(c)
			}
		}()
	}
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
		if p.opts.redisConn != nil {
			// Push to redis if failed
			client := p.opts.redisConn.Get()
			defer client.Close()

			b, err := tapao.Marshal(&kafkaMessageWithSubject{
				Topic:   topic,
				Message: sarama.ByteEncoder(msg),
			})
			if err != nil {
				return err
			}

			_, err = redigo.Int(client.Do("RPUSH", p.opts.failedMessagesRedisKey, b))
			if err != nil {
				log.Error("failed to RPUSH to redis. redis connection problem")
				return err
			}
		} else {
			return fmt.Errorf("cannot send message %v: %v", message, err)
		}
	}

	return nil
}

// ShutDown connection
func (p *Producer) ShutDown() {
	p.conn.Close()
}

func (p *Producer) checkWorkerStatus() bool {
	p.workerLock.Lock()
	p.workerLock.Unlock()
	return p.workerStatus
}

func (p *Producer) setWorkerStatus(b bool) {
	p.workerLock.Lock()
	p.workerStatus = b
	p.workerLock.Unlock()
}

func (p *Producer) publishFailedMessageFromRedis() {
	isRun := p.checkWorkerStatus()
	if isRun {
		log.Error("worker is already running")
		return
	}
	p.setWorkerStatus(true)
	defer p.setWorkerStatus(false)

	client := p.opts.redisConn.Get()
	defer client.Close()

	for {
		b, err := redigo.Bytes(client.Do("LPOP", p.opts.failedMessagesRedisKey))
		if err != nil && err != redigo.ErrNil {
			log.Error("failed to LPOP from redis. redis connection problem")
			return
		}

		if len(b) == 0 {
			return
		}

		msg := new(kafkaMessageWithSubject)
		err = tapao.Unmarshal(b, &msg, tapao.FallbackWith(tapao.JSON))
		if err == nil {
			if len(p.client.Brokers()) > 0 {
				err = p.SendMessage(msg.Topic, msg.Message)
				if err == nil {
					continue
				}
			}
		}

		if err != nil {
			log.Error("Error : ", err)
		}

		_, err = client.Do("RPUSH", p.opts.deadMessagesRedisKey, b)
		if err != nil {
			log.Error("failed to RPUSH to dead messages key. redis connection problem")
			return
		}
	}
}
