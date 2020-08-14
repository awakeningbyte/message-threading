package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"
)

func NewConsumerGroup(s Settings) (sarama.ConsumerGroup, error) {
	brokerList := strings.Split(s.Brokers, ",")
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	// config.ClientID = fmt.Sprint("%d", id)
	config.Version, _ = sarama.ParseKafkaVersion("2.1.1")
	return sarama.NewConsumerGroup(brokerList, s.GroupId, config)
}
func NewAsyncProducer(s Settings) sarama.AsyncProducer {
	brokerList := strings.Split(s.Brokers, ",")
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Partitioner = sarama.NewManualPartitioner
	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		log.WithError(err).Fatal("Failed to start order producer")
	}

	return producer
}

type Consumer struct {
	ready   chan bool
	Id      int
	counter chan<- int64
	rdb     *redis.Client
}

// GenerateMessages is Run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is Run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	timeout := time.After(time.Second * 5)
	startCorrelationGroup := make(chan ChatMessage)
	appendToCorrelationGroup := make(chan ChatMessage)
	groupCreationComplete := make(chan struct {groupId string; err error})
	cache := make(map[string][]ChatMessage, 0)
	go func() {
		select {
		case <- timeout: //assert all message consumed, nothing left over
			if len(cache) > 0 {
				panic("there are messages pending for too long")
			}
		case m := <-startCorrelationGroup:
			log.Infof("start group %s", id)
			createGroupContext(m, groupCreationComplete)
			cache[m.CorrelationId] = make([]ChatMessage, 0)
			timeout = time.After(time.Second * 5)
		case m := <-appendToCorrelationGroup:
			q := cache[m.CorrelationId]
			cache[m.CorrelationId] = append(q, m)
			timeout = time.After(time.Second * 5)
		case e := <-groupCreationComplete:
			if e.err!= nil {
				createGroupContext(m, groupCreationComplete)  //retry
			} else {
				flushGroupMessages(cache[e.groupId])
				delete(cache, e.groupId)
			}
		}
	}()
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		start := time.Now()
		var m ChatMessage
		err := json.Unmarshal(message.Value, &m)
		if err != nil {
			panic(err)
		}
		lock := c.rdb.SetNX(m.CorrelationId, m.SeqNum, time.Second*2)
		if lock.Err() != nil {
			panic(lock.Err())
		}

		if lock.Val() == true {
			startCorrelationGroup <- m
		}

		appendToCorrelationGroup <- m

		// log.Printf("ID: %d, Message claimed: value = %s, timestamp = %v, topic = %s", c.Id, string(message.Value) , message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
		elapsed := time.Since(start)
		c.counter <- elapsed.Nanoseconds()
	}

	return nil
}
