package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sort"
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
	groupCreationTimeout := time.After(time.Second * 5)
	passThroughMessageInterval := time.After(time.Millisecond * 500)
	startCorrelationGroup := make(chan ChatMessage)
	appendToCorrelationGroup := make(chan ChatMessage)
	groupCreationComplete := make(chan struct {
		correlatedId string
		groupId      *string
		err          error
	})
	sequentialProcess := make(chan struct {
		groupId string
		m       ChatMessage
	})
	messageThreadsPendingGroup := make(map[string][]ChatMessage, 0)
	messageThreadsGrouped := make(map[string][]ChatMessage, 0)
	go func() {
		for {
			e := <-groupCreationComplete
			if e.err != nil {
				log.Infof("group created failed %s: %s, retry", *e.groupId, e.correlatedId)
				c.rdb.Del("lock-"+e.correlatedId)
				c.createGroupContext(*e.groupId, groupCreationComplete) // retry
			} else {
				log.Infof("group created %s: %s", *e.groupId, e.correlatedId)
				r := c.rdb.SetNX(e.correlatedId, *e.groupId, 10 * time.Second)
				if r.Err() != nil {
					panic(r.Err())
				}
				flushMessages(*e.groupId, messageThreadsPendingGroup[e.correlatedId])
				delete(messageThreadsPendingGroup, *e.groupId)
			}
		}
	}()

	go func() {
		for {
			if session.Context().Err() != nil {
				if len(messageThreadsGrouped) > 0 {
					for k, v := range messageThreadsGrouped {
						flushMessages(k, v)
					}
				}
				if len(messageThreadsPendingGroup) > 0 {
					log.Errorf("consumer shut down with %d groups pending creation", len(messageThreadsPendingGroup))
				}

				log.Infof("consumer done.")
			}
			defer close(groupCreationComplete)
			select {
			case <-groupCreationTimeout: // assert all message consumed, nothing left over
				log.Info("group creation timeout")
				if len(messageThreadsPendingGroup) > 0 {
					panic("there are messages pending for too long")
				}
				groupCreationTimeout = time.After(time.Second * 5)
			case <-passThroughMessageInterval:
				log.Info("passThroughMessageInterval triggeredS")
				for k, v := range messageThreadsGrouped {
					flushMessages(k, v)
				}
				messageThreadsGrouped = make(map[string][]ChatMessage, 0)
				passThroughMessageInterval = time.After(time.Millisecond * 500)
			case m := <-startCorrelationGroup:
				log.Infof("start group %s", m.CorrelationId)
				messageThreadsPendingGroup[m.CorrelationId] = make([]ChatMessage, 0)
				c.createGroupContext(m.CorrelationId, groupCreationComplete)
				groupCreationTimeout = time.After(time.Second * 5)
			case m := <-appendToCorrelationGroup:
				log.Infof("append message %s: %d", m.CorrelationId, m.SeqNum)
				q := messageThreadsPendingGroup[m.CorrelationId]
				messageThreadsPendingGroup[m.CorrelationId] = append(q, m)
				groupCreationTimeout = time.After(time.Second * 5)
			case g := <-sequentialProcess:
				_, ok := messageThreadsGrouped[g.groupId]
				if !ok {
					messageThreadsGrouped[g.groupId] = make([]ChatMessage, 0)
				}
				messageThreadsGrouped[g.groupId] = append(messageThreadsGrouped[g.groupId], g.m)
			}
		}
	}()

	for message := range claim.Messages() {
		start := time.Now()
		var m ChatMessage
		err := json.Unmarshal(message.Value, &m)
		if err != nil {
			panic(err)
		}

		// check if correlation group is already existing
		groupId, err := c.rdb.Get(m.CorrelationId).Result()
		if err == redis.Nil {
			startCorrelationGroup <- m
			appendToCorrelationGroup <- m

		} else if err != nil {
			panic(err)
		} else {
			sequentialProcess <- struct {
				groupId string
				m       ChatMessage
			}{groupId: groupId, m: m}
		}

		// log.Printf("ID: %d, Message claimed: value = %s, timestamp = %v, topic = %s", c.Id, string(message.Value) , message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
		elapsed := time.Since(start)
		c.counter <- elapsed.Nanoseconds()
	}

	return nil
}

func flushMessages(id string, messages []ChatMessage) {
	sort.SliceStable(messages, func(i, j int) bool {
		return messages[i].SeqNum < messages[j].SeqNum
	})

	filename := path.Join(".", "output", id)
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	for _, m := range messages {
		if _, err := f.WriteString(fmt.Sprintf("%s:%d", m.CorrelationId, m.SeqNum)); err != nil {
			panic(err)
		}
	}
	defer f.Close()

}
func (c *Consumer) createGroupContext(id string, complete chan<- struct {
	correlatedId string
	groupId      *string
	err          error
}) {

	lock := c.rdb.SetNX("lock-"+id, true, time.Second * 2)
	if lock.Err() != nil {
		panic(lock.Err())
	}

	if lock.Val() == false {
		return
	}

	rand.Seed(123) // const seed for benchmark consisting
	delay := rand.Intn(1000)

	time.Sleep(time.Millisecond * time.Duration(delay))

	if delay-(delay/13)*13 == 0 {
		complete <- struct {
			correlatedId string
			groupId      *string
			err          error
		}{correlatedId: id, groupId: nil, err: errors.New("configured error")}
	} else {
		groupId := fmt.Sprintf("group-%s", id)
		complete <- struct {
			correlatedId string
			groupId      *string
			err          error
		}{correlatedId: id, groupId: &groupId, err: nil}
	}
}
