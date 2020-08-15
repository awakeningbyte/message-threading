package main

import (
	"context"
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
	//groupCreationTimeout := time.After(time.Second * 5)
	passThroughMessageInterval := time.After(time.Millisecond * 1000)
	//startCorrelationGroup := make(chan ChatMessage)
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
	retryCount :=0
	go func() {
		for {
			defer close(groupCreationComplete)
			select {
			case e := <-groupCreationComplete:
				if e.err != nil {
					log.Warnf("group created failed %s: %s, retry ...", *e.groupId, e.correlatedId)
					go c.createGroupContext(*e.groupId, groupCreationComplete) // retry
					retryCount++
					// if retryCount > 5 {
					//  c.rdb.Del("lock-"+e.correlatedId) // remove lock
					// 	panic("too much failure & retry")
					// }
				} else {
					log.Infof("group created %s: %s", *e.groupId, e.correlatedId)
					r := c.rdb.SetNX(e.correlatedId, *e.groupId, 0)
					if r.Err() != nil {
						log.Warn("group id already existing")
						continue
					}

					flushMessages(*e.groupId, messageThreadsPendingGroup[e.correlatedId])
					delete(messageThreadsPendingGroup, *e.groupId)
				}
			case <-passThroughMessageInterval:
				// log.Info("passThroughMessageInterval triggeredS")
				//log.Info(".")
				for k, v := range messageThreadsGrouped {
					flushMessages(k, v)
				}
				messageThreadsGrouped = make(map[string][]ChatMessage, 0)
				passThroughMessageInterval = time.After(time.Millisecond * 500)
			//case m := <-startCorrelationGroup:
			//	log.Infof("start group %s", m.CorrelationId)
			//	messageThreadsPendingGroup[m.CorrelationId] = make([]ChatMessage, 0)
			case m := <-appendToCorrelationGroup:
				log.Infof("append message %s: %d", m.CorrelationId, m.SeqNum)
				//requiring lock,
				lock := c.rdb.SetNX("lock-"+m.CorrelationId, true, 0)
				if lock.Err() != nil {
					panic(lock.Err())
				}

				if lock.Val() == true {
					go c.createGroupContext(m.CorrelationId, groupCreationComplete)
				}
				messageThreadsPendingGroup[m.CorrelationId] = append(messageThreadsPendingGroup[m.CorrelationId], m)
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
			//startCorrelationGroup <- m
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
	log.Infof("%s flushed", messages[0].CorrelationId)
	defer f.Close()

}

func (c *Consumer) createGroupContext(id string, complete chan<- struct {
	correlatedId string
	groupId      *string
	err          error
}) {


	ctx, _ := context.WithTimeout(context.Background(), time.Second * 2)
	rand.Seed(123) // const seed for benchmark consisting
	delay := rand.Intn(2500)

	select {
		case <-ctx.Done():
		complete <- struct {
			correlatedId string
			groupId      *string
			err          error
		}{correlatedId: id, groupId: nil, err: errors.New("creating group timeout")}
		case <- time.After(time.Millisecond  * time.Duration(delay)):
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
}
