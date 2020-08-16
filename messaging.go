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
	ready            chan bool
	Id               int
	counter          chan<- int64
	rdb              *redis.Client
	bufferTime       time.Duration
	windowSize       int
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
	messageCacheFlushTimeInterval := time.After(c.bufferTime)
	addToMessageCachePendingGroupId := make(chan ChatMessage)
	addToMessageCacheExistingGroup := make(chan struct {
		groupId string
		m       ChatMessage
	})
	messageCachePendingGroup := make(map[string][]ChatMessage, 0)
	messageCacheExistingGroup := make(map[string][]ChatMessage, 0)
	groupCreationCompleted := make(chan struct {
		correlatedId string
		groupId      *string
		err          error
	})
	retryCount :=0
	go func() {
		for {
			defer close(groupCreationCompleted)
			select {
			case e := <-groupCreationCompleted:
				if e.err != nil {
					log.Warnf("group created failed %s: %s, retry ...", *e.groupId, e.correlatedId)
					go c.createGroupContext(*e.groupId, groupCreationCompleted) // retry
					retryCount++
					// if retryCount > 5 {
					//  c.rdb.Del("lock-"+e.correlatedId) // remove lock
					// 	panic("too much failure & retry")
					// }
				} else {
					log.Debugf("group created %s: %s", *e.groupId, e.correlatedId)
					r := c.rdb.SetNX(e.correlatedId, *e.groupId, 0)
					if r.Err() != nil {
						log.Warn("group id already existing")
						continue
					}

					flushMessages(*e.groupId, messageCachePendingGroup[e.correlatedId])
					delete(messageCachePendingGroup, *e.groupId)
				}
			case <-messageCacheFlushTimeInterval:
				for k, v := range messageCacheExistingGroup {
					if isConsecutive(v) {
						flushMessages(k, v)
						delete(messageCacheExistingGroup, k)
					} else if len(v) > c.windowSize {
						log.Warnf("%s:message missing detected, stop processing",k)
						flushMessages(k, v)
						delete(messageCacheExistingGroup, k)
					} else if len(v) > c.windowSize {
					} else {
						log.Infof("%s:disorder detected, wait to next round",k)
					}
				}
				messageCacheFlushTimeInterval = time.After(c.bufferTime)
			case m := <-addToMessageCachePendingGroupId:
				log.Debugf("append message %s: %d", m.CorrelationId, m.SeqNum)
				//make sure message has been added to the cache before calling creatGroupContext()
				messageCachePendingGroup[m.CorrelationId] = append(messageCachePendingGroup[m.CorrelationId], m)

				//require lock,
				lock := c.rdb.SetNX("lock-"+m.CorrelationId, true, 0)
				if lock.Err() != nil {
					panic(lock.Err())
				}

				if lock.Val() == true {
					go c.createGroupContext(m.CorrelationId, groupCreationCompleted)
				}
			case g := <-addToMessageCacheExistingGroup:
				_, ok := messageCacheExistingGroup[g.groupId]
				if !ok {
					messageCacheExistingGroup[g.groupId] = make([]ChatMessage, 0)
				}
				messageCacheExistingGroup[g.groupId] = append(messageCacheExistingGroup[g.groupId], g.m)
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
			addToMessageCachePendingGroupId <- m

		} else if err != nil {
			panic(err)
		} else {
			addToMessageCacheExistingGroup <- struct {
				groupId string
				m       ChatMessage
			}{groupId: groupId, m: m}
		}

		log.Debugf("ID: %d, Message claimed: value = %s, timestamp = %v, topic = %s", c.Id, string(message.Value) , message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
		elapsed := time.Since(start)
		c.counter <- elapsed.Nanoseconds()
	}

	return nil
}

func isConsecutive(v []ChatMessage) bool {
	n := len(v)
	sort.SliceStable(v, func(i, j int) bool {
		return v[i].SeqNum < v[j].SeqNum
	})
	for i := n - 1; i > 0; i-- {
		if v[i].SeqNum != v[i-1].SeqNum +1 {
			return false
		}
	}
	return true

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
		if _, err := f.WriteString(fmt.Sprintf("%s:%d\n", m.CorrelationId, m.SeqNum)); err != nil {
			panic(err)
		}
	}
	log.Debugf("%s flushed", messages[0].CorrelationId)
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
