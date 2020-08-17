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
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"
)

func NewPartitionConsumer(s Settings, partition int) (sarama.PartitionConsumer, error) {
	brokerList := strings.Split(s.Brokers, ",")
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	// config.ClientID = fmt.Sprint("%d", id)
	config.Version, _ = sarama.ParseKafkaVersion("2.1.1")

	cs, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		panic(err)
	}
	return cs.ConsumePartition(s.Topic, int32(partition), sarama.OffsetNewest)

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

type Processor struct {
	Id         int
	counter    chan<- int64
	rdb        *redis.Client
	bufferTime time.Duration
	windowSize int
}

type SafeCache struct {
	v   map[string][]ChatMessage
	mux sync.Mutex
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *Processor) ConsumeClaim(messages <-chan *sarama.ConsumerMessage) error {
	messageCacheFlushTimeInterval := time.After(c.bufferTime)
	addToMessageCachePendingGroupId := make(chan ChatMessage)
	messageCachePendingGroup := SafeCache{v: make(map[string][]ChatMessage, 0)}
	groupCreationCompleted := make(chan struct {
		correlatedId string
		groupId      *string
		err          error
	})

	retryCount := 0
	var flushed = 0
	go func() {
		for {
			select {
			case e := <-groupCreationCompleted:
				if e.err != nil {
					log.Warnf("group created failed %s, retry ...", e.correlatedId)
					go c.createGroupContext(e.correlatedId, groupCreationCompleted) // retry
					retryCount++
					// if retryCount > 5 {
					//  c.rdb.Del("lock-"+e.correlatedId) // remove lock
					// 	panic("too much failure & retry")
					// }
				} else {
					r := c.rdb.SetNX(e.correlatedId, *e.groupId, 0)
					if r.Err() != nil {
						log.Warn("group id already existing")
						continue
					}
				}
			case <-messageCacheFlushTimeInterval:
				processed := []string{}
				messageCachePendingGroup.mux.Lock()

				for k, v := range messageCachePendingGroup.v {
					groupId, err := c.rdb.Get(k).Result()
					if err == redis.Nil {
						continue
					} else if err != nil {
						panic(err)
					}
					if isConsecutive(v) {
						flushed += len(v)
						flushMessages(groupId, v)
						processed = append(processed, k)
					} else if len(v) > c.windowSize {
						log.WithField("processing", k).Warn("message missing, proceeding stopped")
						continue
					}
				}
				for _, p := range processed {
					delete(messageCachePendingGroup.v, p)
				}
				// if len(processed) == 0 && len(messageCachePendingGroup.v) > 0 {
				// 	log.Infof("%d groups are waiting", len(messageCachePendingGroup.v))
				// 	for i, j := range messageCachePendingGroup.v {
				// 		log.Infof("%s(%d)", i, len(j))
				// 	}
				// }
				messageCachePendingGroup.mux.Unlock()
				messageCacheFlushTimeInterval = time.After(c.bufferTime)
			case m := <-addToMessageCachePendingGroupId:
				m.TimeStamp = time.Now()
				// make sure message has been added to the cache before calling creatGroupContext()

				// require lock,
				lock := c.rdb.SetNX("lock-"+m.CorrelationId, 0, 0)
				if lock.Err() != nil {
					panic(lock.Err())
				}

				if lock.Val() == true {
					go c.createGroupContext(m.CorrelationId, groupCreationCompleted)
				}
				messageCachePendingGroup.mux.Lock()
				messageCachePendingGroup.v[m.CorrelationId] = append(messageCachePendingGroup.v[m.CorrelationId], m)
				messageCachePendingGroup.mux.Unlock()
			}
		}
	}()

	for message := range messages {
		c.counter <- 1
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
			m.GroupId = &groupId
			addToMessageCachePendingGroupId <- m
		}

	}

	return nil
}

func isConsecutive(v []ChatMessage) bool {
	n := len(v)

	sort.SliceStable(v, func(i, j int) bool {
		return v[i].SeqNum < v[j].SeqNum
	})
	for i := n - 1; i > 0; i-- {
		if v[i].SeqNum != v[i-1].SeqNum+1 {
			if n == 1000 {
				log.Infof("%d %d", v[i].SeqNum, v[i-1].SeqNum)
			}
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
	f.Close()
	// if len(messages) < 10 {
	// 	log.Infof("%s - %d, queuing time: %d", id, messages[0].SeqNum, time.Since(messages[0].TimeStamp))
	// }
}

func (c *Processor) createGroupContext(id string, complete chan<- struct {
	correlatedId string
	groupId      *string
	err          error
}) {

	ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
	rand.Seed(123) // const seed for benchmark consisting
	delay := rand.Intn(1000) +1000

	select {
	case <-ctx.Done():
		complete <- struct {
			correlatedId string
			groupId      *string
			err          error
		}{correlatedId: id, groupId: nil, err: errors.New("creating group timeout")}
	case <-time.After(time.Millisecond * time.Duration(delay)):
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
