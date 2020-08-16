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

type Processor struct {
	Id               int
	counter          chan<- int64
	rdb              *redis.Client
	bufferTime       time.Duration
	windowSize       int
}

type SafeCache struct {
	v   map[string][]ChatMessage
	mux sync.Mutex
}
// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *Processor) ConsumeClaim(messages <-chan *sarama.ConsumerMessage) error {
	messageCacheFlushTimeInterval := time.After(c.bufferTime)
	addToMessageCachePendingGroupId := make(chan ChatMessage)
	addToMessageCacheExistingGroup := make(chan struct {
		groupId string
		m       ChatMessage
	})
	messageCachePendingGroup := SafeCache{ v: make(map[string][]ChatMessage, 0)}
	messageCacheExistingGroup := SafeCache{v: make(map[string][]ChatMessage, 0)}
	groupCreationCompleted := make(chan struct {
		correlatedId string
		groupId      *string
		err          error
	})

//	var mLock  sync.Mutex
	retryCount :=0
	go func() {
		for {
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
					r := c.rdb.SetNX(e.correlatedId, *e.groupId, 0)
					if r.Err() != nil {
						log.Warn("group id already existing")
						continue
					}
				}
			case <-messageCacheFlushTimeInterval:
				processed := []string{}
				messageCacheExistingGroup.mux.Lock()
				messageCachePendingGroup.mux.Lock()
				for k, v := range messageCachePendingGroup.v {
					groupId, err := c.rdb.Get(k).Result()
					if err == redis.Nil {
						continue
					} else if err != nil {
						panic(err)
					}

					_, ok := messageCacheExistingGroup.v[groupId]
					if !ok {
						messageCacheExistingGroup.v[groupId] = make([]ChatMessage, 0)
					}
					messageCacheExistingGroup.v[groupId] = append(messageCacheExistingGroup.v[groupId], v...)
					delete(messageCachePendingGroup.v, k)
				}


				for k, v := range messageCacheExistingGroup.v {

					if isConsecutive(v, messageCachePendingGroup.v[k]) {
						flushMessages(k, v)
						processed = append(processed, k)
					} else if len(v) > c.windowSize {
						log.WithField("processing", k).Warn("message missing, proceeding stopped")
						continue
						//flushMessages(k, v)
						//delete(messageCacheExistingGroup, k)
					} else {
						// if  len(messageCacheExistingGroup[k]) == 999 {
						//
						// 	log.WithField("processing", k).Infof("!!! disorder detected, restoring. %d %d %d",
						// 		len(messageCacheExistingGroup[k]),
						// 		len(messageCachePendingGroup[v[0].CorrelationId]),
						// 		messageCachePendingGroup[v[0].CorrelationId][0].SeqNum)
						// }
					}
				}
				for _, p :=range processed {
					delete(messageCacheExistingGroup.v, p)
					//messageCacheExistingGroup[p] = make([]ChatMessage,0)
				}
				messageCachePendingGroup.mux.Unlock()
				messageCacheExistingGroup.mux.Unlock()
				messageCacheFlushTimeInterval = time.After(c.bufferTime)
			case m := <-addToMessageCachePendingGroupId:
				//log.Infof("append message %s: %d", m.CorrelationId, m.SeqNum)
				//make sure message has been added to the cache before calling creatGroupContext()
				messageCachePendingGroup.mux.Lock()
				messageCachePendingGroup.v[m.CorrelationId] = append(messageCachePendingGroup.v[m.CorrelationId], m)

				//require lock,
				lock := c.rdb.SetNX("lock-"+m.CorrelationId, true, 0)
				if lock.Err() != nil {
					panic(lock.Err())
				}

				if lock.Val() == true {
					go c.createGroupContext(m.CorrelationId, groupCreationCompleted)
				}

				messageCachePendingGroup.mux.Unlock()
			case g := <-addToMessageCacheExistingGroup:
				// if g.groupId=="group-col-0" {
				// 	log.Infof("%s : %s : %d added", g.groupId, g.m.CorrelationId, g.m.SeqNum)
				// }
				messageCacheExistingGroup.mux.Lock()
				_, ok := messageCacheExistingGroup.v[g.groupId]
				if !ok {
					messageCacheExistingGroup.v[g.groupId] = make([]ChatMessage, 0)
				}
				messageCacheExistingGroup.v[g.groupId] = append(messageCacheExistingGroup.v[g.groupId], g.m)
				messageCacheExistingGroup.mux.Unlock()
			}
		}
	}()

	for message := range messages {
		start := time.Now()
		var m ChatMessage
		err := json.Unmarshal(message.Value, &m)
		if err !=nil {
			panic(err)
		}
		//sync block
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

		elapsed := time.Since(start)
		c.counter <- elapsed.Nanoseconds()
	}

	return nil
}

func isConsecutive(v []ChatMessage, messages []ChatMessage) bool {
	n := len(v)
	sort.SliceStable(v, func(i, j int) bool {
		return v[i].SeqNum < v[j].SeqNum
	})
	for i := n - 1; i > 0; i-- {
		if v[i].SeqNum != v[i-1].SeqNum +1 {
			// if  n >= 999 {
			// 	x := v[i].SeqNum-1
			// 	log.Infof("!!!%s ---  %d : %d, missing %d, existing %d,  pending %d",v[i].CorrelationId, v[i].SeqNum, v[i-1].SeqNum, x, len(v), len(messages))
			// 	for pos, y := range v {
			// 		if y.SeqNum == x {
			// 			log.Errorf("%d: %d", pos, y.SeqNum)
			// 		}
			// 	}
			// }
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

func (c *Processor) createGroupContext(id string, complete chan<- struct {
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
