package main

import (
	"sync"
	"time"

	"github.com/go-redis/redis"
)

const ()

type ChatMessage struct {
	GroupId       *string
	CorrelationId string
	Content       string
	SeqNum        int
	TimeStamp     time.Time
	QueuingTime 	int64
}

func SetupWorkers(settings Settings, counter chan<- int64, flushCounter chan int, redisClient *redis.Client) {
	workersWg := sync.WaitGroup{}
	for wId := 0; wId < settings.ConcurrentCount; wId++ {
		workersWg.Add(1)
		go func(id int) {
			Worker(id, counter, flushCounter, settings, redisClient)
			defer workersWg.Done()
		}(wId)
	}

	workersWg.Wait()
}

func Run(counter <-chan int64, flushCounter chan int, t int) (int, int, int64) {
	timeout := time.After(time.Duration(t) * time.Second)
	totalProcessingTime := int64(0)
	count := 0
	flushed := 0
	for {
		select {
		case <-timeout:
			return count, flushed, totalProcessingTime
		case <-flushCounter:
			timeout = time.After(time.Duration(t) * time.Second)
			flushed += 1
		case c := <-counter:
			timeout = time.After(time.Duration(t) * time.Second)
			count += 1
			totalProcessingTime = totalProcessingTime + c
		}
	}
	return 0, 0, 0
}
