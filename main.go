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

func SetupWorkers(settings Settings, counter chan<- int64,redisClient *redis.Client) {
	workersWg := sync.WaitGroup{}
	for wId := 0; wId < settings.ConcurrentCount; wId++ {
		workersWg.Add(1)
		go func(id int) {
			Worker(id, counter, settings, redisClient)
			defer workersWg.Done()
		}(wId)
	}

	workersWg.Wait()
}
func main() {

}
func Run(counter <-chan int64,  t int) (int64) {
	timeout := time.After(time.Duration(t) * time.Millisecond)
	count := int64(0)
	for {
		select {
		case <-timeout:
			return count
		case c := <-counter:
			timeout = time.After(time.Duration(t) * time.Millisecond)
			count += c
		}
	}
	return 0
}
