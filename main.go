package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis"
)

const (
)

type ChatMessage struct {
	CorrelationId string
	Content       string
	SeqNum        int
	TimeStamp     time.Time
}

// func main() {
// 	counter :=make(chan int64)
// 	n, err := strconv.Atoi(os.Getenv("ConcurrentCount"))
// 	if err!= nil || n < 1{
// 		panic("ConcurrentCount is invalid")
// 	}
//
// 	c, err := strconv.Atoi(os.Getenv("CorrelationCount"))
// 	if err!= nil || c < 1 {
// 		c = n * 2
// 		log.Infof("CorrelationCount is invalid, using %d instead", c)
// 	}
// 	s, err := strconv.Atoi(os.Getenv("SessionSize"))
// 	if err!= nil || c < 1 {
// 		s = 100
// 		log.Infof("SessionSize is invalid, using %d instead", s)
// 	}
// 	settings := Settings{
// 		ConcurrentCount:  n,
// 		Brokers:          os.Getenv("Brokers"),
// 		Topic:            os.Getenv("Topic"),
// 		GroupId:          os.Getenv("GroupId"),
// 		CorrelationCount: c,
// 		SessionSize:      s,
// 	}
// 	SetupWorkers(settings, counter)
// 	GenerateMessages(settings)
// 	msgCount, processingTime := Run(counter)
// 	fmt.Printf("message count: %d, processing time: %d sec", msgCount, processingTime/1000)
//
// }

func SetupWorkers(settings Settings, counter chan <-int64, redisClient *redis.Client) {
	workersWg := sync.WaitGroup{}
	cancels := make([]context.CancelFunc, 0)
	mux := &sync.Mutex{}
	for wId := 0; wId < settings.ConcurrentCount; wId++ {
		workersWg.Add(1)
		go func(id int) {
			cFunc := Worker(id, counter, settings, redisClient)
			mux.Lock()
			cancels = append(cancels, cFunc)
			mux.Unlock()
			defer workersWg.Done()
		}(wId)
	}
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigterm
		for _, c := range cancels {
			c()
		}
	}()

	workersWg.Wait()
}

func Run(counter <-chan int64, t int) (int, int64) {
	timeout := time.After(time.Duration(t) * time.Second)
	totalProcessingTime := int64(0)
	count := 0
	for {
		select {
		case <-timeout:
			return count,totalProcessingTime
		case c := <-counter:
			timeout = time.After(time.Duration(t) * time.Second)
			count = count +1
			totalProcessingTime = totalProcessingTime + c
		}
	}
	return 0, 0
}
