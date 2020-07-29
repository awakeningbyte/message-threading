package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	TopicSinglePass = "singlepass"
	ConsumerGroupId = "workers1"
)

type ChatMessage struct {
	UserId    int
	Content   string
	SeqNum    int
	TimeStamp time.Time
}

func main() {
	counter :=make(chan int64)
	SetupWorkers(3, counter)
	GenerateMessages(3, 3)
	msgCount, processingTime := Run(counter)
	fmt.Printf("message count: %d, processing time: %d", msgCount, processingTime)

}

func SetupWorkers(n int, counter chan <-int64) {
	workersWg := sync.WaitGroup{}
	cancels := make([]context.CancelFunc, 0)
	mux := &sync.Mutex{}
	for wId := 0; wId < n; wId++ {
		workersWg.Add(1)
		go func(id int) {
			cFunc := Worker(id, counter, ConsumerGroupId)
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

func Run(counter <-chan int64) (int, int64) {
	timeout := time.After(1 * time.Second)
	totalProcessingTime := int64(0)
	count := 0
	for {
		select {
		case <-timeout:
			return count,totalProcessingTime
		case c := <-counter:
			timeout = time.After(1 * time.Second)
			count = count +1
			totalProcessingTime = totalProcessingTime + c
		}
	}
	return 0, 0
}
