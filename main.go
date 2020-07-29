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
	// generate mock client-side messages
	GenerateMessages(3, 3)

	// threading messages
	ProcessThreads(3)
}

func ProcessThreads(n int) {
	workersWg := sync.WaitGroup{}
	cancels := make([]context.CancelFunc, 0)
	mux := &sync.Mutex{}
	counter := make(chan int64)
	for wId := 0; wId < n; wId++ {
		workersWg.Add(1)
		go func(id int) {
			cFunc := Worker(workersWg, id, counter, ConsumerGroupId)
			mux.Lock()
			cancels = append(cancels, cFunc)
			mux.Unlock()
		}(wId)
	}
	done := make(chan bool)
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigterm
		for _, c := range cancels {
			c()
		}
		done <- true
	}()

	go func() {
		workersWg.Wait()
		done <- true
	}()

	heartbeat := time.After(1 * time.Second)
	workerReadyTime := new(int)
	totalProcessingTime := int64(0)
	totalExecutionTime := int(0)
	count := 0
	for {
		select {

		case <-done:
			return
		case <-heartbeat:
			if len(cancels) < n {
				heartbeat = time.After(2 * time.Second)
			} else {
				totalExecutionTime = time.Now().Nanosecond() - *workerReadyTime
				fmt.Printf("stop processing.  message count: %d, total threading execution time: %d,total processing time: %d miliseconds.\n", count, totalExecutionTime / 1000000000,  totalProcessingTime/ 1000)
				return
			}
		case c := <-counter:
			heartbeat = time.After(1 * time.Second)
			if workerReadyTime == nil {
				wt := time.Now().Nanosecond()
				workerReadyTime = &wt
			}
			count = count +1
			totalProcessingTime = totalProcessingTime + c
		}
	}
}
