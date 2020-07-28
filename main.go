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
	total := int64(0)
	count := 0
	for {
		select {

		case <-done:
			return
		case <-heartbeat:
			if len(cancels) < n {
				heartbeat = time.After(1 * time.Second)
			} else {
				fmt.Printf("stop processing. message count: %d, total processing time: %d miliseconds.\n", count, total / 1000)
				return
			}
		case c := <-counter:
			count = count +1
			total = total + c
			heartbeat = time.After(1 * time.Second)
		}
	}
}
