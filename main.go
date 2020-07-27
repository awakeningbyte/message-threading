package main

import (
	"context"
	"log"
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
	UserId int
	Content string
	SeqNum int
	TimeStamp time.Time
}

func main() {
	//generate mock client-side messages
	GenerateMessages()

	//threading messages
	ProcessThreads()
}

func ProcessThreads() {
	log.Printf("bentchmark message processing")
	workersWg := sync.WaitGroup{}
	cancels := make([]context.CancelFunc, 0)
	mux := &sync.Mutex{}
	for wId := range [1]int{} {
		workersWg.Add(1)
		go func(id int) {
			cFunc := Worker(workersWg, wId, ConsumerGroupId)
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

	<-done
}

