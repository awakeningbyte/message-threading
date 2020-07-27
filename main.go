package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
)

const (
	TopicSinglePass = "singlepass"
	ConsumerGroupId = "workers"
)

type ChatMessage struct {
	UserId int
	Content string
	SeqNum int
	TimeStamp time.Time
}

func main() {
	//setup
	clientWg := sync.WaitGroup{}

	for id,_ := range [3]int{} {
		clientWg.Add(1)
		go client(id, &clientWg)
	}

	clientWg.Wait()
	log.Printf("%d concurrent conversations started, each conversation should generate $d test chat message .\n", 3, 10)

	//processing
	log.Printf("bentchmark message processing")
	workersWg := sync.WaitGroup{}
	cancels := make([]context.CancelFunc,0 )
	mux := &sync.Mutex{}
	for wId := range [3]int{} {
		workersWg.Add(1)
		go func(id int) {
			cFunc := Worker(workersWg, id, ConsumerGroupId)
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
		done <-true
	}()

	<-done
}

func client(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	producer := NewAsyncProducer()
	for seqNum :=0; seqNum < 10; seqNum++ {
		message := ChatMessage{
			UserId:    id,
			Content:   uuid.New().String(), //random text
			SeqNum:    seqNum,
			TimeStamp: time.Now(),
		}

		Dispatch(producer,id, &message)
	}
}

func Dispatch(producer sarama.AsyncProducer, id int, message *ChatMessage) {
	v, _:= json.Marshal(*message)
	msg :=&sarama.ProducerMessage{
		Topic:   TopicSinglePass  ,
		Value:   sarama.StringEncoder(v),
		Timestamp: time.Time{},
	}
	producer.Input() <- msg
}

func Worker(wg sync.WaitGroup, wId int, groupId string) context.CancelFunc {
	log.Printf("worker %d start processing messages", wId)
	group, err := NewConsumerGroup(groupId, wId)
	if err != nil {
		panic(err)
	}
	consumer := Consumer{
		ready: make(chan bool),
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		for {
			if err := group.Consume(ctx, strings.Split(TopicSinglePass, ","), &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	<- consumer.ready
	log.Printf ("worker %d up and running.", wId)

	return cancel
}