package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	ConsumerGroupId = "workers1"
)

type ChatMessage struct {
	UserId int
	Content string
	SeqNum int
	TimeStamp time.Time
}

func main() {
	//setup
	GenerateMessages()

	// processing
	//Process()
}

func Process() {
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

func GenerateMessages() {
	clientWg := sync.WaitGroup{}

	for id := range [1]int{} {
		clientWg.Add(1)
		go func(id int) {
			client(id, &clientWg)
		}(id)
	}

	clientWg.Wait()
	log.Printf("%d concurrent conversations started, each conversation should generate $d test chat message .\n", 3, 10)
}
func ProcessResponse(producer sarama.AsyncProducer, ctx context.Context, c context.CancelFunc) {
	for {
		select {
		case <-ctx.Done():
			return
		case result := <-producer.Successes():
			if result != nil {
				log.Printf("> message: \"%s\" sent to partition  %d at offset %d\n", result.Value,  result.Partition, result.Offset)
			} else {
				c()
			}

		case err := <-producer.Errors():
			if err != nil {
				log.Println("Failed to produce message", err)
			} else {
				c()
			}
		}
	}
}
func client(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx, cancel := context.WithCancel( context.Background())
	producer := NewAsyncProducer()
	go ProcessResponse(producer, ctx, cancel)
	defer cancel()
	for seqNum :=0; seqNum < 10; seqNum++ {
		message := ChatMessage{
			UserId:    id,
			Content:   uuid.New().String(), //random text
			SeqNum:    seqNum,
			TimeStamp: time.Now(),
		}

		Dispatch(producer, &message)
		fmt.Printf("user: %d, seq#: %d produced \n", id, seqNum)
	}
	producer.AsyncClose()
	<-ctx.Done()
}

func Dispatch(producer sarama.AsyncProducer,message *ChatMessage) {
	v, _:= json.Marshal(*message)
	producer.Input() <- &sarama.ProducerMessage{
		Topic:   TopicSinglePass  ,
		Value:   sarama.StringEncoder(v),
		Timestamp: time.Time{},
	}
}

func Worker(wg sync.WaitGroup, wId int, groupId string) context.CancelFunc {
	log.Printf("worker %d start processing messages", wId)
	group, err := NewConsumerGroup(groupId, wId)
	if err != nil {
		panic(err)
	}
	consumer := Consumer{
		ready: make(chan bool),
		Id: wId,
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
			fmt.Printf("next")
		}
	}()
	<- consumer.ready
	log.Printf ("worker %d up and running.", wId)

	return cancel
}