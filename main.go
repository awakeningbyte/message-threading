package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
)

const (
	TopicSinglePass = "singlepass"
)

type ChatMessage struct {
	UserId int
	Content string
	SeqNum int
	TimeStamp time.Time
}

func main() {
	producerWg := sync.WaitGroup{}

	for id,_ := range [10]int{} {
		producerWg.Add(1)
		go Conversation(id, &producerWg)
	}

	producerWg.Wait()
	fmt.Printf("%d concurrent conversations started. \n", 10)
}

func Conversation(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	producer := NewAsyncProducer()
	for seqNum :=0; seqNum < 3; seqNum++ {
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