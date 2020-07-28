package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
)

func GenerateMessages(n,m int) {
	clientWg := sync.WaitGroup{}

	for id := 0; id < n; id++ {
		clientWg.Add(1)
		go func(id int) {
			client(id,m, &clientWg)
		}(id)
	}

	clientWg.Wait()
}

func ProcessResponse(producer sarama.AsyncProducer, ctx context.Context, c context.CancelFunc) {
	for {
		select {
		case <-ctx.Done():
			return
		case result := <-producer.Successes():
			if result != nil {
				log.Printf("> message: \"%s\" sent to partition  %d at offset %d\n", result.Value, result.Partition, result.Offset)
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
func client(id int, m int, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx, cancel := context.WithCancel(context.Background())
	producer := NewAsyncProducer()
	go ProcessResponse(producer, ctx, cancel)
	defer cancel()
	for seqNum := 0; seqNum < m; seqNum++ {
		message := ChatMessage{
			UserId:    id,
			Content:   uuid.New().String(), // random text
			SeqNum:    seqNum,
			TimeStamp: time.Now(),
		}

		Dispatch(producer, &message)
	}
	producer.AsyncClose()
	<-ctx.Done()
}

func Dispatch(producer sarama.AsyncProducer, message *ChatMessage) {
	v, _ := json.Marshal(*message)
	producer.Input() <- &sarama.ProducerMessage{
		Topic:     TopicSinglePass,
		Value:     sarama.StringEncoder(v),
		Timestamp: time.Time{},
	}
}
