package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
)

func GenerateMessages(settings Settings) {
	clientWg := sync.WaitGroup{}

	for i := 0; i < settings.ConcurrentCount; i++ {
		clientWg.Add(1)
		go func(idx int) {
			client(idx, settings, &clientWg)
		}(i)
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
				// log.Printf("> message: \"%s\" sent to partition  %d at offset %d\n", result.Value, result.Partition, result.Offset)
			} else {
				c()
			}

		case err := <-producer.Errors():
			if err != nil {
				log.Println("Failed to produce message", err)
			} else {
				c()
			}
			select {
			case <-ctx.Done():
				return
			case result := <-producer.Successes():
				if result != nil {
					// log.Printf("> message: \"%s\" sent to partition  %d at offset %d\n", result.Value, result.Partition, result.Offset)
				} else {
					c()
				}

			case err := <-producer.Errors():
				if err != nil {
					log.Println("Failed to produce message", err)
				} else {
					c()
				}
			}}
	}
}
func client(idx int, s Settings,  wg *sync.WaitGroup) {
	defer wg.Done()
	producer := NewAsyncProducer(s)
	blockSize := (s.CorrelationCount / s.ConcurrentCount)
	if blockSize * s.ConcurrentCount <  s.CorrelationCount {
		blockSize = blockSize + 1
	}
	for cId := 0; cId < blockSize; cId++ {
		correlationId := idx * blockSize + cId
		if correlationId >= s.CorrelationCount {
			break
		}

		for seqNum := 0; seqNum < s.SessionSize; seqNum++ {
			message := ChatMessage{
				CorrelationId: fmt.Sprintf("col%d",correlationId),
				SeqNum:        seqNum,
				Content:       uuid.New().String(), // random text
				TimeStamp:     time.Now(),
			}

			Dispatch(producer, &message, s.Topic)
		}

	}
	producer.Close()
}

func Dispatch(producer sarama.AsyncProducer, message *ChatMessage, topic string) {
	v, _ := json.Marshal(*message)
	producer.Input() <- &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.StringEncoder(v),
		Timestamp: time.Time{},
	}
	select {
	case result := <-producer.Successes():
		if result != nil {
			log.Printf("> message: \"%s\" sent to partition  %d at offset %d\n", result.Value, result.Partition, result.Offset)
		}

	case err := <-producer.Errors():
		if err != nil {
			log.Println("Failed to produce message", err)
		}
	}
}
