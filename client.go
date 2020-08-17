package main

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
)

func client(idx int, s Settings,  wg *sync.WaitGroup) {
	defer wg.Done()
	dWg := sync.WaitGroup{}
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
				CorrelationId: fmt.Sprintf("col-%d",correlationId),
				SeqNum:        seqNum,
				Content:       uuid.New().String(), // random text
				TimeStamp:     time.Now(),
			}

			//mimic outof order message delivery
			wildCard := rand.Intn(s.SessionSize)
			if (wildCard /s.ErrorInterval) *s.ErrorInterval == wildCard {
				dWg.Add(1)
				go func()  {
					time.Sleep(time.Millisecond * time.Duration(s.RetryDelay))
					Dispatch(idx, producer, &message, s.Topic)
					defer dWg.Done()
				}()
			} else {
				//r := rand.Intn(s.MessageDeliveryTimeWindow)
				//time.Sleep(time.Millisecond * time.Duration(r))
				Dispatch(idx, producer, &message, s.Topic)
			}
		}
	}

	dWg.Wait()
	producer.Close()
}

func Dispatch(idx int, producer sarama.AsyncProducer, message *ChatMessage, topic string) {
	v, _ := json.Marshal(*message)
	producer.Input() <- &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.StringEncoder(v),
		Timestamp: time.Time{},
		//Key: sarama.StringEncoder(strconv.Itoa(idx)),
		Partition: int32(idx),
	}
	select {
	case result := <-producer.Successes():
		if result != nil {
			//log.Printf("> message: \"%s\" sent to partition  %d at offset %d\n", result.Value, result.Partition, result.Offset)
		}

	case err := <-producer.Errors():
		if err != nil {
			log.Println("Failed to produce message", err)
		}
	}
}

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

