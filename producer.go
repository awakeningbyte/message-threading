package main

import (
	"os"
	log "github.com/sirupsen/logrus"
	"strings"
	"github.com/Shopify/sarama"
)

func NewAsyncProducer() sarama.AsyncProducer {
	brokerList := strings.Split(os.Getenv("brokers"), ",")
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 1
	config.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		log.WithError(err).Fatal("Failed to start order producer")
	}

	go func() {
		for err := range producer.Errors() {
			log.WithError(err).Error("Producer failed writing Order message")
		}
	}()

	return producer
}