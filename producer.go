package main

import (
	"os"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)
func NewConsumerGroup(name string, id int) (sarama.ConsumerGroup, error) {
	brokerList := strings.Split(os.Getenv("brokers"), ",")
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	//config.ClientID = fmt.Sprint("%d", id)
	config.Version,_ = sarama.ParseKafkaVersion("2.1.1")
	return sarama.NewConsumerGroup(brokerList, name, config)
}
func NewAsyncProducer() sarama.SyncProducer {
	brokerList := strings.Split(os.Getenv("brokers"), ",")
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.WithError(err).Fatal("Failed to start order producer")
	}


	return producer
}

type Consumer struct {
	ready chan bool
	Id int
}

// GenerateMessages is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		log.Printf("ID: %d, Message claimed: value = %s, timestamp = %v, topic = %s", consumer.Id, string(message.Value) , message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}

	return nil
}