package main

import (
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"

	"github.com/go-redis/redis"
)

func Worker(wId int, counter chan<- int64, s Settings, rdb *redis.Client) {
	// log.Printf("worker %d start processing messages", wId)
	partitionConsumer,err := NewPartitionConsumer(s, wId)
	if err != nil {
		panic(err)
	}
	processor := Processor{
		Id: wId,
		counter: counter,
		rdb: rdb,
		bufferTime: time.Duration(s.BufferTime) * time.Millisecond,
		windowSize: s.MaxWindowSize,
	}

	go func(pc sarama.PartitionConsumer) {
		for {
			processor.ConsumeClaim(pc.Messages())
		}
	}(partitionConsumer)

	log.Debugf("worker %d up and running.", wId)
}
