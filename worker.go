package main

import (
	"context"

	log "github.com/sirupsen/logrus"

	"github.com/go-redis/redis"
)

func Worker(wId int, counter chan<- int64, s Settings, rdb *redis.Client) context.CancelFunc {
	// log.Printf("worker %d start processing messages", wId)
	group, err := NewConsumerGroup(s)
	if err != nil {
		panic(err)
	}
	consumer := Consumer{
		ready: make(chan bool),
		Id: wId,
		counter: counter,
		rdb: rdb,
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {

			if err := group.Consume(ctx, []string {s.Topic}, &consumer); err != nil {
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
	log.Debugf("worker %d up and running.", consumer.Id)

	return cancel
}
