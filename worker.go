package main

import (
	"context"
	"log"
	"strings"
)

func Worker( wId int, counter chan <-int64, groupId string) context.CancelFunc {
	// log.Printf("worker %d start processing messages", wId)
	group, err := NewConsumerGroup(groupId, wId)
	if err != nil {
		panic(err)
	}
	consumer := Consumer{
		ready: make(chan bool),
		Id: wId,
		counter: counter,
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
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
	log.Printf ("worker %d up and running.", consumer.Id)

	return cancel
}
