package main

import (
	"sync"
	"time"
	//"github.com/Shopify/sarama"

	"github.com/Shopify/sarama"
)

func main() {
	wg := sync.WaitGroup{}
	for i,_ := range [10]int{} {
		wg.Add(1)
		go Worker(i, &wg)
	}

	wg.Wait()
}

func Worker(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	producer := NewAsyncProducer()
	for i :=0; i< 3; i++ {
		message := time.Now().Local().String()
		Dispatch(producer,id, message)
	}
}

func Dispatch(producer sarama.AsyncProducer, id int, message string) {
	
}