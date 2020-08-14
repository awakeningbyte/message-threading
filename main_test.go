package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)
var counter =make(chan int64)
var settings = Settings{
ConcurrentCount:  6,
Brokers:          "localhost:29092",
Topic:            "Incoming",
GroupId:          "BenchmarkConsumers",
CorrelationCount: 1,
SessionSize:      1,
RedisAddr: "localhost:6379",
}
func TestMain(m *testing.M) {
	output := filepath.Join(".", "output")
	os.MkdirAll(output, os.ModePerm)
	redisClient := CreateRedis(settings.RedisAddr)
	defer redisClient.Close()
	SetupWorkers(settings, counter, redisClient)
	m.Run()
}

func BenchmarkProcessThreads(b *testing.B) {
//	for i:=0;i <b.N;i++ {
		GenerateMessages(settings)
		msgCount, processingTime := Run(counter, 5)
		fmt.Printf("message count: %d, processing time: %d\n", msgCount, processingTime)
//	}
}
