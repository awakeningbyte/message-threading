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
CorrelationCount: 12,
SessionSize:      100,
RedisAddr: "localhost:6379",
}
func TestMain(m *testing.M) {
	output := filepath.Join(".", "output")
	os.MkdirAll(output, os.ModePerm)
	SetupWorkers(settings, counter)
	m.Run()
}

func BenchmarkProcessThreads(b *testing.B) {
	for i:=0;i <b.N;i++ {
		GenerateMessages(settings)
		msgCount, processingTime := Run(counter)
		fmt.Printf("message count: %d, processing time: %d\n", msgCount, processingTime)
	}
}
