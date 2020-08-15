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
CorrelationCount: 10,
SessionSize:      100,
RedisAddr: "localhost:6379",
}
func TestMain(m *testing.M) {
	//prepare output directory
	output := filepath.Join(".", "output")
	os.RemoveAll(output)
	os.MkdirAll(output, os.ModePerm)
	//create redis client
	redisClient := CreateRedis(settings.RedisAddr)
	defer redisClient.Close()

	//setup workers
	SetupWorkers(settings, counter, redisClient)

	//run benchmark
	m.Run()

	//clean redis cache
	redisClient.ClusterResetHard()
}

func BenchmarkProcessThreads(b *testing.B) {
	for i:=0;i <b.N;i++ {
		GenerateMessages(settings)
		msgCount, processingTime := Run(counter, 3)
		fmt.Printf("total message processed: %d, combined time: %d\n", msgCount, processingTime)
	}
}
