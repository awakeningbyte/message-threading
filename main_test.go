package main

import (
	"fmt"
	"os"
	"testing"
)
var counter =make(chan int64)

func TestMain(m *testing.M) {
	os.Setenv("brokers", "localhost:29092")
	SetupWorkers(3, counter)
	m.Run()
	os.Setenv("brokers", "")
}

func BenchmarkProcessThreads(b *testing.B) {
	for i:=0;i <b.N;i++ {
		GenerateMessages(30, 500)
		msgCount, processingTime := Run(counter)
		fmt.Printf("message count: %d, processing time: %d\n", msgCount, processingTime)
	}
}
