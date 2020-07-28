package main

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	os.Setenv("brokers", "localhost:29092")
	GenerateMessages(30, 500)
	m.Run()
	os.Setenv("brokers", "")
}

func BenchmarkProcessThreads(b *testing.B) {
	ProcessThreads(3)
}
