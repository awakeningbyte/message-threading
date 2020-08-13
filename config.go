package main

type Settings struct {
	ConcurrentCount  int
	Brokers          string
	Topic            string
	GroupId          string
	CorrelationCount int
	SessionSize      int
}
