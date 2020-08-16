package main

type Settings struct {
	ConcurrentCount           int
	Brokers                   string
	Topic                     string
	GroupId                   string
	CorrelationCount          int
	SessionSize               int
	RedisAddr                 string
	MaxWindowSize             int
	BufferTime                int
	ErrorInterval             int
	RetryDelay                int
	MessageDeliveryTimeWindow int
}
