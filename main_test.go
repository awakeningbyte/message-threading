package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
)

var counter = make(chan int64)
var flushCounter = make(chan int)
var settings = Settings{
	ConcurrentCount:           6,
	Brokers:                   "localhost:29092",
	Topic:                     "Incoming",
	GroupId:                   "BenchmarkConsumers",
	CorrelationCount:          100,
	SessionSize:               1000,
	RedisAddr:                 "localhost:6379",
	MaxWindowSize:             1000,
	BufferTime:                100, //100
	RetryDelay:                1,
	ErrorInterval:             6,
	// MessageDeliveryTimeWindow: 1,
}
var done = make(chan struct{})
func TestMain(m *testing.M) {
	log.Print("benchmark setup")
	output := filepath.Join(".", "output")
	os.RemoveAll(output)
	os.MkdirAll(output, os.ModePerm)
	// create redis client
	redisClient := CreateRedis(settings.RedisAddr)
	redisClient.FlushAll()
	defer redisClient.Close()
	log.Print("setup workers")
	// setup workers
	SetupWorkers(settings, counter, redisClient)

	log.Print("running benchmark")
	m.Run()

	// clean redis cache
	log.Print("checking output correctness")

	if CheckOutputCorrectness(output) {
		log.Fatal("Assertion failed")
	}
}

func BenchmarkProcessThreads(b *testing.B) {
	for i := 0; i < b.N; i++ {

		GenerateMessages(settings)
		msgCount:= Run(counter, 3000)
		fmt.Printf("total %d message received\n", msgCount)
	}
}

func CheckOutputCorrectness(dir string) (hasError bool) {
	//time.Sleep(time.Second * 10) // wait the disk writing to complete
	files, _ := filepath.Glob(filepath.Join(dir, "*"))
	if len(files) != settings.CorrelationCount {
		log.Fatalf("number of output files not match, expected: %d, got: %d", settings.CorrelationCount, len(files))
	}
	for _, o := range files {
		count, err := assertOutputFile(o)
		if err != nil {
			hasError =  true
			log.Println(err.Error())
		}
		if count != settings.SessionSize {
			hasError =  true
			log.Printf("number of message for %s is incorrect, expect: %d, got: %d", o, settings.SessionSize, count)
		}
	}
	return hasError
}
func assertOutputFile(name string) (int,error) {
	f, err := os.OpenFile(name, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return 0, fmt.Errorf("open file error: %v", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	count := 0
	s := 0
	detected := []string{}
	for sc.Scan() {
		l := sc.Text()
		c := strings.Split(l, ":")
		if seqNum, _ := strconv.Atoi(c[1]); seqNum > s {
			detected = append(detected, fmt.Sprintf("incorrect seq# %d : %d", s, seqNum))
		}
		count++
		s++
	}
	if err = sc.Err(); err != nil {
		return 0, fmt.Errorf("scan file error: %v", err)
	}

	if len(detected) ==0 {
		return count, nil
	} else {
		for _, e := range detected {
			log.WithField("assert", name).Warn(e)
		}
		return count,  errors.New(fmt.Sprintf("%s assertion failed. messages in wrong order", name))
	}
}

