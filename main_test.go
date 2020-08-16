package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var counter = make(chan int64)
var settings = Settings{
	ConcurrentCount:           6,
	Brokers:                   "localhost:29092",
	Topic:                     "Incoming",
	GroupId:                   "BenchmarkConsumers",
	CorrelationCount:          10,
	SessionSize:               200,
	RedisAddr:                 "localhost:6379",
	MaxWindowSize:             30,
	BufferTime:                500,
	RetryDelay:                2,
	ErrorInterval:             6,
	MessageDeliveryTimeWindow: 1,
}

func TestMain(m *testing.M) {
	log.Print("benchmark setup")
	output := filepath.Join(".", "output")
	os.RemoveAll(output)
	os.MkdirAll(output, os.ModePerm)
	// create redis client
	redisClient := CreateRedis(settings.RedisAddr)
	defer redisClient.Close()
	log.Print("setup workers")
	// setup workers
	SetupWorkers(settings, counter, redisClient)

	log.Print("running benchmark")
	m.Run()

	// clean redis cache
	redisClient.ClusterResetHard()
	log.Print("checking output correctness")

	hasError := AssertOutputCorrectness(output)
	if hasError == true {
		log.Fatal("Output is incorrect")
	}
}

func AssertOutputCorrectness(dir string) bool {
	files, _ := filepath.Glob(filepath.Join(dir, "*"))
	if len(files) != settings.CorrelationCount {
		log.Fatalf("number of output files not match, expected: %d, got: %d", settings.CorrelationCount, len(files))
	}
	hasError := false
	for _, o := range files {
		count, err := scanFile(o)
		if err != nil {
			hasError =  true
			log.Println(err.Error())
		}
		if count != settings.SessionSize {
			log.Fatalf("number of message for %s is incorrect, expect: %d, got: %d", o, settings.SessionSize, count)
		}
	}
	return hasError

}
func scanFile(name string) (int,error) {
	f, err := os.OpenFile(name, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return 0, fmt.Errorf("open file error: %v", err)

	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	count := 0
	s := 0
	for sc.Scan() {
		l := sc.Text()
		c := strings.Split(l, ":")
		if seqNum, _ := strconv.Atoi(c[1]); seqNum != s {
			log.Printf("%s: Messages outof order! %d : %d", name, s, seqNum)
		}
		count++
		s++
	}
	if err := sc.Err(); err != nil {
		return 0, fmt.Errorf("scan file error: %v", err)
	}

	return count,  nil
}

func BenchmarkProcessThreads(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GenerateMessages(settings)
		msgCount, processingTime := Run(counter, 3)
		fmt.Printf("total message processed: %d, combined time: %d\n", msgCount, processingTime)
	}
}
