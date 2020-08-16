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
var counter =make(chan int64)
var settings = Settings{
ConcurrentCount:  6,
Brokers:          "localhost:29092",
Topic:            "Incoming",
GroupId:          "BenchmarkConsumers",
CorrelationCount: 100,
SessionSize:      200,
RedisAddr: "localhost:6379",
}
func TestMain(m *testing.M) {
	log.Print("benchmark setup")
	output := filepath.Join(".", "output")
	os.RemoveAll(output)
	os.MkdirAll(output, os.ModePerm)
	//create redis client
	redisClient := CreateRedis(settings.RedisAddr)
	defer redisClient.Close()
	log.Print("setup workers")
	//setup workers
	SetupWorkers(settings, counter, redisClient)

	log.Print("running benchmark")
	m.Run()

	//clean redis cache
	redisClient.ClusterResetHard()
	log.Print("checking output correctness")
	AssertOutputCorrectness(output)
}

func AssertOutputCorrectness(dir string) {
	files, _ := filepath.Glob(filepath.Join(dir, "*"))
	if len(files) != settings.CorrelationCount {
		log.Fatalf("number of output files not match, expected: %d, got: %d", settings.CorrelationCount, len(files))
	}

	for _, o := range files {
		count := scanFile(o)
		if count != settings.SessionSize {
			log.Fatalf("number of message for %s is incorrect, expected: %d, got: %d", o, settings.SessionSize, count)
		}
	}

}
func scanFile(name string) (int) {
	f, err := os.OpenFile(name, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("open file error: %v", err)
		return 0
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	count :=0
	s :=0
	for sc.Scan() {
		l := sc.Text()
		c := strings.Split(l,":")
		if  seqNum, _ := strconv.Atoi(c[1]); seqNum != s {
			log.Fatalf("%s: Messages outof order!", name)
		}
		count++
		s++
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("scan file error: %v", err)
		return 0
	}

	return count
}

func BenchmarkProcessThreads(b *testing.B) {
	for i:=0;i <b.N;i++ {
		GenerateMessages(settings)
		msgCount, processingTime := Run(counter, 3)
		fmt.Printf("total message processed: %d, combined time: %d\n", msgCount, processingTime)
	}
}
