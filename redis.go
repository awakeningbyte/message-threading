package main

import (
	"log"
	"os"

	"github.com/go-redis/redis"
)

var redisClient *redis.Client

func CreateRedis(addr string) *redis.Client{
	redisClient = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_PASSWORD"),
	})

	_, err := redisClient.Ping().Result()
	if err != nil {
		log.Fatal("Redis connection error ", err)
	}
	return redisClient
}

