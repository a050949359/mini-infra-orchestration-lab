package main

import (
	"log"
	"os"
	"strconv"
	"time"
)

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func getenvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

type workerConfig struct {
	streamKey       string
	group           string
	statusStreamKey string
	failedStatusKey string
	fallbackFile    string
	processDelay    time.Duration
}

type appConfig struct {
	redisURL      string
	node2RedisURL string
	logFile       string
	consumerBase  string
	workerCount   int
	batchSize     int64
	blockMS       time.Duration
	retryInterval time.Duration
	worker        workerConfig
}

func loadConfig() appConfig {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatal("REDIS_URL must be set")
	}
	node2RedisURL := os.Getenv("NODE2_REDIS_URL")
	if node2RedisURL == "" {
		log.Fatal("NODE2_REDIS_URL must be set")
	}
	return appConfig{
		redisURL:      redisURL,
		node2RedisURL: node2RedisURL,
		logFile:       getenv("WORKER_LOG_FILE", "/var/log/mini-orch/worker.log"),
		consumerBase:  getenv("QUEUE_CONSUMER", "node2-worker"),
		workerCount:   getenvInt("WORKER_GOROUTINES", 5),
		batchSize:     int64(getenvInt("QUEUE_BATCH_SIZE", 10)),
		blockMS:       time.Duration(getenvInt("QUEUE_BLOCK_MS", 5000)) * time.Millisecond,
		retryInterval: time.Duration(getenvInt("STATUS_RETRY_INTERVAL_MS", 30000)) * time.Millisecond,
		worker: workerConfig{
			streamKey:       getenv("QUEUE_STREAM_KEY", "jobs:stream"),
			group:           getenv("QUEUE_GROUP", "node2-workers"),
			statusStreamKey: getenv("STATUS_STREAM_KEY", "jobs:status"),
			failedStatusKey: getenv("STATUS_FAILED_KEY", "jobs:status:failed"),
			fallbackFile:    getenv("FALLBACK_FILE", "/var/log/mini-orch/fallback.jsonl"),
			processDelay:    time.Duration(getenvInt("WORKER_PROCESS_SLEEP_MS", 5000)) * time.Millisecond,
		},
	}
}
