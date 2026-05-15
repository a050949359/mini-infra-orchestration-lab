package main

import (
	"fmt"
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

func buildRedisURL(host, port, db, passwd string) string {
	return fmt.Sprintf("redis://:%s@%s:%s/%s", passwd, host, port, db)
}

func loadConfig() appConfig {
	node1Host := os.Getenv("NODE1_REDIS_HOST")
	if node1Host == "" {
		log.Fatal("NODE1_REDIS_HOST must be set")
	}
	redisURL := buildRedisURL(
		node1Host,
		getenv("NODE1_REDIS_PORT", "6379"),
		getenv("NODE1_REDIS_DB", "0"),
		os.Getenv("NODE1_REDIS_PASSWD"),
	)
	node2RedisURL := buildRedisURL(
		getenv("NODE2_REDIS_HOST", "127.0.0.1"),
		getenv("NODE2_REDIS_PORT", "6379"),
		getenv("NODE2_REDIS_DB", "0"),
		os.Getenv("NODE2_REDIS_PASSWD"),
	)
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
