package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
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

func ensureGroup(ctx context.Context, rdb *redis.Client, stream, group string) error {
	err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err == nil || err.Error() == "BUSYGROUP Consumer Group name already exists" {
		return nil
	}
	return err
}

func updateJobStatus(apiURL, jobID, status string) error {
	payload, err := json.Marshal(map[string]string{"status": status})
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/v1/jobs/%s/status", apiURL, jobID)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("node1 api status=%d body=%s", resp.StatusCode, string(body))
	}

	return nil
}

func setupLogOutput() func() {
	logPath := getenv("WORKER_LOG_FILE", "/var/log/mini-orch/worker.log")
	if logPath == "" {
		return func() {}
	}

	if err := os.MkdirAll(filepath.Dir(logPath), 0755); err != nil {
		log.Printf("unable to create log directory for %s: %v", logPath, err)
		return func() {}
	}

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("unable to open log file %s: %v", logPath, err)
		return func() {}
	}

	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.Printf("file logging enabled: %s", logPath)

	return func() {
		if err := logFile.Close(); err != nil {
			log.Printf("close log file failed: %v", err)
		}
	}
}

func main() {
	log.SetOutput(os.Stdout)
	closeLog := setupLogOutput()
	defer closeLog()

	redisURL := getenv("REDIS_URL", "redis://:IntraNet-Redis-2026!ChangeMe@10.0.0.143:6379/0")
	apiURL := getenv("NODE1_API_URL", "http://10.0.0.143:5000")
	streamKey := getenv("QUEUE_STREAM_KEY", "jobs:stream")
	group := getenv("QUEUE_GROUP", "node2-workers")
	consumerBase := getenv("QUEUE_CONSUMER", "node2-worker")
	workerCount := getenvInt("WORKER_GOROUTINES", 5)
	batchSize := int64(getenvInt("QUEUE_BATCH_SIZE", 10))
	blockMS := time.Duration(getenvInt("QUEUE_BLOCK_MS", 5000)) * time.Millisecond

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("invalid REDIS_URL: %v", err)
	}

	rdb := redis.NewClient(opt)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}
	if err := ensureGroup(ctx, rdb, streamKey, group); err != nil {
		log.Fatalf("unable to ensure stream group: %v", err)
	}

	log.Printf("worker started: stream=%s group=%s consumer=%s goroutines=%d", streamKey, group, consumerBase, workerCount)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("received signal: %s", sig)
		cancel()
	}()

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			consumer := fmt.Sprintf("%s-%d", consumerBase, workerID+1)
			log.Printf("worker goroutine started: consumer=%s", consumer)

			for {
				if ctx.Err() != nil {
					log.Printf("worker goroutine stopped: consumer=%s", consumer)
					return
				}

				res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    group,
					Consumer: consumer,
					Streams:  []string{streamKey, ">"},
					Count:    batchSize,
					Block:    blockMS,
				}).Result()
				if err != nil {
					if err == redis.Nil || ctx.Err() != nil {
						continue
					}
					log.Printf("xreadgroup error (consumer=%s): %v", consumer, err)
					time.Sleep(1 * time.Second)
					continue
				}

				for _, stream := range res {
					for _, msg := range stream.Messages {
						jobID := fmt.Sprintf("%v", msg.Values["job_id"])
						typeVal := fmt.Sprintf("%v", msg.Values["type"])
						priority := fmt.Sprintf("%v", msg.Values["priority"])

						log.Printf("processing message id=%s job_id=%s type=%s priority=%s consumer=%s", msg.ID, jobID, typeVal, priority, consumer)

						if err := updateJobStatus(apiURL, jobID, "processing"); err != nil {
							log.Printf("update processing failed job_id=%s: %v", jobID, err)
							continue
						}

						time.Sleep(5 * time.Second)

						if err := updateJobStatus(apiURL, jobID, "done"); err != nil {
							log.Printf("update done failed job_id=%s: %v", jobID, err)
							continue
						}

						if err := rdb.XAck(ctx, streamKey, group, msg.ID).Err(); err != nil {
							log.Printf("xack failed id=%s: %v", msg.ID, err)
							continue
						}
					}
				}
			}
		}(i)
	}

	wg.Wait()
}
