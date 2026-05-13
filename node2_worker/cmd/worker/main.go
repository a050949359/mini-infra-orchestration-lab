package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
	if err == nil || strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
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

func publishStatus(ctx context.Context, rdb *redis.Client, stream, jobID, status string) error {
	return rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{
			"job_id": jobID,
			"status": status,
		},
	}).Err()
}

type failedEntry struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

func retryFailedStatus(ctx context.Context, rdbLocal, rdbNode1 *redis.Client, failedKey, statusStream string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				val, err := rdbLocal.LPop(ctx, failedKey).Result()
				if err == redis.Nil {
					break
				}
				if err != nil {
					log.Printf("retry: lpop failed: %v", err)
					break
				}
				var entry failedEntry
				if err := json.Unmarshal([]byte(val), &entry); err != nil {
					log.Printf("retry: unmarshal failed val=%s: %v", val, err)
					continue
				}
				if err := publishStatus(ctx, rdbNode1, statusStream, entry.JobID, entry.Status); err != nil {
					log.Printf("retry: xadd failed job_id=%s: %v, re-queuing", entry.JobID, err)
					if pushErr := rdbLocal.RPush(ctx, failedKey, val).Err(); pushErr != nil {
						log.Printf("retry: re-queue failed job_id=%s: %v", entry.JobID, pushErr)
					}
					break
				}
				log.Printf("retry: status recovered job_id=%s status=%s", entry.JobID, entry.Status)
			}
		}
	}
}

func main() {
	log.SetOutput(os.Stdout)
	closeLog := setupLogOutput()
	defer closeLog()

	redisURL := getenv("REDIS_URL", "redis://:IntraNet-Redis-2026!ChangeMe@10.0.0.143:6379/0")
	node2RedisURL := getenv("NODE2_REDIS_URL", "redis://localhost:6379/0")

	streamKey := getenv("QUEUE_STREAM_KEY", "jobs:stream")
	group := getenv("QUEUE_GROUP", "node2-workers")
	consumerBase := getenv("QUEUE_CONSUMER", "node2-worker")
	workerCount := getenvInt("WORKER_GOROUTINES", 5)
	batchSize := int64(getenvInt("QUEUE_BATCH_SIZE", 10))
	blockMS := time.Duration(getenvInt("QUEUE_BLOCK_MS", 5000)) * time.Millisecond

	statusStreamKey := getenv("STATUS_STREAM_KEY", "jobs:status")
	failedStatusKey := getenv("STATUS_FAILED_KEY", "jobs:status:failed")
	retryInterval := time.Duration(getenvInt("STATUS_RETRY_INTERVAL_MS", 30000)) * time.Millisecond
	processDelay := time.Duration(getenvInt("WORKER_PROCESS_SLEEP_MS", 5000)) * time.Millisecond

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("invalid REDIS_URL: %v", err)
	}
	rdbNode1 := redis.NewClient(opt)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer rdbNode1.Close()

	if err := rdbNode1.Ping(ctx).Err(); err != nil {
		log.Fatalf("node1 redis ping failed: %v", err)
	}
	if err := ensureGroup(ctx, rdbNode1, streamKey, group); err != nil {
		log.Fatalf("unable to ensure stream group: %v", err)
	}

	optLocal, err := redis.ParseURL(node2RedisURL)
	if err != nil {
		log.Fatalf("invalid NODE2_REDIS_URL: %v", err)
	}
	rdbLocal := redis.NewClient(optLocal)
	defer rdbLocal.Close()

	if err := rdbLocal.Ping(ctx).Err(); err != nil {
		log.Fatalf("node2 local redis ping failed: %v", err)
	}

	log.Printf("worker started: stream=%s group=%s consumer=%s goroutines=%d", streamKey, group, consumerBase, workerCount)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("received signal: %s", sig)
		cancel()
	}()

	go retryFailedStatus(ctx, rdbLocal, rdbNode1, failedStatusKey, statusStreamKey, retryInterval)

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

				res, err := rdbNode1.XReadGroup(ctx, &redis.XReadGroupArgs{
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
						jobID, _ := msg.Values["job_id"].(string)
						typeVal, _ := msg.Values["type"].(string)
						priority, _ := msg.Values["priority"].(string)

						log.Printf("processing message id=%s job_id=%s type=%s priority=%s consumer=%s", msg.ID, jobID, typeVal, priority, consumer)

						if err := publishStatus(ctx, rdbNode1, statusStreamKey, jobID, "processing"); err != nil {
							log.Printf("publish processing failed job_id=%s: %v", jobID, err)
							continue // 不 XACK，讓 PEL reclaim
						}

						time.Sleep(processDelay)

						if err := publishStatus(ctx, rdbNode1, statusStreamKey, jobID, "done"); err != nil {
							log.Printf("publish done failed job_id=%s, saving to local fallback: %v", jobID, err)
							entry, _ := json.Marshal(failedEntry{JobID: jobID, Status: "done"})
							if pushErr := rdbLocal.RPush(ctx, failedStatusKey, string(entry)).Err(); pushErr != nil {
								log.Printf("fallback push failed job_id=%s: %v", jobID, pushErr)
							}
						}

						if err := rdbNode1.XAck(ctx, streamKey, group, msg.ID).Err(); err != nil {
							log.Printf("xack failed id=%s: %v", msg.ID, err)
						}
					}
				}
			}
		}(i)
	}

	wg.Wait()
}
