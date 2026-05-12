package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
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

func main() {
	redisURL := getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
	streamKey := getenv("QUEUE_STREAM_KEY", "jobs:stream")
	group := getenv("QUEUE_GROUP", "node2-workers")
	consumer := getenv("QUEUE_CONSUMER", "node2-worker-1")
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

	log.Printf("worker started: stream=%s group=%s consumer=%s", streamKey, group, consumer)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("received signal: %s", sig)
		cancel()
	}()

	for {
		if ctx.Err() != nil {
			log.Println("worker shutting down")
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
			log.Printf("xreadgroup error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, stream := range res {
			for _, msg := range stream.Messages {
				jobID := fmt.Sprintf("%v", msg.Values["job_id"])
				typeVal := fmt.Sprintf("%v", msg.Values["type"])
				priority := fmt.Sprintf("%v", msg.Values["priority"])

				// Placeholder job processing logic.
				log.Printf("processing message id=%s job_id=%s type=%s priority=%s", msg.ID, jobID, typeVal, priority)

				if err := rdb.XAck(ctx, streamKey, group, msg.ID).Err(); err != nil {
					log.Printf("xack failed id=%s: %v", msg.ID, err)
					continue
				}
			}
		}
	}
}
