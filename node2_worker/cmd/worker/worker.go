package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type failedEntry struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

func publishStatus(ctx context.Context, rdb *redis.Client, stream, jobID, status string) error {
	return rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{
			"job_id": jobID,
			"status": status,
		},
	}).Err()
}

func processMessage(ctx context.Context, rdbNode1, rdbLocal *redis.Client, msg redis.XMessage, cfg workerConfig, consumer string) {
	sm, err := parseStreamMessage(msg)
	if err != nil {
		log.Printf("parse message failed id=%s: %v, discarding", msg.ID, err)
		if ackErr := rdbNode1.XAck(ctx, cfg.streamKey, cfg.group, msg.ID).Err(); ackErr != nil {
			log.Printf("xack failed id=%s: %v", msg.ID, ackErr)
		}
		return
	}

	log.Printf("processing message id=%s job_id=%s type=%s priority=%d user_id=%d action=%s consumer=%s",
		msg.ID, sm.JobID, sm.Type, sm.Priority, sm.Payload.UserID, sm.Payload.Action, consumer)

	if err := publishStatus(ctx, rdbNode1, cfg.statusStreamKey, sm.JobID, "processing"); err != nil {
		log.Printf("publish processing failed job_id=%s: %v", sm.JobID, err)
		return // 不 XACK，讓 PEL reclaim
	}

	if strings.Contains(sm.Payload.Action, "force_fail") {
		log.Printf("simulated failure job_id=%s action=%s", sm.JobID, sm.Payload.Action)
		if err := publishStatus(ctx, rdbNode1, cfg.statusStreamKey, sm.JobID, "failed"); err != nil {
			log.Printf("publish failed status error job_id=%s: %v", sm.JobID, err)
		}
		if err := rdbNode1.XAck(ctx, cfg.streamKey, cfg.group, msg.ID).Err(); err != nil {
			log.Printf("xack failed id=%s: %v", msg.ID, err)
		}
		return
	}

	time.Sleep(cfg.processDelay)

	if err := publishStatus(ctx, rdbNode1, cfg.statusStreamKey, sm.JobID, "done"); err != nil {
		log.Printf("publish done failed job_id=%s, saving to local fallback: %v", sm.JobID, err)
		entry, marshalErr := json.Marshal(failedEntry{JobID: sm.JobID, Status: "done"})
		if marshalErr != nil {
			log.Printf("fallback marshal failed job_id=%s: %v", sm.JobID, marshalErr)
		} else if pushErr := rdbLocal.RPush(ctx, cfg.failedStatusKey, string(entry)).Err(); pushErr != nil {
			log.Printf("fallback push failed job_id=%s: %v", sm.JobID, pushErr)
		}
	}

	if err := rdbNode1.XAck(ctx, cfg.streamKey, cfg.group, msg.ID).Err(); err != nil {
		log.Printf("xack failed id=%s: %v", msg.ID, err)
	}
}

func runWorker(ctx context.Context, rdbNode1, rdbLocal *redis.Client, cfg appConfig, workerID int) {
	consumer := fmt.Sprintf("%s-%d", cfg.consumerBase, workerID+1)
	log.Printf("worker goroutine started: consumer=%s", consumer)

	for {
		if ctx.Err() != nil {
			log.Printf("worker goroutine stopped: consumer=%s", consumer)
			return
		}

		res, err := rdbNode1.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    cfg.worker.group,
			Consumer: consumer,
			Streams:  []string{cfg.worker.streamKey, ">"},
			Count:    cfg.batchSize,
			Block:    cfg.blockMS,
		}).Result()
		if err != nil {
			if err == redis.Nil || ctx.Err() != nil {
				continue
			}
			log.Printf("xreadgroup error (consumer=%s): %v", consumer, err)
			time.Sleep(1 * time.Second)
			continue
		}

		if len(res) > 0 {
			for _, msg := range res[0].Messages {
				processMessage(ctx, rdbNode1, rdbLocal, msg, cfg.worker, consumer)
			}
		}
	}
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
