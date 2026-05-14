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

const maxRetries = 3

type failedEntry struct {
	JobID   string `json:"job_id"`
	Status  string `json:"status"`
	Retries int    `json:"retries"`
}

func publishStatus(ctx context.Context, rdb *redis.Client, stream, jobID, status string) error {
	return rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		MaxLen: 50000,
		Approx: true,
		Values: map[string]any{
			"job_id": jobID,
			"status": status,
		},
	}).Err()
}

func publishTerminal(ctx context.Context, rdbNode1 *redis.Client, statusStreamKey, fallbackFile, jobID, status string, entry failedEntry) {
	if err := publishStatus(ctx, rdbNode1, statusStreamKey, jobID, status); err != nil {
		log.Printf("publish %s failed job_id=%s, saving to file: %v", status, jobID, err)
		data, marshalErr := json.Marshal(entry)
		if marshalErr != nil {
			log.Printf("marshal failed job_id=%s: %v", jobID, marshalErr)
			return
		}
		appendFallbackFile(fallbackFile, data)
	}
}

func processMessage(ctx context.Context, rdbNode1, rdbLocal *redis.Client, msg redis.XMessage, cfg workerConfig, consumer string) {
	sm, err := parseStreamMessage(msg)
	if err != nil {
		log.Printf("parse message failed id=%s: %v, moving to dead", msg.ID, err)
		jobID, _ := msg.Values["job_id"].(string)
		publishTerminal(ctx, rdbNode1, cfg.statusStreamKey, cfg.fallbackFile, jobID, "dead", failedEntry{JobID: jobID, Status: "dead"})
		if ackErr := rdbNode1.XAck(ctx, cfg.streamKey, cfg.group, msg.ID).Err(); ackErr != nil {
			log.Printf("xack failed id=%s: %v", msg.ID, ackErr)
		}
		return
	}

	if ackErr := rdbNode1.XAck(ctx, cfg.streamKey, cfg.group, msg.ID).Err(); ackErr != nil {
		log.Printf("xack failed id=%s: %v", msg.ID, ackErr)
	}

	if pubErr := publishStatus(ctx, rdbNode1, cfg.statusStreamKey, sm.JobID, "processing"); pubErr != nil {
		log.Printf("publish processing failed job_id=%s: %v", sm.JobID, pubErr)
		data, _ := json.Marshal(failedEntry{JobID: sm.JobID, Status: "processing"})
		appendFallbackFile(cfg.fallbackFile, data)
	}

	log.Printf("processing id=%s job_id=%s type=%s priority=%d user_id=%d action=%s consumer=%s",
		msg.ID, sm.JobID, sm.Type, sm.Priority, sm.Payload.UserID, sm.Payload.Action, consumer)

	if strings.Contains(sm.Payload.Action, "force_fail") {
		log.Printf("simulated failure job_id=%s action=%s", sm.JobID, sm.Payload.Action)
		entry := failedEntry{JobID: sm.JobID, Status: "failed:force_fail"}
		data, marshalErr := json.Marshal(entry)
		if marshalErr != nil {
			log.Printf("force_fail marshal failed job_id=%s: %v", sm.JobID, marshalErr)
		} else if pushErr := rdbLocal.RPush(ctx, cfg.failedStatusKey, string(data)).Err(); pushErr != nil {
			log.Printf("force_fail rpush failed job_id=%s: %v", sm.JobID, pushErr)
		}
	} else {
		time.Sleep(cfg.processDelay)
		publishTerminal(ctx, rdbNode1, cfg.statusStreamKey, cfg.fallbackFile, sm.JobID, "done", failedEntry{JobID: sm.JobID, Status: "done"})
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
			log.Printf("xreadgroup error consumer=%s: %v", consumer, err)
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

// TODO: force_fail entries re-pushed within the same tick can be immediately re-popped,
// causing all 3 retries to exhaust in a single interval. Fix by collecting entries to
// re-push after the inner loop drains the list, so each retry waits for the next tick.
func retryFailedStatus(ctx context.Context, rdbLocal, rdbNode1 *redis.Client, failedKey, fallbackFile, statusStream string, interval time.Duration) {
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

				switch entry.Status {
				case "failed:force_fail":
					entry.Retries++
					if entry.Retries >= maxRetries {
						log.Printf("retry: max retries reached job_id=%s, escalating to dead", entry.JobID)
						deadEntry := failedEntry{JobID: entry.JobID, Status: "dead", Retries: entry.Retries}
						publishTerminal(ctx, rdbNode1, statusStream, fallbackFile, entry.JobID, "dead", deadEntry)
					} else {
						log.Printf("retry: re-queuing force_fail job_id=%s attempt=%d/%d", entry.JobID, entry.Retries, maxRetries)
						updated, marshalErr := json.Marshal(entry)
						if marshalErr != nil {
							log.Printf("retry: marshal failed job_id=%s: %v", entry.JobID, marshalErr)
							break
						}
						if pushErr := rdbLocal.RPush(ctx, failedKey, string(updated)).Err(); pushErr != nil {
							log.Printf("retry: re-queue failed job_id=%s: %v", entry.JobID, pushErr)
						}
					}

				default:
					log.Printf("retry: unknown status job_id=%s status=%s, skipping", entry.JobID, entry.Status)
					break
				}

				time.Sleep(500 * time.Millisecond)
			}
		}
	}
}
