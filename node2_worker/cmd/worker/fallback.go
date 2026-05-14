package main

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var fallbackMu sync.Mutex

func appendFallbackFile(path string, data []byte) {
	fallbackMu.Lock()
	defer fallbackMu.Unlock()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("fallback file open failed path=%s: %v", path, err)
		return
	}
	defer f.Close()
	if _, err := f.Write(append(data, '\n')); err != nil {
		log.Printf("fallback file write failed path=%s: %v", path, err)
	}
}

func replayFallbackFile(ctx context.Context, rdb *redis.Client, path, statusStream string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			replayOnce(ctx, rdb, path, statusStream)
		}
	}
}

func replayOnce(ctx context.Context, rdb *redis.Client, path, statusStream string) {
	fallbackMu.Lock()
	defer fallbackMu.Unlock()

	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		log.Printf("fallback replay: open failed: %v", err)
		return
	}

	var remaining [][]byte
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var entry failedEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			log.Printf("fallback replay: unmarshal failed val=%s: %v", line, err)
			remaining = append(remaining, append([]byte{}, line...))
			continue
		}
		if err := publishStatus(ctx, rdb, statusStream, entry.JobID, entry.Status); err != nil {
			log.Printf("fallback replay: publish failed job_id=%s status=%s: %v", entry.JobID, entry.Status, err)
			remaining = append(remaining, append([]byte{}, line...))
		} else {
			log.Printf("fallback replay: published job_id=%s status=%s", entry.JobID, entry.Status)
		}
	}
	f.Close()

	if err := scanner.Err(); err != nil {
		log.Printf("fallback replay: scan error: %v", err)
		return
	}

	if err := rewriteFallbackFile(path, remaining); err != nil {
		log.Printf("fallback replay: rewrite failed: %v", err)
	}
}

func rewriteFallbackFile(path string, lines [][]byte) error {
	if len(lines) == 0 {
		return os.Remove(path)
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	for _, line := range lines {
		if _, err := f.Write(append(line, '\n')); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}
