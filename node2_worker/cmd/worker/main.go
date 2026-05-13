package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	log.SetOutput(os.Stdout)
	cfg := loadConfig()
	closeLog := setupLogOutput(cfg.logFile)
	defer closeLog()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdbNode1 := mustRedisClient(ctx, cfg.redisURL, "node1")
	defer rdbNode1.Close()

	rdbLocal := mustRedisClient(ctx, cfg.node2RedisURL, "node2-local")
	defer rdbLocal.Close()

	if err := ensureGroup(ctx, rdbNode1, cfg.worker.streamKey, cfg.worker.group); err != nil {
		log.Fatalf("unable to ensure stream group: %v", err)
	}

	log.Printf("worker started: stream=%s group=%s consumer=%s goroutines=%d",
		cfg.worker.streamKey, cfg.worker.group, cfg.consumerBase, cfg.workerCount)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("received signal: %s", sig)
		cancel()
	}()

	go retryFailedStatus(ctx, rdbLocal, rdbNode1, cfg.worker.failedStatusKey, cfg.worker.deadStatusKey, cfg.worker.statusStreamKey, cfg.retryInterval)

	var wg sync.WaitGroup
	for i := 0; i < cfg.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, rdbNode1, rdbLocal, cfg, workerID)
		}(i)
	}

	wg.Wait()
}
