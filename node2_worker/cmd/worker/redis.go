package main

import (
	"context"
	"log"
	"strings"

	"github.com/redis/go-redis/v9"
)

func mustRedisClient(ctx context.Context, url, label string) *redis.Client {
	opt, err := redis.ParseURL(url)
	if err != nil {
		log.Fatalf("%s: invalid redis URL: %v", label, err)
	}
	rdb := redis.NewClient(opt)
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("%s: ping failed: %v", label, err)
	}
	return rdb
}

func ensureGroup(ctx context.Context, rdb *redis.Client, stream, group string) error {
	err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err == nil || strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}
