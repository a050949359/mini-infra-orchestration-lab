package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type JobPayload struct {
	UserID int    `json:"user_id"`
	Action string `json:"action"`
	Data   string `json:"data"`
}

type StreamMessage struct {
	JobID     string
	Status    string
	Type      string
	Priority  int
	Payload   JobPayload
	CreatedAt string
}

func parseStreamMessage(msg redis.XMessage) (StreamMessage, error) {
	str := func(key string) string {
		v, _ := msg.Values[key].(string)
		return v
	}

	priority, err := strconv.Atoi(str("priority"))
	if err != nil {
		return StreamMessage{}, fmt.Errorf("invalid priority %q: %w", str("priority"), err)
	}

	var payload JobPayload
	if err := json.Unmarshal([]byte(str("payload")), &payload); err != nil {
		return StreamMessage{}, fmt.Errorf("invalid payload: %w", err)
	}

	return StreamMessage{
		JobID:     str("job_id"),
		Status:    str("status"),
		Type:      str("type"),
		Priority:  priority,
		Payload:   payload,
		CreatedAt: str("created_at"),
	}, nil
}
