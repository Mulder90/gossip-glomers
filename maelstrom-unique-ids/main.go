package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Snowflake struct {
	mu            sync.Mutex
	lastTimestamp int64
	nodeId        int64
	sequence      int64
}

func NewSlowflake(nodeId int64) *Snowflake {
	return &Snowflake{nodeId: nodeId}
}

func (s *Snowflake) waitForNextMillis() int64 {
	timestamp := time.Now().UnixMilli()
	for timestamp <= s.lastTimestamp {
		time.Sleep(1 * time.Millisecond)
		timestamp = time.Now().UnixMilli()
	}

	return timestamp
}

func (s *Snowflake) NextId() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	timestamp := time.Now().UnixMilli()

	if timestamp == s.lastTimestamp {
		s.sequence = (s.sequence + 1) & 4095
		if s.sequence == 0 {
			timestamp = s.waitForNextMillis()
		}
	} else {
		s.sequence = 0
	}

	s.lastTimestamp = timestamp

	return strconv.FormatInt(s.lastTimestamp<<22|s.nodeId<<12|s.sequence, 2)
}

func main() {
	n := maelstrom.NewNode()
	var snowflake *Snowflake

	n.Handle("init", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		nodeId := strings.TrimPrefix(n.ID(), "n")
		parsedNodeId, err := strconv.ParseInt(nodeId, 10, 64)
		if err != nil {
			return err
		}
		snowflake = NewSlowflake(parsedNodeId)

		return n.Reply(msg, map[string]any{"type": "init_ok"})
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		response := map[string]any{
			"type": "generate_ok",
			"id":   snowflake.NextId(),
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
