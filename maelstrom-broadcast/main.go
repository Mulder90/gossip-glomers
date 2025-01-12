package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMessage struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
	MsgID   int    `json:"msg_id"`
}

func main() {
	n := maelstrom.NewNode()

	var received_messages []int

	n.Handle("init", func(msg maelstrom.Message) error {
		received_messages = []int{}
		return n.Reply(msg, map[string]any{"type": "init_ok"})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		received_messages = append(received_messages, body.Message)

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": received_messages})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
