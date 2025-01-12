package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	msgBroadcast = "broadcast"
	msgRead      = "read"
	msgTopology  = "topology"
)

const (
	respBroadcastOk = "broadcast_ok"
	respReadOk      = "read_ok"
	respTopologyOk  = "topology_ok"
)

type BroadcastMessage struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
	MsgID   int    `json:"msg_id"`
}

type Server struct {
	node     *maelstrom.Node
	messages []int
}

func NewServer() *Server {
	return &Server{
		node:     maelstrom.NewNode(),
		messages: make([]int, 0),
	}
}

func (s *Server) handleBroadcast(msg maelstrom.Message) error {
	var body BroadcastMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.messages = append(s.messages, body.Message)

	return s.node.Reply(msg, map[string]any{"type": respBroadcastOk})
}

func (s *Server) handleRead(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]any{"type": respReadOk, "messages": s.messages})
}

func (s *Server) handleTopology(msg maelstrom.Message) error {
	return s.node.Reply(msg, map[string]any{"type": respTopologyOk})
}

func main() {
	server := NewServer()

	server.node.Handle(msgBroadcast, server.handleBroadcast)
	server.node.Handle(msgRead, server.handleRead)
	server.node.Handle(msgTopology, server.handleTopology)

	if err := server.node.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
