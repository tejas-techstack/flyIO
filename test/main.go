// TODO: Implementing Gossip protocol.

package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n            *maelstrom.Node
	store        []any
	nodeTopology []any
	seenMessages map[any]bool
	mu           sync.Mutex // Mutex to protect shared resources
	wg           sync.WaitGroup // WaitGroup to manage goroutines
}

type anymap map[string]any

func main() {
	s := server{
		seenMessages: make(map[any]bool),
	}
	s.n = maelstrom.NewNode()

	s.n.Handle("topology", s.topology)
	s.n.Handle("read", s.read)
	s.n.Handle("broadcast", s.broadcast)

	if err := s.n.Run(); err != nil {
		log.Println("Run Error:", err)
		os.Exit(1)
	}
}

func unmarshalJSON(msg maelstrom.Message) (map[string]any, error) {
	var body anymap

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil, err
	}

	return body, nil
}

func (s *server) topology(msg maelstrom.Message) error {
	body, err := unmarshalJSON(msg)
	if err != nil {
		return err
	}

	s.mu.Lock() // Lock to protect shared resource
	s.nodeTopology = body["topology"].(map[string]any)[s.n.ID()].([]any)
	s.mu.Unlock()

	log.Printf("Topology for %s: %v", s.n.ID(), s.nodeTopology)

	return s.n.Reply(msg, anymap{"type": "topology_ok"})
}

func (s *server) read(msg maelstrom.Message) error {
	s.mu.Lock() // Lock to protect shared resource
	defer s.mu.Unlock()
	return s.n.Reply(msg, anymap{"type": "read_ok", "messages": s.store})
}

func (s *server) broadcast(msg maelstrom.Message) error {
	body, err := unmarshalJSON(msg)
	if err != nil {
		return err
	}

	message := body["message"]
	s.mu.Lock() // Lock to protect shared resource
	if _, seen := s.seenMessages[message]; seen {
		s.mu.Unlock() // Unlock before returning
		return nil // Ignore already seen messages
	}
	s.seenMessages[message] = true
	s.store = append(s.store, message)
	s.mu.Unlock() // Unlock after modifying shared resources

	for _, node := range s.nodeTopology {
		if node == s.n.ID() {
			continue
		}
		s.wg.Add(1) // Increment WaitGroup counter
		go func(node any) {
			defer s.wg.Done() // Decrement counter when goroutine completes
			if err := s.n.Send(node.(string), anymap{"type": "broadcast", "message": message, "fromNode": true}); err != nil {
				log.Printf("Failed to send message to %s: %v", node, err)
			}
		}(node)
	}

	if body["fromNode"] != true {
		return s.n.Reply(msg, anymap{"type": "broadcast_ok"})
	}

	return nil
}

