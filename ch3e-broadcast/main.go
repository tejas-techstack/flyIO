package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n            *maelstrom.Node
	store        []any
	nodeTopology []any
	seenMessages map[any]bool

	toFlush      []any
	mu           sync.Mutex
	wg           sync.WaitGroup
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
	s.n.Handle("flush", s.flushHandler)

	go s.startPeriodicFlush()

	if err := s.n.Run(); err != nil {
		log.Println("Run Error:", err)
		os.Exit(1)
	}
}

func (s *server) startPeriodicFlush() {
	ticker := time.NewTicker(5 * time.Second) 
	defer ticker.Stop()

	for range ticker.C {
		log.Println("Triggering periodic flush")
		err := s.flush()
		if err != nil {
			log.Println("Flush Error:", err)
		}
	}
}

func unmarshalJSON(msg maelstrom.Message) (map[string]any, error) {
	var body anymap

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil, err
	}

	return body, nil
}

func (s *server) selectRandomNodes(allNodes []string, percentage float64) []any {
	totalNodes := len(allNodes)
	numToSelect := int(float64(totalNodes) * percentage)

	selectedNodes := make(map[string]bool)
	var result []any

	for len(result) < numToSelect {
		node := allNodes[rand.Intn(totalNodes)]
		if node != s.n.ID() && !selectedNodes[node] {
			selectedNodes[node] = true
			result = append(result, node)
		}
	}

	return result
}

func (s *server) topology(msg maelstrom.Message) error {
	allNodes := s.n.NodeIDs()

	s.mu.Lock()
	s.nodeTopology = s.selectRandomNodes(allNodes, 0.2)
	s.mu.Unlock()

	log.Printf("Randomized topology for %s: %v", s.n.ID(), s.nodeTopology)

	return s.n.Reply(msg, anymap{"type": "topology_ok"})
}

func (s *server) read(msg maelstrom.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.n.Reply(msg, anymap{"type": "read_ok", "messages": s.store})
}

func (s *server) broadcast(msg maelstrom.Message) error {
	body, err := unmarshalJSON(msg)
	if err != nil {
		return err
	}

	message := body["message"]
	s.mu.Lock()
	if _, seen := s.seenMessages[message]; seen {
		s.mu.Unlock()
		return nil
	}
	s.seenMessages[message] = true
	s.store = append(s.store, message)
	s.mu.Unlock()

	s.mu.Lock()
	for _, node := range s.nodeTopology {
		if node == s.n.ID() {
			continue
		}
		if err := s.n.Send(node.(string), anymap{"type": "broadcast", "message": message, "fromNode": true}); err != nil {
			log.Println(err)
		}
	}
	s.mu.Unlock()

	if body["fromNode"] != true {
		return s.n.Reply(msg, anymap{"type": "broadcast_ok"})
	}

	return nil
}

func (s *server) flushHandler(msg maelstrom.Message) error {
	log.Println("Handling Flush")
	body, err := unmarshalJSON(msg)
	if err != nil {
		return err
	}

	for _, val := range body["message"].([]any) {
		s.mu.Lock()
		if _, seen := s.seenMessages[val]; seen {
			s.mu.Unlock()
			continue
		}
		s.seenMessages[val] = true
		s.store = append(s.store, val)
		s.mu.Unlock()
	}

	return nil
}

func (s *server) flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Println("Beginning Flush")
	for _, node := range s.n.NodeIDs() {
		if node == s.n.ID() {
			continue
		}
		if err := s.n.Send(node, anymap{"type": "flush", "message": s.store}); err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

