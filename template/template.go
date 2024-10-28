package main

import (
	"encoding/json"
	"log"
	// "math/rand"
	"os"
	"sync"
	// "time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n            *maelstrom.Node

	mu           sync.Mutex
	wg           sync.WaitGroup
}

type anymap map[string]any

func unmarshalJSON(msg maelstrom.Message) (map[string]any, error) {
	var body anymap

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil, err
	}

	return body, nil
}

func main(){
  s := server{}
	s.n = maelstrom.NewNode()

	if err := s.n.Run(); err != nil {
		log.Println("Run Error:", err)
		os.Exit(1)
	}
}



