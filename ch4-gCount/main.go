package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"sync"
	// "time"
  "context"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n            *maelstrom.Node
  kv           *maelstrom.KV
  prevWrite    float64

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
	s.kv = maelstrom.NewSeqKV(s.n)

  randWrite := rand.Float64()
  s.prevWrite = randWrite
  err := s.kv.Write(context.Background(), "randomWrite", s.prevWrite)
  if err != nil {
    log.Println("randomWrite Error: ",err)
    os.Exit(1)
  }

  _, err = s.kv.ReadInt(context.Background(), "counter")
  if err != nil{
    writeErr := s.kv.Write(context.Background(), "counter", 0)
    if writeErr != nil{
      log.Println("Initate Error:", err)
      os.Exit(1)
    }
  }

  s.n.Handle("read", s.read)
  s.n.Handle("add", s.add)

	if err := s.n.Run(); err != nil {
		log.Println("Run Error:", err)
		os.Exit(1)
	}
}

func (s *server) read(msg maelstrom.Message) error {
  
  s.mu.Lock()

  // Write a random float value to a different key
  // to ensure state change during compare and swap
  value, _ := s.kv.ReadInt(context.Background(), "counter")
  randWrite := rand.Float64()
  err := s.kv.CompareAndSwap(context.Background(), "randomWrite", s.prevWrite, randWrite, true)
  for maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed{
    value, _ = s.kv.ReadInt(context.Background(), "counter")
    err = s.kv.CompareAndSwap(context.Background(), "randomWrite", s.prevWrite, randWrite, true)
  }
  s.prevWrite = randWrite

  s.mu.Unlock()

  return s.n.Reply(msg, anymap{"type": "read_ok", "value" : value,})
} 

func (s * server) add(msg maelstrom.Message) error {

  body, err := unmarshalJSON(msg)
  if err != nil{
    return err
  }

  s.mu.Lock()
  randWrite := rand.Float64()
  err = s.kv.CompareAndSwap(context.Background(), "randomWrite", s.prevWrite, randWrite, true)
  for maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed{
    oldValue, err := s.kv.ReadInt(context.Background(), "counter")
    err = s.kv.CompareAndSwap(context.Background(), "randomWrite", s.prevWrite, randWrite, true)
  }
  s.prevWrite = randWrite
  s.mu.Unlock()

  delta := int(body["delta"].(float64))
  updateCounterWith := oldValue + delta

  // perform write with most recent read
  s.mu.Lock()
  err = s.kv.Write(context.Background(), "counter", updateCounterWith)
  s.mu.Unlock()
  if err != nil{
    return err
  }

  return s.n.Reply(msg, anymap{"type": "add_ok",})
}
