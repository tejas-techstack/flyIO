package main

import (
  "encoding/json"
  "log"
  "os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main(){
  n := maelstrom.NewNode()
  var queue []any

  n.Handle("topology", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil{
      return err
    }

    response := map[string]any{
      "type" : "topology_ok",
    }
      
    return n.Reply(msg, response)
  })

  n.Handle("broadcast", func(msg maelstrom.Message) error {
    var body map[string]any

    if err := json.Unmarshal(msg.Body, &body); err != nil{
      return err
    }

    queue = append(queue, body["message"])

    response := map[string]any{
      "type" : "broadcast_ok",
    }

    return n.Reply(msg, response)
  })

  n.Handle("read", func(msg maelstrom.Message) error {
    var body map[string]any

    if err := json.Unmarshal(msg.Body, &body); err != nil{
      return err
    }

    response := map[string]any{
      "type" : "read_ok",
      "messages" : queue,
    }

    return n.Reply(msg, response)
  })


  if err := n.Run(); err != nil{
    log.Fatal(err)
    os.Exit(1)
  }
}

