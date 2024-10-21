package main

import (
  "encoding/json"
  "log"
  "os"
  "fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main(){
  n := maelstrom.NewNode()
  var queue []any
  topology := make(map[string]any)

  n.Handle("topology", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil{
      return err
    }

    for i,_ := range n.NodeIDs(){
      if (i+1) == len(n.NodeIDs()){
        topology[n.NodeIDs()[i]] = n.NodeIDs()[0]
      } else {
        topology[n.NodeIDs()[i]] = n.NodeIDs()[i+1]
      }
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

    propogate_details := map[string]any{
      "type" : "propogate",
      "message" : body["message"],
      "og_sender" : n.ID(),
    }

    dest := fmt.Sprintf("%v", topology[n.ID()])
    if err := n.Send(dest, propogate_details); err != nil{
      return err
    }

    response := map[string]any{
      "type" : "broadcast_ok",
    }

    return n.Reply(msg, response)
  })

  n.Handle("propogate", func(msg maelstrom.Message) error {
    var body map[string]any

    if err := json.Unmarshal(msg.Body, &body); err != nil{
      return err
    }

    queue = append(queue, body["message"])

    propogate_details := map[string]any{
      "type" : "propogate",
      "message" : body["message"],
      "og_sender" : body["og_sender"],
    }

    if (body["og_sender"] != topology[n.ID()]){
     dest := fmt.Sprintf("%v", topology[n.ID()])
     n.Send(dest, propogate_details)
    }

    return nil
  })

  n.Handle("read", func(msg maelstrom.Message) error {
    log.Println(msg)
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

