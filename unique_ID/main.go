/*

Date: 2/10/2024
Author: Tejas Ramakrishnan

*/

package main

import (
  "encoding/json"
  "fmt"
  "log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)


func main(){
  n := maelstrom.NewNode()

  // Create handler for generate
  n.Handle("generate", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil{
      return err
    }

    // the pair of destination and msg_id is always unique for any request
    // leverage this to create a unique id for each request recived
    // since this will always be unique
    //
    //
    // This is a quick solution for this although it wasn't intended to be solved like this

    body["type"] = "generate_ok"
    body["id"] = fmt.Sprintf("%v%v", msg.Dest, body["msg_id"])

    // reply with updated body
    return n.Reply(msg, body)
  })
  
  // run the node
  if err := n.Run(); err != nil {
    log.Fatal(err)
  }
}
