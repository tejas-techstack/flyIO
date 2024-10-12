// TODO implementing Gossip protocol.

package main

import (
  "encoding/json"
  //"context"
  "log"
  "os"
  //"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct{
  n *maelstrom.Node;
  nodeID string;
  nodes []string;

  store []any;
}

type anymap map[string]any

func makeServer() (* server){
  var s * server
  s.n = maelstrom.NewNode()
  s.nodeID = s.n.ID()
  s.nodes = s.n.NodeIDs()

  return s
}

func main(){
  s := makeServer();
  log.Println(s)

  s.n.Handle("topology", s.topology);
  s.n.Handle("read", s.read);
  s.n.Handle("broadcast", s.broadcast);

  if err := s.n.Run(); err != nil{
    log.Println("Run Error: ", err)
    os.Exit(1)
  }
}

func unmarshalJSON(msg maelstrom.Message) (map[string]any, error) {
  var body anymap

  if err := json.Unmarshal(msg.Body, &body); err != nil{
    return nil, err
  }

  return body, nil
}

func (s * server) topology(msg maelstrom.Message) error {
  return s.n.Reply(msg, anymap{"type":"topology_ok",});
}

func (s * server) read(msg maelstrom.Message) error {
  
  body, err := unmarshalJSON(msg)
  if err != nil {return err}

  s.store = append(s.store, body["message"])

  return s.n.Reply(msg, anymap{"type": "read_ok","messages":s.store})
}

func (s * server) broadcast(msg maelstrom.Message) error {
  


  return s.n.Reply(msg, anymap{"type": "broadcast_ok",})
}

