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
  store []any;
}

type anymap map[string]any

func main(){
  s := server{};
  s.n = maelstrom.NewNode()

  // log.Println(s)

  s.n.Handle("topology", s.topology);
  s.n.Handle("read", s.read);
  s.n.Handle("broadcast", s.broadcast);
  s.n.Handle("unicast", s.unicast);

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
  return s.n.Reply(msg, anymap{"type": "read_ok","messages":s.store})
}

func (s *server) broadcast(msg maelstrom.Message) error {
  body, err := unmarshalJSON(msg)
  if err != nil {
    return err
  }

  message := body["message"]
  s.store = append(s.store, message)

  log.Println(s.n.NodeIDs())
  for _, node := range s.n.NodeIDs() {
    if node == s.n.ID() {
      continue
    }

    // log.Printf("%v, %T\n", node, node)
    s.n.Send(node, anymap{"type": "unicast", "message":body["message"],})
  }

  return s.n.Reply(msg, anymap{"type": "broadcast_ok"})
}

func (s * server) unicast(msg maelstrom.Message) error {
  body, err := unmarshalJSON(msg)
  if err != nil {
    return err
  }
  s.store = append(s.store, body["message"])

  return nil
}

