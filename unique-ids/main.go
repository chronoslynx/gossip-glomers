package main

import (
	"encoding/json"
	"log"
	"sort"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	// Strided ID generation. Because nodes neither join nor leave each can safely generate ids at
	// a stride with width len(nodes).
	//
	// The first ID is the current node's offset in a sorted list of IDs.
	// This does not work if nodes can join or leave the cluster, or if sequential IDs are required.
	var nextID uint64
	var nodes []string
	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		if nodes == nil {
			nodes = n.NodeIDs()
			sort.Strings(nodes)
			nextID = uint64(sort.SearchStrings(nodes, n.ID()))
		}
		body["type"] = "generate_ok"
		body["id"] = nextID
		nextID += uint64(len(nodes))
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
