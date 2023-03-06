package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"glomers/gossip"
	"glomers/node"
)

type Delta struct {
	MsgType string `json:"type"`
	Delta   uint64 `json:"delta"`
}

type Read struct {
	MsgType string `json:"type"`
	Value   uint64 `json:"value"`
}

var addOk = json.RawMessage(`{"type":"add_ok"}`)
var topoOk = json.RawMessage(`{"type":"topology_ok"}`)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pool *gossip.Heap[uint64]

	var value uint64 = 0
	n := node.New(ctx, func(n node.Node) error {
		pool = gossip.NewHeap[uint64](n, 20*time.Millisecond)
		go pool.Run(ctx)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case delta := <-pool.Gossip:
					// writes are serialized so we don't need atomics
					value += delta
				}
			}
		}()
		return nil
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var m Delta
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return err
		}
		pool.Apply(m.Delta)

		return n.Reply(msg, addOk)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// We don't care if our read is stale
		return n.Reply(msg, Read{
			MsgType: "read_ok",
			Value:   value,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
