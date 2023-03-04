package main

import (
	"context"
	"encoding/json"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	gossip "glomers/gossip"
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

	n := maelstrom.NewNode()

	var pool *gossip.Heap[uint64]
	var value uint64 = 0

	applyUpdate := func(delta uint64) {
		atomic.AddUint64(&value, delta)
	}

	n.Handle("init", func(_ maelstrom.Message) error {
		pool = gossip.NewHeap(n, applyUpdate, 20)
		go pool.Run(ctx)
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
		return n.Reply(msg, Read{
			MsgType: "read_ok",
			Value:   atomic.LoadUint64(&value),
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, topoOk)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
