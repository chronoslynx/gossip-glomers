package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	gossip "glomers/gossip"
)

type Broadcast struct {
	MsgType string `json:"type"`
	Message int    `json:"message"`
}

type Bulk struct {
	MsgType  string `json:"type"`
	Messages []int  `json:"messages"`
}

type Topology struct {
	Neighbors map[string][]string `json:"topology"`
}

var broadcastOk = json.RawMessage(`{"type":"broadcast_ok"}`)
var topoOk = json.RawMessage(`{"type":"topology_ok"}`)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := maelstrom.NewNode()
	block := new(sync.RWMutex)
	var broadcasts []int
	var pool *gossip.Heap[int]

	applyUpdate := func(msg int) {
		block.Lock()
		defer block.Unlock()
		broadcasts = append(broadcasts, msg)
	}

	n.Handle("init", func(_ maelstrom.Message) error {
		pool = gossip.NewHeap(n, applyUpdate, 100*time.Millisecond)
		go pool.Run(ctx)
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var bcast Broadcast
		if err := json.Unmarshal(msg.Body, &bcast); err != nil {
			return err
		}
		pool.Apply(bcast.Message)

		return n.Reply(msg, broadcastOk)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		block.RLock()
		bcasts := make([]int, len(broadcasts))
		copy(bcasts, broadcasts)
		block.RUnlock()

		return n.Reply(msg, Bulk{
			MsgType:  "read_ok",
			Messages: bcasts,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, topoOk)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
