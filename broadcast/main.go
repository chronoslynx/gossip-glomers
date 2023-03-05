package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"glomers/gossip"
	"glomers/node"
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

	var pool *gossip.Heap[int]

	initialized := make(chan struct{})

	n := node.New(ctx, func(n node.Node) error {
		pool = gossip.NewHeap[int](n, 100*time.Millisecond)
		go pool.Run(ctx)
		close(initialized)
		return nil
	})

	bcasts, bcastOK := node.Handle[Broadcast, json.RawMessage](n, "broadcast")
	reads, replies := node.Handle[struct{}, Bulk](n, "read")

	go func() {
		var broadcasts []int
		select {
		case <-ctx.Done():
			return
		case <-initialized:
		}
		for {
			select {
			case <-ctx.Done():
				return
			case bcast := <-bcasts:
				pool.Apply(bcast.Message)
				node.Reply(n, bcastOK, broadcastOk)
			case data := <-pool.Gossip:
				broadcasts = append(broadcasts, data)
			case <-reads:
				bcasts := make([]int, len(broadcasts))
				copy(bcasts, broadcasts)
				node.Reply(n, replies, Bulk{
					MsgType:  "read_ok",
					Messages: bcasts,
				})
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
