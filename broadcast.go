package main

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// NOTE: were this a real system I'd estimate this based on RTT between nodes
const internodeLatency = 100 * time.Millisecond
const retryDelay = internodeLatency * 4

type Broadcast struct {
	MsgType string `json:"type"`
	Message int    `json:"message"`
}

type Bulk struct {
	MsgType  string `json:"type"`
	Messages []int  `json:"messages"`
}

type Gossip struct {
	MessageID uint64 `json:"mid"`
	Messages  []int  `json:"messages"`
}

type Topology struct {
	Neighbors map[string][]string `json:"topology"`
}

type UpdateAck struct {
	Sender  string `json:"-"`
	MsgType string `json:"type"`
	ID      uint64 `json:"id"`
}

type Update struct {
	Recipient string    `json:"-"`
	NextTry   time.Time `json:"-"`
	MsgType   string    `json:"type"`
	ID        uint64    `json:"id"`
	Messages  []int     `json:"messages"`
}

var broadcastOk = json.RawMessage(`{"type":"broadcast_ok"}`)
var topoOk = json.RawMessage(`{"type":"topology_ok"}`)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := maelstrom.NewNode()
	seen := make(map[int]struct{})
	block := new(sync.RWMutex)
	var broadcasts []int

	// node -> message id -> time last attempted
	var pending map[uint64]Update
	var unsent map[string][]int
	var gossipID uint64 = 1

	var children []string

	updates := make(chan int, 5)
	acks := make(chan UpdateAck, 5)

	go func() {
		tick := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				now := time.Now()
				nextTry := now.Add(retryDelay)
				for _, u := range pending {
					if now.After(u.NextTry) {
						u.NextTry = nextTry
						pending[u.ID] = u
						log.Printf("[goss] Retry %+v", u)
						if err := n.Send(u.Recipient, u); err != nil {
							log.Fatalf("Failed to send message %+v: %s", u, err)
						}
					}
				}
				for child, msgs := range unsent {
					if len(msgs) == 0 {
						continue
					}
					u := Update{
						Recipient: child,
						NextTry:   nextTry,
						MsgType:   "gossip",
						ID:        gossipID,
						Messages:  msgs,
					}
					gossipID += 1
					if err := n.Send(child, u); err != nil {
						log.Fatalf("Failed to update %s with message %+v: %s", child, u, err)
					}
					pending[u.ID] = u
				}
			case msg := <-updates:
				for _, child := range children {
					unsent[child] = append(unsent[child], msg)
				}
			case ack := <-acks:
				delete(pending, ack.ID)
			}

		}
	}()

	n.Handle("init", func(_ maelstrom.Message) error {
		nodes := n.NodeIDs()
		sort.Strings(nodes)
		// easy 2n, 2n+1 heaps rely on starting at one
		heapIdx := sort.SearchStrings(nodes, n.ID()) + 1
		children = []string{
			nodes[(2*heapIdx-1)%len(nodes)],
			nodes[(2*heapIdx)%len(nodes)],
		}
		pending = make(map[uint64]Update)
		unsent = map[string][]int{
			children[0]: nil,
			children[1]: nil,
		}
		return nil
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var update Update
		if err := json.Unmarshal(msg.Body, &update); err != nil {
			return err
		}
		block.Lock()
		for idx := range update.Messages {
			msg := update.Messages[idx]
			if _, ok := seen[msg]; !ok {
				seen[msg] = struct{}{}
				broadcasts = append(broadcasts, msg)

				updates <- msg
			}
		}
		block.Unlock()
		return n.Reply(msg, UpdateAck{
			MsgType: "gossip_ok",
			ID:      update.ID,
		})
	})

	n.Handle("gossip_ok", func(msg maelstrom.Message) error {
		var ack UpdateAck
		if err := json.Unmarshal(msg.Body, &ack); err != nil {
			return err
		}
		ack.Sender = msg.Src
		acks <- ack

		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var bcast Broadcast
		if err := json.Unmarshal(msg.Body, &bcast); err != nil {
			return err
		}
		block.Lock()
		if _, ok := seen[bcast.Message]; ok {
			// Already seen, no need to propagate
			block.Unlock()
		} else {
			seen[bcast.Message] = struct{}{}
			broadcasts = append(broadcasts, bcast.Message)
			block.Unlock()

			updates <- bcast.Message
		}

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
