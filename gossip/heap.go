package gossip

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Id struct {
	Src   int    `json:"src"`
	MsgId uint64 `json:"msgid"`
}

type Update[T comparable] struct {
	Id   Id `json:"id"`
	Data T  `json:"data"`
}

type Gossip[T comparable] struct {
	Src         string      `json:"-"`
	MaelstromID int         `json:"msg_id,omitempty"`
	Recipient   string      `json:"-"`
	NextTry     time.Time   `json:"-"`
	MsgType     string      `json:"type"`
	ID          uint64      `json:"id"`
	Deltas      []Update[T] `json:"deltas"`
}

type GossipAck struct {
	InReplyTo int    `json:"in_reply_to,omitempty"`
	Sender    string `json:"-"`
	MsgType   string `json:"type"`
	ID        uint64 `json:"id"`
}

type Heap[T comparable] struct {
	// We use a numeric ID here so we can use the tuple (nodeID, gossipID) as a map key
	// that uniquely identifies an update originating at that node
	nodeID  int
	node    *maelstrom.Node
	nextID  uint64
	peers   []string
	pending map[uint64]Gossip[T]
	seen    map[Id]struct{}
	unsent  map[string][]Update[T]
	updates chan T
	gossip  chan Gossip[T]
	// NOTE: were this a real system I'd estimate this based on RTT between nodes
	retryDelay time.Duration
	// Called when receiving a new update via gossip
	applyUpdate func(T)
	acks        chan uint64
}

func NewHeap[T comparable](n *maelstrom.Node, apply func(T), latency time.Duration) *Heap[T] {
	group := n.NodeIDs()
	self := n.ID()
	sort.Strings(group)
	// easy 2n, 2n+1 heaps rely on starting at one
	heapIdx := sort.SearchStrings(group, self) + 1
	gh := &Heap[T]{
		retryDelay: latency * 4,
		nodeID:     heapIdx,
		node:       n,
		nextID:     1,
		peers: []string{
			group[(2*heapIdx-1)%len(group)],
			group[(2*heapIdx)%len(group)],
		},
		seen:        make(map[Id]struct{}),
		pending:     make(map[uint64]Gossip[T]),
		unsent:      make(map[string][]Update[T]),
		updates:     make(chan T),
		gossip:      make(chan Gossip[T]),
		applyUpdate: apply,
		acks:        make(chan uint64),
	}

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var goss Gossip[T]
		if err := json.Unmarshal(msg.Body, &goss); err != nil {
			return err
		}
		goss.Src = msg.Src
		gh.gossip <- goss
		return nil
	})

	n.Handle("gossip_ok", func(msg maelstrom.Message) error {
		var ack GossipAck
		if err := json.Unmarshal(msg.Body, &ack); err != nil {
			return err
		}
		ack.Sender = msg.Src
		gh.acks <- ack.ID

		return nil
	})

	return gh
}

func (g *Heap[T]) Apply(data T) {
	g.updates <- data
}

func (g *Heap[T]) Run(ctx context.Context) {
	tick := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			now := time.Now()
			nextTry := now.Add(g.retryDelay)
			for _, u := range g.pending {
				if now.After(u.NextTry) {
					u.NextTry = nextTry
					g.pending[u.ID] = u
					log.Printf("[goss] Retry %+v", u)
					if err := g.node.Send(u.Recipient, u); err != nil {
						log.Fatalf("Failed to send message %+v: %s", u, err)
					}
				}
			}
			for child, msgs := range g.unsent {
				if len(msgs) == 0 {
					continue
				}
				u := Gossip[T]{
					Recipient: child,
					NextTry:   nextTry,
					MsgType:   "gossip",
					ID:        g.nextID,
					Deltas:    msgs,
				}
				g.nextID += 1
				if err := g.node.Send(child, u); err != nil {
					log.Fatalf("Failed to update %s with message %+v: %s", child, u, err)
				}
				g.pending[u.ID] = u
			}
			g.unsent = make(map[string][]Update[T])
		case goss := <-g.gossip:
			for idx := range goss.Deltas {
				msg := goss.Deltas[idx]
				if _, ok := g.seen[msg.Id]; !ok {
					g.seen[msg.Id] = struct{}{}
					g.applyUpdate(msg.Data)
					for _, child := range g.peers {
						g.unsent[child] = append(g.unsent[child], msg)
					}
				}
			}
			err := g.node.Send(goss.Src, GossipAck{
				MsgType:   "gossip_ok",
				ID:        goss.ID,
				InReplyTo: goss.MaelstromID,
			})
			if err != nil {
				log.Fatalf("Failed to respond to gossip: %s", err)
			}
		case msg := <-g.updates:
			u := Update[T]{
				Id: Id{
					Src:   g.nodeID,
					MsgId: g.nextID,
				},
				Data: msg,
			}
			g.nextID += 1
			for _, child := range g.peers {
				g.unsent[child] = append(g.unsent[child], u)
			}
		case ack := <-g.acks:
			delete(g.pending, ack)
		}

	}
}
