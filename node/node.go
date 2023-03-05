// Wrap maelstrom nodes with some helpers
package node

import (
	"context"
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var initOk = json.RawMessage(`{"type":"init_ok"}`)
var topoOk = json.RawMessage(`{"type":"topology_ok"}`)

type Node struct {
	ctx  context.Context
	n    *maelstrom.Node
	init func(n *maelstrom.Node) error
}

func New(ctx context.Context, init func(n *maelstrom.Node) error) Node {
	n := Node{
		ctx:  ctx,
		n:    maelstrom.NewNode(),
		init: init,
	}

	if init != nil {
		n.n.Handle("init", func(m maelstrom.Message) error {
			if err := init(n.n); err != nil {
				return err
			}
			return n.n.Reply(m, initOk)
		})
	}
	// I don't care about topology messages either
	n.n.Handle("topology", func(msg maelstrom.Message) error {
		return n.n.Reply(msg, topoOk)
	})

	return n
}

func (n Node) Run() error {
	return n.n.Run()
}

func Handle[In any, Out any](n Node, kind string) (<-chan In, chan<- Out) {
	// All channels are buffered by one so that replies _hopefully_ don't block receipt of new messages
	inCh := make(chan In, 1)
	outCh := make(chan Out, 1)

	done := n.ctx.Done()
	n.n.Handle(kind, func(msg maelstrom.Message) error {
		var in In
		if err := json.Unmarshal(msg.Body, &in); err != nil {
			return fmt.Errorf("failed to unmarshal %s message as %T: %w", kind, in, err)
		}
		select {
		case <-done:
			return nil
		case inCh <- in:
			select {
			case <-done:
				return nil
			case reply := <-outCh:
				return n.n.Reply(msg, reply)
			}
		}
	})

	return inCh, outCh
}

func Reply[T any](n Node, tCh chan<- T, v T) {
	select {
	case <-n.ctx.Done():
		return
	case tCh <- v:
		return
	}
}
