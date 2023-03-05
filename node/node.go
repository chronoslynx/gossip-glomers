// Wrap maelstrom nodes with some helpers
package node

import (
	"context"
	"encoding/json"
	"fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var topoOk = json.RawMessage(`{"type":"topology_ok"}`)

type Node struct {
	ctx  context.Context
	init func(n Node) error
	*maelstrom.Node
}

func New(ctx context.Context, init func(n Node) error) Node {
	n := Node{
		ctx,
		init,
		maelstrom.NewNode(),
	}

	if init != nil {
		// Init requires special handling as maelstrom automatically ACKs it for you
		n.Handle("init", func(m maelstrom.Message) error {
			if err := init(n); err != nil {
				return err
			}
			return nil
		})
	}
	// I don't care about topology messages either
	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, topoOk)
	})

	return n
}

func HandleNoAck[In any](n Node, kind string) <-chan In {
	// All channels are buffered by one so that replies _hopefully_ don't block receipt of new messages
	inCh := make(chan In, 1)

	done := n.ctx.Done()
	n.Handle(kind, func(msg maelstrom.Message) error {
		var in In
		if err := json.Unmarshal(msg.Body, &in); err != nil {
			return fmt.Errorf("failed to unmarshal %s message as %T: %w", kind, in, err)
		}
		select {
		case <-done:
		case inCh <- in:
		}
		return nil
	})

	return inCh
}

func Handle[In any, Out any](n Node, kind string) (<-chan In, chan<- Out) {
	// All channels are buffered by one so that replies _hopefully_ don't block receipt of new messages
	inCh := make(chan In, 1)
	outCh := make(chan Out, 1)

	done := n.ctx.Done()
	n.Handle(kind, func(msg maelstrom.Message) error {
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
				return n.Reply(msg, reply)
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
