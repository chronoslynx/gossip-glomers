package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Add struct {
	MsgType string `json:"type"`
	Delta   int    `json:"delta"`
}

type Topology struct {
	Neighbors map[string][]string `json:"topology"`
}

type Read struct {
	MsgType string `json:"type"`
	Value   int    `json:"value"`
}

var initOk = json.RawMessage(`{"type":"init_ok"}`)
var addOk = json.RawMessage(`{"type":"add_ok"}`)
var topoOk = json.RawMessage(`{"type":"topology_ok"}`)

const key = "counter"

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	n.Handle("add", func(msg maelstrom.Message) error {
		var add Add
		err := json.Unmarshal(msg.Body, &add)
		if err != nil {
			return err
		}
		errCode := maelstrom.PreconditionFailed
		for errCode == maelstrom.PreconditionFailed {
			var v int
			v, err = kv.ReadInt(ctx, key)
			if err != nil {
				return err
			}
			err = kv.CompareAndSwap(ctx, key, v, v+add.Delta, false)
			if err == nil {
				errCode = 0
			} else if rer, ok := err.(*maelstrom.RPCError); ok {
				errCode = rer.Code
			}
		}
		if errCode != 0 {
			return err
		}
		return n.Reply(msg, addOk)
	})

	n.Handle("init", func(msg maelstrom.Message) error {
		if err := kv.Write(ctx, key, 0); err != nil {
			return err
		}
		return n.Reply(msg, initOk)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		v, err := kv.ReadInt(ctx, key)
		if err != nil {
			return err
		}
		return n.Reply(msg, Read{
			MsgType: "read_ok",
			Value:   v,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, topoOk)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
