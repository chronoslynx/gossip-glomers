package main

import (
	"context"
	"log"
	"sort"

	"glomers/node"
)

type GenerateOK struct {
	MsgType string `json:"type"`
	ID      uint64 `json:"id"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Strided ID generation. Because nodes neither join nor leave each can safely generate ids at
	// a stride with width len(nodes).
	//
	// The first ID is the current node's offset in a sorted list of IDs.
	// This does not work if nodes can join or leave the cluster, or if sequential IDs are required.
	var nextID uint64
	var nodes []string
	n := node.New(ctx, func(n node.Node) error {
		nodes = n.NodeIDs()
		sort.Strings(nodes)
		nextID = uint64(sort.SearchStrings(nodes, n.ID()))
		return nil
	})
	reqs, resps := node.Handle[map[string]any, GenerateOK](n, "generate")
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-reqs:
				resp := GenerateOK{
					MsgType: "generate_ok",
					ID:      nextID,
				}
				nextID += uint64(len(nodes))
				node.Reply(n, resps, resp)
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
