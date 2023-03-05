package main

import (
	"context"
	"encoding/json"
	"log"

	"glomers/node"
)

type Send struct {
	Key string `json:"key"`
	Msg int    `json:"msg"`
}

type SendOk struct {
	MsgType string `json:"type"`
	Offset  int    `json:"offset"`
}

// poll, commit_offsets, list_committed_offsets_ok
type Offsets struct {
	MsgType string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollOK struct {
	MsgType string              `json:"type"`
	Msgs    map[string][][2]int `json:"msgs"`
}

type ListCommittedOffsets struct {
	Keys []string `json:"keys"`
}

var coffOk = json.RawMessage(`{"type":"commit_offsets_ok"}`)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := node.New(ctx, nil)

	go func() {
		sends, sendOK := node.Handle[Send, SendOk](n, "send")
		polls, pollOK := node.Handle[Offsets, PollOK](n, "poll")
		commits, commitOK := node.Handle[Offsets, json.RawMessage](n, "commit_offsets")
		listReqs, lists := node.Handle[ListCommittedOffsets, Offsets](n, "list_committed_offsets")

		// don't make this unlimited. It's a mistake
		const pollLimit = 5

		// State
		keys := make(map[string][]int)
		committed := make(map[string]int)
		for {
			select {
			case <-ctx.Done():
				return
			case s := <-sends:
				keys[s.Key] = append(keys[s.Key], s.Msg)
				node.Reply(n, sendOK, SendOk{
					MsgType: "send_ok",
					Offset:  len(keys[s.Key]),
				})

			case poll := <-polls:
				resp := PollOK{MsgType: "poll_ok", Msgs: make(map[string][][2]int)}
				for k, vs := range keys {
					start := poll.Offsets[k]
					end := start + pollLimit
					if end > len(vs) {
						end = len(vs)
					}
					for i, v := range vs[start:end] {
						resp.Msgs[k] = append(resp.Msgs[k], [2]int{poll.Offsets[k] + i, v})
					}
				}
				node.Reply(n, pollOK, resp)
			case o := <-commits:
				for k, v := range o.Offsets {
					committed[k] = v
				}
				node.Reply(n, commitOK, coffOk)
			case <-listReqs:
				resp := Offsets{MsgType: "list_committed_offsets_ok", Offsets: make(map[string]int)}
				for k, offs := range committed {
					resp.Offsets[k] = offs
				}
				node.Reply(n, lists, resp)

			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
