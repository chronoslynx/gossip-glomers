package main

import (
	"context"
	"log"

	"glomers/node"
)

type Txn struct {
	MsgType    string           `json:"type"`
	Operations [][3]interface{} `json:"txn"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := node.New(ctx, nil)

	go func() {
		txns, results := node.Handle[Txn, Txn](n, "txn")
		// State
		keys := make(map[float64]float64)
		for {
			select {
			case <-ctx.Done():
				return
			case txn := <-txns:
				txn.MsgType = "txn_ok"
				for i := range txn.Operations {
					op := txn.Operations[i]
					opType := op[0].(string)
					key := op[1].(float64)
					switch opType {
					case "r":
						op[2] = keys[key]
					case "w":
						keys[key] = op[2].(float64)
					default:
						log.Fatalf("Invalid op %+v", op)
					}
				}
				node.Reply(n, results, txn)
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
