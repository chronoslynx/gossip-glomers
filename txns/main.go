package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"

	"glomers/node"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Txn struct {
	MsgType    string           `json:"type"`
	Operations [][3]interface{} `json:"txn"`
}

type IsolationLvl int

const (
	LvlReadUncommitted IsolationLvl = iota
)

type LockState int

const (
	LockReleased  LockState = 0
	LockShared    LockState = 1
	LockExclusive LockState = math.MaxInt
)

func (l LockState) String() string {
	switch l {
	case LockReleased:
		return "released"
	case LockShared:
		return "shared"
	case LockExclusive:
		return "exclusive"
	default:
		panic(l)
	}
}

const lockPfx = "lock"

type LockHold struct {
	Name  string
	State LockState
}

type KV struct {
	*maelstrom.KV
}

type TxnState string

const (
	StateRunning    TxnState = "running"
	StateCommitting TxnState = "committing"
)

type Transaction struct {
	ctx    context.Context
	kv     KV
	ID     uint64
	state  TxnState
	isoLvl IsolationLvl
	locks  map[string]*LockHold
}

func NewTransaction(ctx context.Context, kv KV, id uint64, isoLvl IsolationLvl) *Transaction {
	return &Transaction{
		ctx,
		kv,
		id,
		StateRunning,
		isoLvl,
		make(map[string]*LockHold),
	}
}

func (t *Transaction) release(lock *LockHold) error {
	var err error
	errCode := maelstrom.PreconditionFailed
	for errCode == maelstrom.PreconditionFailed {
		v, err := t.kv.ReadInt(t.ctx, lock.Name)
		if err != nil {
			return err
		}
		new := v - int(lock.State)
		if new < 0 {
			log.Fatalf("Invalid value for %s: invalid modification %d + %d -> %d", lock.Name, v, int(lock.State), new)
		}
		err = t.kv.CompareAndSwap(t.ctx, lock.Name, v, new, true)
		if err == nil {
			errCode = 0
			lock.State = LockReleased
		} else {
			errCode = maelstrom.ErrorCode(err)
		}
	}

	return err
}

// Code shared by both Commit and, if I implement it, Abort
func (t *Transaction) releaseLocks() error {
	var errs []string
	for k, lock := range t.locks {
		if lock.State == LockReleased {
			errs = append(errs, fmt.Sprintf("2PC violation: lock %s released during acquire phase", lock.Name))
		} else {
			log.Printf("txn %d releasing %s lock for %s", t.ID, lock.State, k)
			if err := t.release(lock); err != nil {
				errs = append(errs, err.Error())
			}
		}
	}
	if errs != nil {
		return errors.New(strings.Join(errs, ","))
	}
	return nil
}

func (t *Transaction) Commit() error {
	// Note: all this should happen on abort, but I'm not supporting that yet
	if t.state != StateRunning {
		return fmt.Errorf("Cannot commit txn %d in state %s", t.ID, t.state)
	}
	log.Printf("Committing txn %d", t.ID)
	t.state = StateCommitting

	return t.releaseLocks()
}

var KeyNotFound = errors.New("Key not found")

func (t *Transaction) Read(key string) (int, error) {
	val, err := t.kv.ReadInt(t.ctx, key)
	if err != nil && maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
		err = KeyNotFound
	}
	return val, err
}

// TODO how do we handle deadlocks? I can cheat in this challenge by scanning the full transaction for locks
// and acquiring them in sorted order. That'll ensure we can't get a history like:
//
// deadlock:
//
//	t1w[x]
//	t2w[y]
//	t1w[y]
//	t2w[x]
//
// As they'll always grab locks in the order (x, y). This wouldn't be great for a realy database as transactions can
// be long-lived and we don't know everything up front...

func (t *Transaction) acquireExclusive(lockName string) (hold *LockHold, err error) {
	errCode := maelstrom.PreconditionFailed
	for errCode == maelstrom.PreconditionFailed {
		err = t.kv.CompareAndSwap(t.ctx, lockName, 0, int(LockExclusive), true)
		if err == nil {
			hold = &LockHold{
				Name:  lockName,
				State: LockExclusive,
			}
			errCode = 0
		} else {
			errCode = maelstrom.ErrorCode(err)
		}
	}

	return
}

func (t *Transaction) acquireShared(lockName string) (hold *LockHold, err error) {
	errCode := maelstrom.PreconditionFailed
	for errCode == maelstrom.PreconditionFailed {
		v := int(LockExclusive)
		for v == int(LockExclusive) {
			v, err = t.kv.ReadInt(t.ctx, lockName)
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				v = 0
			} else if err != nil {
				return
			}
		}

		err = t.kv.CompareAndSwap(t.ctx, lockName, v, v+1, true)
		if err == nil {
			hold = &LockHold{
				Name:  lockName,
				State: LockShared,
			}
			errCode = 0
		} else {
			errCode = maelstrom.ErrorCode(err)
		}
	}

	return
}

func (t *Transaction) acquire(key string, mode LockState) (hold *LockHold, err error) {
	if t.state != StateRunning {
		err = fmt.Errorf("2PC violation: txn %d attempted to lock %s during release phase", t.ID, key)
		return
	}
	lKey := fmt.Sprintf("lock-%s", key)
	log.Printf("txn %d acquiring %s %s", t.ID, mode, lKey)
	switch mode {
	case LockReleased:
		err = fmt.Errorf("Fuck off with that. LockReleased is not a valid way to lock")
	case LockShared:
		hold, err = t.acquireShared(lKey)
	case LockExclusive:
		hold, err = t.acquireExclusive(lKey)
	default:
		log.Fatalf("Invalid lock state %+v", mode)
	}
	if err != nil {
		return
	}
	log.Printf("txn %d acquired %s %s", t.ID, mode, lKey)
	return
}

func (t *Transaction) Prelock(needs map[string]LockState) error {
	keys := make([]string, 0, len(needs))
	for k := range needs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i := range keys {
		key := keys[i]
		mode, ok := needs[key]
		if !ok {
			log.Fatalf("Trying to lock unknown key %s given %+v", key, needs)
		}
		lock, err := t.acquire(key, mode)
		if err != nil {
			return err
		}
		t.locks[key] = lock
	}
	return nil
}

func (t *Transaction) upgradeLock(key string) error {
	var err error
	errCode := maelstrom.PreconditionFailed
	for errCode == maelstrom.PreconditionFailed {
		err = t.kv.CompareAndSwap(t.ctx, key, 1, int(LockExclusive), true)
		if err == nil {
			errCode = 0
		} else {
			errCode = maelstrom.ErrorCode(err)
		}
	}
	if err == nil {
		lock := t.locks[key]
		lock.State = LockExclusive
		t.locks[key] = lock
	}
	return err
}

func (t *Transaction) Write(key string, value int) error {
	if lock, held := t.locks[key]; !held {
		var err error
		lock, err = t.acquire(key, LockExclusive)
		if err != nil {
			return err
		}
		t.locks[key] = lock
	} else if lock.State == LockShared {
		if err := t.upgradeLock(key); err != nil {
			return err
		}
	}
	return t.kv.Write(t.ctx, key, value)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var nextTxnId uint64
	var nNodes uint64
	n := node.New(ctx, func(n node.Node) error {
		// For unique id generation
		nodes := n.NodeIDs()
		sort.Strings(nodes)
		nextTxnId = uint64(sort.SearchStrings(nodes, n.ID()))
		nNodes = uint64(len(nodes))
		return nil
	})
	kv := KV{maelstrom.NewLinKV(n.Node)}

	go func() {
		txns, results := node.Handle[Txn, Txn](n, "txn")

		for {
			select {
			case <-ctx.Done():
				return
			case txn := <-txns:
				t := NewTransaction(ctx, kv, nextTxnId, LvlReadUncommitted)
				nextTxnId += nNodes
				txn.MsgType = "txn_ok"
				// Maelstrom assumes that all transactions must complete, even if they'd deadlock when run concurrently
				// Rather than serialize transaction execution let's just scan ahead and gather all the locks
				// ahead of time
				lockNeeds := make(map[string]LockState)
				for i := range txn.Operations {
					op := txn.Operations[i]
					opType := op[0].(string)
					key := strconv.FormatInt(int64(op[1].(float64)), 10)
					if _, found := lockNeeds[key]; !found && opType == "r" {
						lockNeeds[key] = LockShared
					} else if opType == "w" {
						lockNeeds[key] = LockExclusive
					}
				}
				if err := t.Prelock(lockNeeds); err != nil {
					log.Fatalf("txn %d was unable to gather the locks it requires: %s", t.ID, err)
				}
				for i := range txn.Operations {
					op := txn.Operations[i]
					opType := op[0].(string)
					key := strconv.FormatInt(int64(op[1].(float64)), 10)
					switch opType {
					case "r":
						val, err := t.Read(key)
						if err == KeyNotFound {
							// I'm just being lazy here
						} else if err != nil {
							log.Fatalf("txn %d failed to read %s: %s", t.ID, key, err)
						} else {
							op[2] = val
						}
					case "w":
						val := int(op[2].(float64))
						if err := t.Write(key, val); err != nil {
							log.Fatalf("txn %d failed to write %d to %s: %s", t.ID, val, key, err)
						}
					default:
						log.Fatalf("Invalid op %+v", op)
					}
				}
				if err := t.Commit(); err != nil {
					log.Fatalf("Failed to commit txn %d: %s", t.ID, err)
				}
				node.Reply(n, results, txn)
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
