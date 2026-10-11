package janus

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/imdea-software/swiftpaxos/client"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
	"github.com/imdea-software/swiftpaxos/state"
)

// Op is one operation of a one-shot transaction: an operation applied to a
// key, with its (optional) argument. For PUT, Arg is the value. For SCAN, Count
// is the length of the scanned range [Key, Key+Count]. A GET has no argument.
type Op struct {
	Op    state.Operation
	Key   int64
	Arg   []byte
	Count int64
}

// Txn is a one-shot transaction: a list of operations, each on a single key.
type Txn []Op

func Put(key int64, v []byte) Op { return Op{Op: state.PUT, Key: key, Arg: v} }
func Get(key int64) Op           { return Op{Op: state.GET, Key: key} }
func Scan(key, count int64) Op   { return Op{Op: state.SCAN, Key: key, Count: count} }

func (t Txn) commands() []state.Command {
	cmds := make([]state.Command, len(t))
	for i, o := range t {
		c := state.Command{Op: o.Op, K: state.Key(o.Key), V: state.NIL()}
		switch o.Op {
		case state.PUT:
			c.V = state.Value(o.Arg)
		case state.SCAN:
			c.V = make([]byte, 8)
			binary.LittleEndian.PutUint64(c.V, uint64(o.Count))
		}
		cmds[i] = c
	}
	return cmds
}

// Client submits transactions to the closest replica, which coordinates them.
type Client struct {
	*client.Client

	replies chan fastrpc.Serializable

	mu      sync.Mutex
	seq     int32
	waiting map[int32]chan [][]byte
}

// NewClient wraps a connected client.
func NewClient(c *client.Client) *Client {
	jc := &Client{
		Client:  c,
		replies: make(chan fastrpc.Serializable, 1024),
		waiting: make(map[int32]chan [][]byte),
	}
	t := fastrpc.NewTableId(defs.RPC_TABLE)
	t.Register(new(TxnPropose), make(chan fastrpc.Serializable)) // keeps codes aligned with replicas
	t.Register(new(TxnReply), jc.replies)
	c.RegisterRPCTable(t)
	go func() {
		for m := range jc.replies {
			rep := m.(*TxnReply)
			jc.mu.Lock()
			ch := jc.waiting[rep.CommandId]
			delete(jc.waiting, rep.CommandId)
			jc.mu.Unlock()
			if ch != nil {
				ch <- rep.Vals
			}
		}
	}()
	return jc
}

// Execute runs a transaction and returns the result of each operation, in order.
// It is safe for concurrent use.
func (c *Client) Execute(txn Txn) ([][]byte, error) {
	if len(txn) == 0 {
		return nil, errors.New("janus: empty transaction")
	}
	ch := make(chan [][]byte, 1)
	c.mu.Lock()
	id := c.seq
	c.seq++
	c.waiting[id] = ch
	c.mu.Unlock()
	c.SendMsg(int32(c.ClosestId), TxnProposeCode, &TxnPropose{
		CommandId: id,
		ClientId:  c.ClientId,
		Cmds:      txn.commands(),
	})
	return <-ch, nil
}

// Loop runs a closed-loop workload of reqs+1 random transactions (the first is
// not measured) with txnSize operations each. writes is the percentage of PUTs,
// conflicts the percentage of operations on the shared key conflictKey.
func (c *Client) Loop(reqs, txnSize, psize, writes, conflicts int, conflictKey int64) {
	if txnSize <= 0 {
		txnSize = 1
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(c.ClientId)))
	val := make([]byte, psize)
	rnd.Read(val)
	var launch time.Time
	for i := 0; i <= reqs; i++ {
		txn := make(Txn, txnSize)
		for j := range txn {
			key := rnd.Int63n(1 << 30)
			if rnd.Intn(100) < conflicts {
				key = conflictKey
			}
			if rnd.Intn(100) < writes {
				txn[j] = Put(key, val)
			} else {
				txn[j] = Get(key)
			}
		}
		start := time.Now()
		if i == 1 {
			launch = start
		}
		c.Execute(txn)
		if i != 0 {
			c.Printf("latency %v\n", float64(time.Since(start).Nanoseconds())/float64(time.Millisecond))
		}
	}
	c.Printf("Test took %v\n", time.Since(launch))
	c.Disconnect()
}
