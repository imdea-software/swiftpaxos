package client

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/imdea-software/swiftpaxos/state"
)

// Commands of the state machine (see state/state.go and state/row.go).

func Put(key int64, val []byte) state.Command {
	return state.Command{Op: state.PUT, K: state.Key(key), V: state.Value(val)}
}

func Get(key int64) state.Command {
	return state.Command{Op: state.GET, K: state.Key(key), V: state.NIL()}
}

// Scan reads the values of the keys in [key, key+count].
func Scan(key, count int64) state.Command {
	v := make([]byte, 8)
	binary.LittleEndian.PutUint64(v, uint64(count))
	return state.Command{Op: state.SCAN, K: state.Key(key), V: v}
}

// Update merges the given columns into the row stored at key.
func Update(key int64, cols state.Row) state.Command {
	return state.Command{Op: state.UPDATE, K: state.Key(key), V: cols.Encode()}
}

// Incr adds delta to a counter column of the row stored at key. It returns the
// new value of the counter (in decimal).
func Incr(key int64, column string, delta int64) state.Command {
	return state.Command{Op: state.INCR, K: state.Key(key), V: state.IncrArg(column, delta)}
}

func Delete(key int64) state.Command {
	return state.Command{Op: state.DELETE, K: state.Key(key), V: state.NIL()}
}

// Execute runs a one-shot transaction (a list of commands, each on a single
// key) and returns the result of each command, in order. If the protocol is
// transactional (Client.Transactional, as reported by the master), the
// transaction is executed atomically (it is sent as a single TXN command).
// Otherwise, as in the YCSB driver, its commands are executed one after the
// other, each one atomically.
//
// Replies must be collected with WaitReplies, and Execute must not be called
// concurrently with other operations of the client.
func (c *BufferClient) Execute(txn []state.Command) ([]state.Value, error) {
	if len(txn) == 0 {
		return nil, errors.New("client: empty transaction")
	}
	for i := range txn {
		if txn[i].Op == state.NONE || txn[i].Op == state.TXN {
			return nil, errors.New("client: invalid command in transaction")
		}
	}
	if c.Transactional {
		c.SendCommand(state.Command{Op: state.TXN, V: state.EncodeTxn(txn)})
		r := <-c.Reply
		vals, err := state.DecodeValues(r.Val)
		if err == nil && len(vals) != len(txn) {
			err = errors.New("client: malformed transaction reply")
		}
		return vals, err
	}
	vals := make([]state.Value, len(txn))
	for i := range txn {
		c.SendCommand(txn[i])
		vals[i] = (<-c.Reply).Val
	}
	return vals, nil
}

// LoopTxn runs a closed-loop workload of reqNum+1 random transactions (the
// first is not measured) with txnSize commands each: a command is a PUT with
// probability writes%, a GET otherwise, and it accesses the key conflictKey
// with probability conflict%.
func (c *BufferClient) LoopTxn(txnSize int) {
	if txnSize <= 0 {
		txnSize = 1
	}
	val := make([]byte, c.psize)
	c.rand.Read(val)
	for i := 0; i <= c.reqNum; i++ {
		txn := make([]state.Command, txnSize)
		for j := range txn {
			key := c.rand.Int63n(1 << 30)
			if c.randomTrue(c.conflict) {
				key = c.conflictKey
			}
			if c.randomTrue(c.writes) {
				txn[j] = Put(key, val)
			} else {
				txn[j] = Get(key)
			}
		}
		start := time.Now()
		if i == 1 {
			c.launchTime = start
		}
		if _, err := c.Execute(txn); err != nil {
			c.Println(err)
		}
		if i != 0 {
			c.Printf("latency %v\n", float64(time.Since(start).Nanoseconds())/float64(time.Millisecond))
		}
	}
	c.Printf("Test took %v\n", time.Since(c.launchTime))
	c.Disconnect()
}
