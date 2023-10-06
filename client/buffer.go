package client

import (
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	"github.com/imdea-software/swiftpaxos/state"
)

type ReqReply struct {
	Val    state.Value
	Seqnum int
	Time   time.Time
}

type BufferClient struct {
	*Client

	Reply        chan *ReqReply
	GetClientKey func() int64

	seq         bool
	psize       int
	reqNum      int
	writes      int
	window      int32
	conflict    int
	syncFreq    int
	conflictKey int64

	reqTime    []time.Time
	launchTime time.Time

	rand *rand.Rand
}

func NewBufferClient(c *Client, reqNum, psize, conflict, writes int, conflictKey int64) *BufferClient {
	bc := &BufferClient{
		Client: c,

		Reply: make(chan *ReqReply, reqNum+1),

		seq:         true,
		psize:       psize,
		reqNum:      reqNum,
		writes:      writes,
		conflict:    conflict,
		conflictKey: conflictKey,

		reqTime: make([]time.Time, reqNum+1),
	}
	source := rand.NewSource(time.Now().UnixNano() + int64(c.ClientId))
	bc.rand = rand.New(source)
	return bc
}

func (c *BufferClient) Pipeline(syncFreq int, window int32) {
	c.seq = false
	c.syncFreq = syncFreq
	c.window = window
}

func (c *BufferClient) RegisterReply(val state.Value, seqnum int32) {
	t := time.Now()
	c.Reply <- &ReqReply{
		Val:    val,
		Seqnum: int(seqnum),
		Time:   t,
	}
}

func (c *BufferClient) Write(key int64, val []byte) {
	c.SendWrite(key, val)
	<-c.Reply
	return
}

func (c *BufferClient) Read(key int64) []byte {
	c.SendRead(key)
	r := <-c.Reply
	return r.Val
}

func (c *BufferClient) Scan(key, count int64) []byte {
	c.SendScan(key, count)
	r := <-c.Reply
	return r.Val
}

// Assumed to be connected
func (c *BufferClient) Loop() {
	getKey := c.genGetKey()
	val := make([]byte, c.psize)
	c.rand.Read(val)

	var cmdM sync.Mutex
	cmdNum := int32(0)
	wait := make(chan struct{}, 0)
	go func() {
		for i := 0; i <= c.reqNum; i++ {
			r := <-c.Reply
			// Ignore first request
			if i != 0 {
				d := r.Time.Sub(c.reqTime[r.Seqnum])
				m := float64(d.Nanoseconds()) / float64(time.Millisecond)
				c.Println("Returning:", r.Val.String())
				c.Printf("latency %v\n", m)
			}
			if c.window > 0 {
				cmdM.Lock()
				if cmdNum == c.window {
					cmdNum--
					cmdM.Unlock()
					wait <- struct{}{}
				} else {
					cmdNum--
					cmdM.Unlock()
				}
			}
			if c.seq || (c.syncFreq > 0 && i%c.syncFreq == 0) {
				wait <- struct{}{}
			}
		}
		if !c.seq {
			wait <- struct{}{}
		}
	}()

	for i := 0; i <= c.reqNum; i++ {
		key := getKey()
		write := c.randomTrue(c.writes)
		c.reqTime[i] = time.Now()

		// Ignore first request
		if i == 1 {
			c.launchTime = c.reqTime[i]
		}

		if write {
			c.SendWrite(key, state.Value(val))
			// TODO: if the return value != i, something's wrong
		} else {
			c.SendRead(key)
			// TODO: if the return value != i, something's wrong
		}
		if c.window > 0 {
			cmdM.Lock()
			if cmdNum == c.window-1 {
				cmdNum++
				cmdM.Unlock()
				<-wait
			} else {
				cmdNum++
				cmdM.Unlock()
			}
		}
		if c.seq || (c.syncFreq > 0 && i%c.syncFreq == 0) {
			<-wait
		}
	}

	if !c.seq {
		<-wait
	}

	c.Printf("Test took %v\n", time.Now().Sub(c.launchTime))
	c.Disconnect()
}

func (c *BufferClient) WaitReplies(waitFrom int) {
	go func() {
		for {
			r, err := c.GetReplyFrom(waitFrom)
			if err != nil {
				c.Println(err)
				break
			}
			if r.OK != defs.TRUE {
				c.Println("Faulty reply")
				break
			}
			go func(val state.Value, seqnum int32) {
				time.Sleep(c.dt.WaitDuration(c.replicas[waitFrom]))
				c.RegisterReply(val, seqnum)
			}(r.Value, r.CommandId)
		}
	}()
}

func (c *BufferClient) genGetKey() func() int64 {
	key := int64(uuid.New().Time())
	getKey := func() int64 {
		if c.randomTrue(c.conflict) {
			return c.conflictKey
		}
		if c.GetClientKey == nil {
			return key
		}
		return int64(c.GetClientKey())
	}
	return getKey
}

func (c *BufferClient) randomTrue(prob int) bool {
	if prob >= 100 {
		return true
	}
	if prob > 0 {
		return c.rand.Intn(100) <= prob
	}
	return false
}
