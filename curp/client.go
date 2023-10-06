package curp

import (
	"sync"

	"github.com/imdea-software/swiftpaxos/client"
	"github.com/imdea-software/swiftpaxos/replica"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
	"github.com/imdea-software/swiftpaxos/state"
)

type Client struct {
	*client.BufferClient

	acks  map[CommandId]*replica.MsgSet
	macks map[CommandId]*replica.MsgSet

	N         int
	t         *Timer
	Q         replica.ThreeQuarters
	M         replica.Majority
	cs        CommunicationSupply
	num       int
	val       state.Value
	leader    int32
	ballot    int32
	delivered map[int32]struct{}

	lastCmdId CommandId

	slowPaths   int
	alreadySlow map[CommandId]struct{}
}

var (
	m         sync.Mutex
	clientNum int
)

// pclients - Number of clients already running on other machines
// This is needed to generate a new key for each new request
func NewClient(b *client.BufferClient, repNum, reqNum, pclients int) *Client {
	m.Lock()
	num := clientNum
	clientNum++
	m.Unlock()

	c := &Client{
		BufferClient: b,

		N:   repNum,
		t:   NewTimer(),
		Q:   replica.NewThreeQuartersOf(repNum),
		M:   replica.NewMajorityOf(repNum),
		num: num,
		val: nil,

		leader:    -1,
		ballot:    -1,
		delivered: make(map[int32]struct{}),

		acks:  make(map[CommandId]*replica.MsgSet),
		macks: make(map[CommandId]*replica.MsgSet),

		slowPaths:   0,
		alreadySlow: make(map[CommandId]struct{}),
	}

	c.lastCmdId = CommandId{
		ClientId: c.ClientId,
		SeqNum:   0,
	}

	t := fastrpc.NewTableId(defs.RPC_TABLE)
	initCs(&c.cs, t)
	c.RegisterRPCTable(t)

	// Generate a new key for each new request
	if pclients != -1 {
		i := 0
		c.GetClientKey = func() int64 {
			k := 100 + i + (reqNum * (c.num + pclients))
			i++
			return int64(k)
		}
	}

	go c.handleMsgs()

	return c
}

func (c *Client) initMsgSets(cmdId CommandId) {
	m, exists := c.acks[cmdId]
	initAcks := !exists || m == nil
	m, exists = c.macks[cmdId]
	initMacks := !exists || m == nil

	accept := func(_, _ interface{}) bool {
		return true
	}

	if initAcks {
		c.acks[cmdId] = c.acks[cmdId].ReinitMsgSet(c.Q, accept, func(interface{}) {}, c.handleAcks)
	}
	if initMacks {
		c.macks[cmdId] = c.macks[cmdId].ReinitMsgSet(c.M, accept, func(interface{}) {}, c.handleAcks)
	}
}

func (c *Client) handleMsgs() {
	for {
		select {
		case m := <-c.cs.replyChan:
			rep := m.(*MReply)
			c.handleReply(rep)

		case m := <-c.cs.recordAckChan:
			recAck := m.(*MRecordAck)
			c.handleRecordAck(recAck, false)

		case m := <-c.cs.syncReplyChan:
			rep := m.(*MSyncReply)
			c.handleSyncReply(rep)

		case needSync := <-c.t.c:
			// FIXME
			break
			if needSync && c.leader != -1 {
				if _, exists := c.delivered[c.lastCmdId.SeqNum]; !exists {
					sync := &MSync{
						CmdId: c.lastCmdId,
					}
					// TODO: send to closest?
					c.SendMsg(c.leader, c.cs.syncRPC, sync)
				}
			}
		}
	}
}

func (c *Client) handleReply(r *MReply) {
	if _, exists := c.delivered[r.CmdId.SeqNum]; exists {
		return
	}

	ack := &MRecordAck{
		Replica: r.Replica,
		Ballot:  r.Ballot,
		CmdId:   r.CmdId,
		Ok:      r.Ok,
	}
	c.val = state.Value(r.Rep)
	c.handleRecordAck(ack, true)
}

func (c *Client) handleRecordAck(r *MRecordAck, fromLeader bool) {
	if _, exists := c.delivered[r.CmdId.SeqNum]; exists {
		return
	}

	if c.ballot == -1 {
		c.ballot = r.Ballot
	} else if c.ballot < r.Ballot {
		c.ballot = r.Ballot
	} else if c.ballot > r.Ballot {
		return
	}

	if fromLeader {
		c.leader = r.Replica
	}

	if fromLeader || r.Ok == ORDERED {
		c.initMsgSets(r.CmdId)
		c.macks[r.CmdId].Add(r.Replica, fromLeader, r)
	}

	if r.Ok == TRUE {
		c.initMsgSets(r.CmdId)
		c.acks[r.CmdId].Add(r.Replica, fromLeader, r)
	}
}

func (c *Client) handleSyncReply(rep *MSyncReply) {
	if _, exists := c.delivered[rep.CmdId.SeqNum]; exists {
		return
	}

	if c.ballot == -1 {
		c.ballot = rep.Ballot
	} else if c.ballot < rep.Ballot {
		c.ballot = rep.Ballot
	} else if c.ballot > rep.Ballot {
		return
	}

	c.val = state.Value(rep.Rep)
	c.delivered[rep.CmdId.SeqNum] = struct{}{}
	c.RegisterReply(c.val, rep.CmdId.SeqNum)
	c.Println("Slow Paths:", c.slowPaths)
}

func (c *Client) handleAcks(leaderMsg interface{}, msgs []interface{}) {
	if leaderMsg == nil {
		return
	}

	if _, exists := c.delivered[leaderMsg.(*MRecordAck).CmdId.SeqNum]; exists {
		return
	}

	c.delivered[leaderMsg.(*MRecordAck).CmdId.SeqNum] = struct{}{}
	c.RegisterReply(c.val, leaderMsg.(*MRecordAck).CmdId.SeqNum)
	c.Println("Slow Paths:", c.slowPaths)
}
