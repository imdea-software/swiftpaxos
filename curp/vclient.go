package curp

import (
	"github.com/imdea-software/swiftpaxos/replica"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	"github.com/imdea-software/swiftpaxos/state"
)

type VirtualClient struct {
	replica *Replica

	acks  map[CommandId]*replica.MsgSet
	macks map[CommandId]*replica.MsgSet

	proposedByMe map[CommandId]*defs.GPropose

	N         int
	t         *Timer
	Q         replica.ThreeQuarters
	M         replica.Majority
	vals      map[CommandId]state.Value
	leader    int32
	ballot    int32
	delivered map[CommandId]struct{}

	slowPaths   int
	alreadySlow map[CommandId]struct{}
}

func NewVirtualClient(rp *Replica) *VirtualClient {
	repNum := rp.N

	c := &VirtualClient{
		replica: rp,

		proposedByMe: make(map[CommandId]*defs.GPropose),

		N:    repNum,
		Q:    replica.NewThreeQuartersOf(repNum),
		M:    replica.NewMajorityOf(repNum),
		vals: make(map[CommandId]state.Value),

		leader:    -1,
		ballot:    -1,
		delivered: make(map[CommandId]struct{}),

		acks:  make(map[CommandId]*replica.MsgSet),
		macks: make(map[CommandId]*replica.MsgSet),

		slowPaths:   0,
		alreadySlow: make(map[CommandId]struct{}),
	}

	return c
}

func (c *VirtualClient) initMsgSets(cmdId CommandId) {
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

func (c *VirtualClient) propose(p *defs.GPropose) {
	c.proposedByMe[CommandId{
		ClientId: p.ClientId,
		SeqNum:   p.CommandId,
	}] = p
	rp := &defs.Propose{
		CommandId: p.CommandId,
		ClientId:  p.ClientId,
		Command:   p.Command,
		Timestamp: int64(c.replica.Id), // a bit of a hack
	}
	c.replica.sender.SendToAll(rp, c.replica.cs.replicaProposeRPC)
	c.replica.cs.replicaProposeChan <- rp
}

func (c *VirtualClient) deliver(cmdId CommandId, val state.Value) {
	c.replica.cs.syncReplyChan <- &MSyncReply{
		Replica: c.replica.Id,
		Ballot:  c.replica.ballot,
		CmdId:   cmdId,
		Rep:     val,
	}
}

func (c *VirtualClient) handleReply(r *MReply) {
	if _, exists := c.delivered[r.CmdId]; exists {
		return
	}

	ack := &MRecordAck{
		Replica: r.Replica,
		Ballot:  r.Ballot,
		CmdId:   r.CmdId,
		Ok:      r.Ok,
	}
	c.vals[r.CmdId] = state.Value(r.Rep)
	c.handleRecordAck(ack, true)
}

func (c *VirtualClient) handleRecordAck(r *MRecordAck, fromLeader bool) {
	if _, exists := c.delivered[r.CmdId]; exists {
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

func (c *VirtualClient) handleSyncReply(rep *MSyncReply) {
	if _, exists := c.delivered[rep.CmdId]; exists {
		return
	}

	if c.ballot == -1 {
		c.ballot = rep.Ballot
	} else if c.ballot < rep.Ballot {
		c.ballot = rep.Ballot
	} else if c.ballot > rep.Ballot {
		return
	}

	c.vals[rep.CmdId] = state.Value(rep.Rep)
	c.delivered[rep.CmdId] = struct{}{}
	c.reply(rep.CmdId)
}

func (c *VirtualClient) handleAcks(leaderMsg interface{}, msgs []interface{}) {
	if leaderMsg == nil {
		return
	}

	if _, exists := c.delivered[leaderMsg.(*MRecordAck).CmdId]; exists {
		return
	}

	c.delivered[leaderMsg.(*MRecordAck).CmdId] = struct{}{}
	c.reply(leaderMsg.(*MRecordAck).CmdId)
}

func (c *VirtualClient) reply(cmdId CommandId) {
	if propose, exists := c.proposedByMe[cmdId]; exists {
		rep := &defs.ProposeReplyTS{
			OK:        defs.TRUE,
			CommandId: propose.CommandId,
			Value:     c.vals[cmdId],
			Timestamp: int64(c.replica.Id),
		}
		c.replica.ReplyProposeTS(rep, propose.Reply, propose.Mutex)
	}
}
