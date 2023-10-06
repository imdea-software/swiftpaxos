package fastpaxos

import (
	"sort"

	"github.com/imdea-software/swiftpaxos/config"
	"github.com/imdea-software/swiftpaxos/dlog"
	"github.com/imdea-software/swiftpaxos/replica"
	"github.com/imdea-software/swiftpaxos/replica/defs"
)

type Replica struct {
	*replica.Replica

	next         int
	cmds         map[CommandId]*defs.GPropose
	delivered    map[CommandId]struct{}
	instances    []*instance
	lastExecuted int

	sender replica.Sender
	cs     CommunicationSupply

	defaultFastQuorum replica.QuorumI
}

type instance struct {
	value     CommandId
	ballot    int32
	cballot   int32
	committed bool

	m2bSets map[int32]*replica.MsgSet
}

const INST_NUM = 1e8

func New(alias string, rid int, addrs []string, exec bool, f int, conf *config.Config, l *dlog.Logger) *Replica {
	r := &Replica{
		Replica:      replica.New(alias, rid, f, addrs, false, exec, false, conf, l),
		next:         0,
		cmds:         make(map[CommandId]*defs.GPropose),
		delivered:    make(map[CommandId]struct{}),
		instances:    make([]*instance, INST_NUM),
		lastExecuted: -1,
	}
	qs, _, err := replica.NewQuorumsFromFile(conf.Quorum, r.Replica)
	if err != nil {
		r.defaultFastQuorum = replica.NewThreeQuartersOf(r.N)
	} else {
		r.defaultFastQuorum = qs[0]
	}
	initCs(&r.cs, r.RPC)
	r.sender = replica.NewSender(r.Replica)
	go r.run()
	return r
}

func (r *Replica) run() {
	r.ConnectToPeers()
	go r.WaitForClientConnections()

	for !r.Shutdown {
		select {
		case proposal := <-r.ProposeChan:
			r.handleProposal(proposal)
		case m := <-r.cs.m2BChan:
			r.handleM2B(m.(*M2B))
		}
	}
}

func (r *Replica) handleProposal(proposal *defs.GPropose) {
	cmdId := CommandId{
		ClientId: proposal.ClientId,
		SeqNum:   proposal.CommandId,
	}
	r.cmds[cmdId] = proposal
	i := r.getInstance(r.next)
	r.next++
	i.value = cmdId

	m2b := &M2B{
		CmdId:      i.value,
		Ballot:     i.ballot,
		Cballot:    i.cballot,
		Replica:    r.Id,
		InstanceId: r.next - 1,
	}
	r.sender.SendToAll(m2b, r.cs.m2BRPC)
	r.handleM2B(m2b)
}

func (r *Replica) handleM2B(m2b *M2B) {
	r.getM2bSet(r.getInstance(m2b.InstanceId), m2b.Ballot).Add(m2b.Replica, false, m2b)
}

func (r *Replica) handleM2Bs(_ interface{}, msgs []interface{}) {
	var (
		value  *CommandId
		commit = true
	)
	m2bs := make([]*M2B, len(msgs))
	for i, m := range msgs {
		m2bs[i] = m.(*M2B)
		if value == nil {
			value = &m2bs[i].CmdId
		}
		commit = commit && *value == m2bs[i].CmdId
	}
	if commit {
		r.commit(m2bs[0].InstanceId, *value)
		// TODO: clean m2bsets for this instance
	} else {
		// uncoordinated recovery
		// use quorum of 2Bs as a quorum of 1Bs
		// TODO: for now this is correct only for one fixed fast quorum,
		//       for multiple quorums we must first guarantee that there is no
		//       agreement in any fast quorum. This is due to the fact that the
		//       quorum that will be (deterministically) chosen during recovery
		//       can differ from the one that actually accepts a command.
		sort.Slice(m2bs, func(i, j int) bool {
			if m2bs[i].Cballot > m2bs[j].Cballot {
				return false
			}
			return m2bs[i].Cballot < m2bs[j].Cballot || m2bs[i].Replica < m2bs[j].Replica
		})
		maxV := m2bs[len(m2bs)-1]
		if r.instances[maxV.InstanceId].ballot != maxV.Ballot {
			// A priori I don't know from which quorum I should recover.
			// Must retry when ballot = maxV.Ballot or completely ignore
			// (if ballot > maxV.Ballot).
			// As of now, this branch should be unreachable because of
			// a fixed fast quorum that is also a collision recovery quorum,
			// and because there is no recovery from failures.
			return
		}
		r.instances[maxV.InstanceId].value = maxV.CmdId
		r.advanceBallot(r.instances[maxV.InstanceId], maxV.Ballot+1)
		m2b := &M2B{
			CmdId:      r.instances[maxV.InstanceId].value,
			Ballot:     r.instances[maxV.InstanceId].ballot,
			Cballot:    r.instances[maxV.InstanceId].cballot,
			Replica:    r.Id,
			InstanceId: maxV.InstanceId,
		}
		r.sender.SendToAll(m2b, r.cs.m2BRPC)
		r.handleM2B(m2b)
	}
}

func (r *Replica) getInstance(instanceId int) *instance {
	if instanceId < len(r.instances) {
		if r.instances[instanceId] == nil {
			i := &instance{
				ballot:    0,
				cballot:   0,
				committed: false,
				m2bSets:   make(map[int32]*replica.MsgSet),
			}
			r.instances[instanceId] = i
		}
		return r.instances[instanceId]
	}
	is := make([]*instance, instanceId+1)
	copy(is, r.instances)
	return r.getInstance(instanceId)
}

func (r *Replica) getM2bSet(i *instance, ballot int32) *replica.MsgSet {
	s, exists := i.m2bSets[ballot]
	if !exists || s == nil {
		s = replica.NewMsgSet(r.defaultFastQuorum, func(_, _ interface{}) bool {
			return true
		}, func(interface{}) {}, r.handleM2Bs)
		i.m2bSets[ballot] = s
	}
	return s
}

func (r *Replica) advanceBallot(i *instance, ballot int32) {
	if ballot > i.ballot {
		i.ballot = ballot
		i.cballot = ballot
	}
}

func (r *Replica) commit(id int, v CommandId) {
	// FIXME: this can be called before the proposal is handled

	r.getInstance(id).value = v
	r.getInstance(id).committed = true

	i := r.getInstance(r.lastExecuted + 1)
	for i.committed {
		v = i.value
		if _, exists := r.delivered[v]; !exists {
			// Should check whether v is already delivered or not,
			// this is because v can be committed at more than one instances
			// (tho this is impossible without failure recovery and with a
			// single fast quorum)
			r.delivered[v] = struct{}{}
			res := &defs.ProposeReplyTS{
				OK:        defs.TRUE,
				Value:     r.cmds[v].Command.Execute(r.State),
				CommandId: r.cmds[v].CommandId,
				Timestamp: r.cmds[v].Timestamp,
			}
			if r.cmds[v].Proxy {
				r.ReplyProposeTS(res, r.cmds[v].Reply, r.cmds[v].Mutex)
			}
		}
		r.lastExecuted++
		i = r.getInstance(r.lastExecuted + 1)
	}
}
