package paxos

import (
	"encoding/binary"
	"io"
	"math"
	"time"

	"github.com/imdea-software/swiftpaxos/config"
	"github.com/imdea-software/swiftpaxos/dlog"
	"github.com/imdea-software/swiftpaxos/replica"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
	"github.com/imdea-software/swiftpaxos/state"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)

const COMMIT_GRACE_PERIOD = 3 * 1e9 // 3 second(s)
const SLEEP_TIME_NS = 1e6

type Replica struct {
	*replica.Replica
	prepareChan           chan fastrpc.Serializable
	acceptChan            chan fastrpc.Serializable
	commitChan            chan fastrpc.Serializable
	commitShortChan       chan fastrpc.Serializable
	prepareReplyChan      chan fastrpc.Serializable
	acceptReplyChan       chan fastrpc.Serializable
	instancesToRecover    chan int32
	prepareRPC            uint8
	acceptRPC             uint8
	commitRPC             uint8
	commitShortRPC        uint8
	prepareReplyRPC       uint8
	acceptReplyRPC        uint8
	IsLeader              bool
	instanceSpace         []*Instance
	crtInstance           int32
	maxRecvBallot         int32
	defaultBallot         []int32
	smallestDefaultBallot int32
	Shutdown              bool
	counter               int
	flush                 bool
	executedUpTo          int32
	batchWait             int

	totalRecNum  int
	totalSendNum int
}

type InstanceStatus int

const (
	PREPARING InstanceStatus = iota
	PREPARED
	ACCEPTED
	COMMITTED
)

type Instance struct {
	cmds   []state.Command
	bal    int32
	vbal   int32
	status InstanceStatus
	lb     *LeaderBookkeeping
}

type LeaderBookkeeping struct {
	clientProposals []*defs.GPropose
	prepareOKs      int
	acceptOKs       int
	nacks           int
	ballot          int32
	cmds            []state.Command
	lastTriedBallot int32
}

func New(alias string, id int, addrs []string, isLeader bool, f int, conf *config.Config, logger *dlog.Logger) *Replica {
	r := &Replica{
		Replica: replica.New(alias, id, f, addrs, false, true, false, conf, logger),

		prepareChan:      make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		acceptChan:       make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		commitChan:       make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		commitShortChan:  make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		prepareReplyChan: make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		acceptReplyChan:  make(chan fastrpc.Serializable, 3*defs.CHAN_BUFFER_SIZE),

		instancesToRecover: make(chan int32, 3*defs.CHAN_BUFFER_SIZE),
		instanceSpace:      make([]*Instance, 15*1024*1024),

		crtInstance:   -1,
		maxRecvBallot: -1,
		defaultBallot: make([]int32, len(addrs)),

		smallestDefaultBallot: -1,

		IsLeader: isLeader,

		flush:        true,
		executedUpTo: -1,
	}

	if r.IsLeader {
		r.BeTheLeader(nil, nil)
	}

	for i := 0; i < len(r.defaultBallot); i++ {
		r.defaultBallot[i] = -1
	}

	r.prepareRPC = r.RPC.Register(new(Prepare), r.prepareChan)
	r.acceptRPC = r.RPC.Register(new(Accept), r.acceptChan)
	r.commitRPC = r.RPC.Register(new(Commit), r.commitChan)
	r.commitShortRPC = r.RPC.Register(new(CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RPC.Register(new(PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RPC.Register(new(AcceptReply), r.acceptReplyChan)

	go r.run()

	return r
}

func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	var b [5]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.bal))
	b[4] = byte(inst.status)
	r.StableStore.Write(b[:])
}

func (r *Replica) recordCommands(cmds []state.Command) {
	if !r.Durable {
		return
	}

	if cmds == nil {
		return
	}
	for i := 0; i < len(cmds); i++ {
		cmds[i].Marshal(io.Writer(r.StableStore))
	}
}

func (r *Replica) sync() {
	if !r.Durable {
		return
	}

	r.StableStore.Sync()
}

func (r *Replica) BeTheLeader(args *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
	r.IsLeader = true
	r.totalRecNum = 0
	r.totalSendNum = 0
	r.Println("I am the leader")
	return nil
}

func (r *Replica) replyPrepare(replicaId int32, reply *PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *AcceptReply) {
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

var fastClockChan chan bool

func (r *Replica) fastClock() {
	for !r.Shutdown {
		time.Sleep(time.Duration(r.batchWait) * time.Millisecond)
		fastClockChan <- true
	}
}

func (r *Replica) BatchingEnabled() bool {
	return r.batchWait > 0
}

func (r *Replica) run() {

	r.ConnectToPeers()

	r.ComputeClosestPeers()

	if r.Exec {
		go r.executeCommands()
	}

	fastClockChan = make(chan bool, 1)

	if r.BatchingEnabled() {
		go r.fastClock()
	}

	onOffProposeChan := r.ProposeChan

	go r.WaitForClientConnections()

	for !r.Shutdown {

		select {
		case propose := <-onOffProposeChan:
			r.handlePropose(propose)
			if r.BatchingEnabled() {
				onOffProposeChan = nil
			}
			break

		case <-fastClockChan:
			onOffProposeChan = r.ProposeChan
			break

		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*Prepare)
			r.handlePrepare(prepare)
			break

		case acceptS := <-r.acceptChan:
			accept := acceptS.(*Accept)
			r.handleAccept(accept)
			break

		case commitS := <-r.commitChan:
			commit := commitS.(*Commit)
			r.handleCommit(commit)
			break

		case commitS := <-r.commitShortChan:
			commit := commitS.(*CommitShort)
			r.handleCommitShort(commit)
			break

		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*PrepareReply)
			r.handlePrepareReply(prepareReply)
			break

		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*AcceptReply)
			r.handleAcceptReply(acceptReply)
			break

		case iid := <-r.instancesToRecover:
			r.recover(iid)
			break
		}

	}
}

func (r *Replica) makeBallot(instance int32) {
	lb := r.instanceSpace[instance].lb
	n := int32(r.Id)
	if r.IsLeader {
		for n < r.defaultBallot[r.Id] || n < r.maxRecvBallot {
			n += int32(r.N)
		}
	}
	lb.lastTriedBallot = n
}

func (r *Replica) bcastPrepare(instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("Prepare bcast failed:", err)
		}
	}()

	args := &Prepare{r.Id, instance, r.instanceSpace[instance].lb.lastTriedBallot}

	n := r.N - 1

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.prepareRPC, args)
		sent++
		if sent >= n {
			break
		}
	}

}

var pa Accept

func (r *Replica) bcastAccept(instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("Accept bcast failed:", err)
		}
	}()
	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.Ballot = r.instanceSpace[instance].lb.lastTriedBallot
	pa.Command = r.instanceSpace[instance].lb.cmds
	args := &pa

	n := r.N - 1

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.acceptRPC, args)
		sent++
		if sent >= n {
			break
		}
	}

}

var pc Commit
var pcs CommitShort

func (r *Replica) bcastCommit(instance int32, ballot int32, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("Commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = ballot
	pc.Command = command

	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = ballot
	pcs.Count = int32(len(command))
	argsShort := &pcs

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.commitShortRPC, argsShort)
		sent++
	}

}

func (r *Replica) handlePropose(propose *defs.GPropose) {
	if !r.IsLeader {
		preply := &defs.ProposeReplyTS{FALSE, -1, state.NIL(), 0}
		r.ReplyProposeTS(preply, propose.Reply, propose.Mutex)
		return
	}

	batchSize := len(r.ProposeChan) + 1

	proposals := make([]*defs.GPropose, batchSize)
	cmds := make([]state.Command, batchSize)
	proposals[0] = propose
	cmds[0] = propose.Command
	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		proposals[i] = prop
		cmds[i] = prop.Command
	}

	r.crtInstance++
	r.instanceSpace[r.crtInstance] = &Instance{
		nil,
		r.defaultBallot[r.Id],
		r.defaultBallot[r.Id],
		PREPARING,
		&LeaderBookkeeping{proposals, 0, 0, 0, r.Id, nil, -1}}
	r.makeBallot(r.crtInstance)

	inst := r.instanceSpace[r.crtInstance]
	lb := inst.lb
	r.defaultBallot[r.Id] = lb.lastTriedBallot

	if lb.lastTriedBallot != r.smallestDefaultBallot {
		r.bcastPrepare(r.crtInstance)
	} else {
		inst.cmds = cmds
		inst.lb.cmds = cmds
		inst.bal = lb.lastTriedBallot
		inst.vbal = lb.lastTriedBallot
		inst.status = ACCEPTED
		r.bcastAccept(r.crtInstance)
	}
}

func (r *Replica) handlePrepare(prepare *Prepare) {
	if prepare.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = prepare.Ballot
	}

	inst := r.instanceSpace[prepare.Instance]
	if inst == nil {
		if prepare.Instance > r.crtInstance {
			r.crtInstance = prepare.Instance
		}
		r.instanceSpace[prepare.Instance] = &Instance{
			nil,
			r.defaultBallot[r.Id],
			r.defaultBallot[r.Id],
			PREPARING,
			nil}
		inst = r.instanceSpace[prepare.Instance]
	}

	if inst.status == COMMITTED {
		var pc Commit
		pc.LeaderId = prepare.LeaderId
		pc.Instance = prepare.Instance
		pc.Ballot = inst.vbal
		pc.Command = inst.cmds
		args := &pc
		r.SendMsg(pc.LeaderId, r.commitRPC, args)
		return
	}

	if inst.bal > prepare.Ballot {
	} else if inst.bal < prepare.Ballot {
		inst.bal = prepare.Ballot
		inst.status = PREPARED
		if r.crtInstance == prepare.Instance {
			r.defaultBallot[r.Id] = prepare.Ballot
		}
	}

	preply := &PrepareReply{prepare.Instance, inst.bal, inst.vbal, r.defaultBallot[r.Id], r.Id, inst.cmds}
	r.replyPrepare(prepare.LeaderId, preply)
}

func (r *Replica) handleAccept(accept *Accept) {
	inst := r.instanceSpace[accept.Instance]

	if accept.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = accept.Ballot
	}

	if inst == nil {
		if accept.Instance > r.crtInstance {
			r.crtInstance = accept.Instance
		}
		r.instanceSpace[accept.Instance] = &Instance{
			accept.Command,
			accept.Ballot,
			accept.Ballot,
			ACCEPTED,
			nil}
		inst = r.instanceSpace[accept.Instance]
		r.recordInstanceMetadata(r.instanceSpace[accept.Instance])
		r.recordCommands(accept.Command)
		r.sync()
	} else if accept.Ballot < inst.bal {
		dlog.Printf("Smaller ballot %d < %d\n", accept.Ballot, inst.bal)
	} else if inst.status == COMMITTED {
		dlog.Printf("Already committed \n")
	} else {
		inst.cmds = accept.Command
		inst.bal = accept.Ballot
		inst.vbal = accept.Ballot
		inst.status = ACCEPTED
		r.recordInstanceMetadata(r.instanceSpace[accept.Instance])
		r.recordCommands(accept.Command)
		r.sync()
	}

	areply := &AcceptReply{accept.Instance, inst.bal}
	r.replyAccept(accept.LeaderId, areply)
}

func (r *Replica) handleCommit(commit *Commit) {
	inst := r.instanceSpace[commit.Instance]
	if inst == nil {
		if commit.Instance > r.crtInstance {
			r.crtInstance = commit.Instance
		}
		r.instanceSpace[commit.Instance] = &Instance{
			nil,
			r.defaultBallot[r.Id],
			r.defaultBallot[r.Id],
			PREPARING,
			nil}
		inst = r.instanceSpace[commit.Instance]
	}

	if inst != nil && inst.status == COMMITTED {
		return
	}

	if commit.Ballot < inst.bal {
		return
	}

	// FIXME timeout on client side?
	if inst.lb != nil && inst.lb.clientProposals != nil {
		for _, p := range inst.lb.clientProposals {
			r.ProposeChan <- p
		}
		inst.lb.clientProposals = nil
	}

	inst.cmds = commit.Command
	inst.bal = commit.Ballot
	inst.vbal = commit.Ballot
	inst.status = COMMITTED
	r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
	r.recordCommands(commit.Command)
}

func (r *Replica) handleCommitShort(commit *CommitShort) {
	inst := r.instanceSpace[commit.Instance]
	if inst == nil {
		return
	}

	if inst.status == COMMITTED {
		return
	}

	if commit.Ballot < inst.bal {
		return
	}

	r.instanceSpace[commit.Instance].status = COMMITTED
	r.instanceSpace[commit.Instance].bal = commit.Ballot
	r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
	r.recordCommands(r.instanceSpace[commit.Instance].cmds)
}

func (r *Replica) handlePrepareReply(preply *PrepareReply) {
	inst := r.instanceSpace[preply.Instance]
	lb := r.instanceSpace[preply.Instance].lb

	if preply.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = preply.Ballot
	}

	if preply.Ballot < lb.lastTriedBallot {
		return
	}

	if preply.Ballot > lb.lastTriedBallot {
		lb.nacks++
		if lb.nacks+1 > r.N>>1 {
			if r.IsLeader {
				r.makeBallot(preply.Instance)
				r.bcastPrepare(preply.Instance)
			}
		}
		return
	}

	if preply.VBallot > lb.ballot {
		lb.ballot = preply.VBallot
		lb.cmds = preply.Command
	}

	lb.prepareOKs++
	if r.defaultBallot[preply.AcceptorId] < preply.DefaultBallot {
		r.defaultBallot[preply.AcceptorId] = preply.DefaultBallot
	}

	if lb.prepareOKs < r.Replica.ReadQuorumSize() && len(preply.Command) != 0 {
		r.totalRecNum += len(preply.Command)
		r.Println("totalRecNum:", r.totalRecNum)
	}

	// TODO: In the following we update lb.cmds,
	// ignoring `lb.cmds = preply.Command` executed
	// previously. This is strange

	if lb.prepareOKs+1 >= r.Replica.ReadQuorumSize() {
		if lb.clientProposals != nil {
			cmds := make([]state.Command, len(lb.clientProposals))
			for i := 0; i < len(lb.clientProposals); i++ {
				cmds[i] = lb.clientProposals[i].Command
			}
			lb.cmds = cmds
		} else {
			lb.cmds = state.NOOP()
		}
		inst.cmds = lb.cmds
		inst.bal = lb.lastTriedBallot
		inst.status = ACCEPTED

		m := int32(math.MaxInt32)
		count := 0
		for _, e := range r.defaultBallot {
			if e != -1 {
				count++
				if e < m {
					m = e
				}
			}
		}
		if count >= r.Replica.ReadQuorumSize()-1 && m > r.smallestDefaultBallot {
			r.smallestDefaultBallot = m
		}

		r.recordInstanceMetadata(r.instanceSpace[preply.Instance])
		r.sync()
		r.bcastAccept(preply.Instance)
		if len(inst.cmds) != 0 {
			r.totalSendNum += len(inst.cmds)
			r.Println("totalSendNum:", r.totalSendNum)
		}
	}
}

func (r *Replica) handleAcceptReply(areply *AcceptReply) {
	inst := r.instanceSpace[areply.Instance]
	lb := r.instanceSpace[areply.Instance].lb

	if areply.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = areply.Ballot
	}

	if inst.status >= COMMITTED {
		return
	}

	if areply.Ballot < lb.lastTriedBallot {
		return
	}

	if areply.Ballot > lb.lastTriedBallot {
		lb.nacks++
		if lb.nacks+1 >= r.Replica.WriteQuorumSize() {
			if r.IsLeader {
				r.makeBallot(areply.Ballot)
				r.bcastPrepare(areply.Instance)
			}
		}
		return
	}

	lb.acceptOKs++
	if lb.acceptOKs+1 >= r.Replica.WriteQuorumSize() {
		inst = r.instanceSpace[areply.Instance]
		inst.status = COMMITTED
		r.recordInstanceMetadata(r.instanceSpace[areply.Instance])
		r.sync()

		r.bcastCommit(areply.Instance, inst.bal, inst.cmds)
		if lb.clientProposals != nil && !r.Dreply {
			for i := 0; i < len(inst.cmds); i++ {
				propreply := &defs.ProposeReplyTS{
					TRUE,
					lb.clientProposals[i].CommandId,
					state.NIL(),
					lb.clientProposals[i].Timestamp}
				r.ReplyProposeTS(propreply, lb.clientProposals[i].Reply, lb.clientProposals[i].Mutex)
			}
		}
	}
}

func (r *Replica) recover(instance int32) {
	if r.instanceSpace[instance] == nil {
		r.instanceSpace[instance] = &Instance{
			nil,
			r.defaultBallot[r.Id],
			r.defaultBallot[r.Id],
			PREPARING,
			nil}

	}

	if r.instanceSpace[instance].lb == nil {
		r.instanceSpace[instance].lb = &LeaderBookkeeping{nil, 0, 0, 0, -1, nil, -1}
	}

	r.makeBallot(instance)
	r.bcastPrepare(instance)
}

func (r *Replica) executeCommands() {
	timeout := int64(0)
	problemInstance := int32(0)

	for !r.Shutdown {
		executed := false

		// FIXME idempotence
		for i := r.executedUpTo + 1; i <= r.crtInstance; i++ {
			inst := r.instanceSpace[i]
			if inst != nil && inst.cmds != nil && inst.status == COMMITTED {
				for j := 0; j < len(inst.cmds); j++ {
					if r.Dreply && inst.lb != nil && inst.lb.clientProposals != nil {
						val := inst.cmds[j].Execute(r.State)
						propreply := &defs.ProposeReplyTS{
							TRUE,
							inst.lb.clientProposals[j].CommandId,
							val,
							inst.lb.clientProposals[j].Timestamp}
						r.ReplyProposeTS(propreply, inst.lb.clientProposals[j].Reply, inst.lb.clientProposals[j].Mutex)
					} else if inst.cmds[j].Op == state.PUT {
						inst.cmds[j].Execute(r.State)
					}
				}
				executed = true
				r.executedUpTo++
			} else {
				if i == problemInstance {
					timeout += SLEEP_TIME_NS
					if timeout >= COMMIT_GRACE_PERIOD {
						for k := problemInstance; k <= r.crtInstance; k++ {
							r.instancesToRecover <- k
						}
						problemInstance = 0
						timeout = 0
					}
				} else {
					problemInstance = i
					timeout = 0
				}
				break
			}
		}

		if !executed {
			r.M.Lock()
			r.M.Unlock() // FIXME for cache coherence
			time.Sleep(SLEEP_TIME_NS)
		}
	}
}
