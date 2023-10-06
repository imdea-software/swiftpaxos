package epaxos

import (
	"encoding/binary"
	"io"
	"sync"
	"time"

	"github.com/imdea-software/swiftpaxos/config"
	"github.com/imdea-software/swiftpaxos/dlog"
	"github.com/imdea-software/swiftpaxos/replica"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
	"github.com/imdea-software/swiftpaxos/state"
)

const MAX_INSTANCE = 10 * 1024 * 1024

const MAX_DEPTH_DEP = 10
const TRUE = uint8(1)
const FALSE = uint8(0)
const ADAPT_TIME_SEC = 10

const COMMIT_GRACE_PERIOD = 10 * 1e9 // 10 second(s)

const BF_K = 4
const BF_M_N = 32.0

const HT_INIT_SIZE = 200000

// main differences with the original code base
// - fix N=3 case
// - add vbal variable (TLA spec. is wrong)
// - remove checkpoints (need to fix them first)
// - remove short commits (with N>7 propagating committed dependencies is necessary)
// - must run with thriftiness on (recovery is incorrect otherwise)
// - when conflicts are transitive skip waiting prior commuting commands

var cpMarker []state.Command
var cpcounter = 0

type Replica struct {
	*replica.Replica
	prepareChan           chan fastrpc.Serializable
	preAcceptChan         chan fastrpc.Serializable
	acceptChan            chan fastrpc.Serializable
	commitChan            chan fastrpc.Serializable
	prepareReplyChan      chan fastrpc.Serializable
	preAcceptReplyChan    chan fastrpc.Serializable
	preAcceptOKChan       chan fastrpc.Serializable
	acceptReplyChan       chan fastrpc.Serializable
	tryPreAcceptChan      chan fastrpc.Serializable
	tryPreAcceptReplyChan chan fastrpc.Serializable
	prepareRPC            uint8
	prepareReplyRPC       uint8
	preAcceptRPC          uint8
	preAcceptReplyRPC     uint8
	acceptRPC             uint8
	acceptReplyRPC        uint8
	commitRPC             uint8
	tryPreAcceptRPC       uint8
	tryPreAcceptReplyRPC  uint8
	// the space of all instances (used and not yet used)
	InstanceSpace [][]*Instance
	// highest active instance numbers that this replica knows about
	crtInstance []int32
	// highest committed instance per replica that this replica knows about
	CommittedUpTo []int32
	// instance up to which all commands have been executed (including iteslf)
	ExecedUpTo       []int32
	exec             *Exec
	conflicts        []map[state.Key]*InstPair
	maxSeqPerKey     map[state.Key]int32
	maxSeq           int32
	latestCPReplica  int32
	latestCPInstance int32
	// for synchronizing when sending replies to clients
	// from multiple go-routines
	clientMutex        *sync.Mutex
	instancesToRecover chan *instanceId
	// does this replica think it is the leader
	IsLeader      bool
	maxRecvBallot int32
	batchWait     int
	transconf     bool
}

type InstPair struct {
	last      int32
	lastWrite int32
}

type Instance struct {
	Cmds           []state.Command
	bal, vbal      int32
	Status         int8
	Seq            int32
	Deps           []int32
	lb             *LeaderBookkeeping
	Index, Lowlink int
	bfilter        any
	proposeTime    int64
	id             *instanceId
}

type instanceId struct {
	replica  int32
	instance int32
}

type LeaderBookkeeping struct {
	clientProposals   []*defs.GPropose
	ballot            int32
	allEqual          bool
	preAcceptOKs      int
	acceptOKs         int
	nacks             int
	originalDeps      []int32
	committedDeps     []int32
	prepareReplies    []*PrepareReply
	preparing         bool
	tryingToPreAccept bool
	possibleQuorum    []bool
	tpaReps           int
	tpaAccepted       bool
	lastTriedBallot   int32
	cmds              []state.Command
	status            int8
	seq               int32
	deps              []int32
	leaderResponded   bool
}

func New(alias string, id int, peerAddrList []string, exec, beacon, durable bool, batchWait int, transconf bool, failures int, conf *config.Config, logger *dlog.Logger) *Replica {
	r := &Replica{
		replica.New(alias, id, failures, peerAddrList, true, exec, false, conf, logger),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE*2),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE),
		0, 0, 0, 0, 0, 0, 0, 0, 0,
		make([][]*Instance, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		nil,
		make([]map[state.Key]*InstPair, len(peerAddrList)),
		make(map[state.Key]int32),
		0,
		0,
		-1,
		new(sync.Mutex),
		make(chan *instanceId, defs.CHAN_BUFFER_SIZE),
		false,
		-1,
		batchWait,
		transconf,
	}

	r.Beacon = beacon
	r.Durable = durable

	for i := 0; i < r.N; i++ {
		r.InstanceSpace[i] = make([]*Instance, MAX_INSTANCE) // FIXME
		r.crtInstance[i] = -1
		r.ExecedUpTo[i] = -1
		r.CommittedUpTo[i] = -1
		r.conflicts[i] = make(map[state.Key]*InstPair, HT_INIT_SIZE)
	}

	r.exec = &Exec{r}

	cpMarker = make([]state.Command, 0)

	//register RPCs
	r.prepareRPC = r.RPC.Register(new(Prepare), r.prepareChan)
	r.prepareReplyRPC = r.RPC.Register(new(PrepareReply), r.prepareReplyChan)
	r.preAcceptRPC = r.RPC.Register(new(PreAccept), r.preAcceptChan)
	r.preAcceptReplyRPC = r.RPC.Register(new(PreAcceptReply), r.preAcceptReplyChan)
	r.acceptRPC = r.RPC.Register(new(Accept), r.acceptChan)
	r.acceptReplyRPC = r.RPC.Register(new(AcceptReply), r.acceptReplyChan)
	r.commitRPC = r.RPC.Register(new(Commit), r.commitChan)
	r.tryPreAcceptRPC = r.RPC.Register(new(TryPreAccept), r.tryPreAcceptChan)
	r.tryPreAcceptReplyRPC = r.RPC.Register(new(TryPreAcceptReply), r.tryPreAcceptReplyChan)

	r.Stats.M["weird"], r.Stats.M["conflicted"], r.Stats.M["slow"], r.Stats.M["fast"], r.Stats.M["totalCommitTime"], r.Stats.M["totalBatching"], r.Stats.M["totalBatchingSize"] = 0, 0, 0, 0, 0, 0, 0

	go r.run()

	return r
}

func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	b := make([]byte, 9+r.N*4)
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.bal))
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.vbal))
	b[4] = byte(inst.Status)
	binary.LittleEndian.PutUint32(b[5:9], uint32(inst.Seq))
	l := 9
	for _, dep := range inst.Deps {
		binary.LittleEndian.PutUint32(b[l:l+4], uint32(dep))
		l += 4
	}
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

var fastClockChan chan bool
var slowClockChan chan bool

func (r *Replica) fastClock() {
	for !r.Shutdown {
		time.Sleep(time.Duration(r.batchWait) * time.Millisecond)
		fastClockChan <- true
	}
}
func (r *Replica) slowClock() {
	for !r.Shutdown {
		time.Sleep(150 * time.Millisecond)
		slowClockChan <- true
	}
}

func (r *Replica) stopAdapting() {
	time.Sleep(1000 * 1000 * 1000 * ADAPT_TIME_SEC)
	r.Beacon = false
	time.Sleep(1000 * 1000 * 1000)

	for i := 0; i < r.N-1; i++ {
		min := i
		for j := i + 1; j < r.N-1; j++ {
			if r.Ewma[r.PreferredPeerOrder[j]] < r.Ewma[r.PreferredPeerOrder[min]] {
				min = j
			}
		}
		aux := r.PreferredPeerOrder[i]
		r.PreferredPeerOrder[i] = r.PreferredPeerOrder[min]
		r.PreferredPeerOrder[min] = aux
	}

	r.Println(r.PreferredPeerOrder)
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

	slowClockChan = make(chan bool, 1)
	fastClockChan = make(chan bool, 1)
	go r.slowClock()

	if r.BatchingEnabled() {
		go r.fastClock()
	}

	if r.Beacon {
		go r.stopAdapting()
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

		case preAcceptS := <-r.preAcceptChan:
			preAccept := preAcceptS.(*PreAccept)
			r.handlePreAccept(preAccept)
			break

		case acceptS := <-r.acceptChan:
			accept := acceptS.(*Accept)
			r.handleAccept(accept)
			break

		case commitS := <-r.commitChan:
			commit := commitS.(*Commit)
			r.handleCommit(commit)
			break

		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*PrepareReply)
			r.handlePrepareReply(prepareReply)
			break

		case preAcceptReplyS := <-r.preAcceptReplyChan:
			preAcceptReply := preAcceptReplyS.(*PreAcceptReply)
			r.handlePreAcceptReply(preAcceptReply)
			break

		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*AcceptReply)
			r.handleAcceptReply(acceptReply)
			break

		case tryPreAcceptS := <-r.tryPreAcceptChan:
			tryPreAccept := tryPreAcceptS.(*TryPreAccept)
			r.handleTryPreAccept(tryPreAccept)
			break

		case tryPreAcceptReplyS := <-r.tryPreAcceptReplyChan:
			tryPreAcceptReply := tryPreAcceptReplyS.(*TryPreAcceptReply)
			r.handleTryPreAcceptReply(tryPreAcceptReply)
			break

		case beacon := <-r.BeaconChan:
			r.ReplyBeacon(beacon)
			break

		case <-slowClockChan:
			if r.Beacon {
				r.Printf("weird %d; conflicted %d; slow %d; fast %d\n", r.Stats.M["weird"], r.Stats.M["conflicted"], r.Stats.M["slow"], r.Stats.M["fast"])
				for q := int32(0); q < int32(r.N); q++ {
					if q == r.Id {
						continue
					}
					r.SendBeacon(q)
				}
			}
			break

		case iid := <-r.instancesToRecover:
			r.startRecoveryForInstance(iid.replica, iid.instance)
		}
	}
}

func (r *Replica) executeCommands() {
	const SLEEP_TIME_NS = 1e6
	problemInstance := make([]int32, r.N)
	timeout := make([]uint64, r.N)
	for q := 0; q < r.N; q++ {
		problemInstance[q] = -1
		timeout[q] = 0
	}

	for !r.Shutdown {
		executed := false
		for q := int32(0); q < int32(r.N); q++ {
			for inst := r.ExecedUpTo[q] + 1; inst <= r.crtInstance[q]; inst++ {
				if r.InstanceSpace[q][inst] != nil && r.InstanceSpace[q][inst].Status == EXECUTED {
					if inst == r.ExecedUpTo[q]+1 {
						r.ExecedUpTo[q] = inst
					}
					continue
				}
				if r.InstanceSpace[q][inst] == nil || r.InstanceSpace[q][inst].Status < COMMITTED || r.InstanceSpace[q][inst].Cmds == nil {
					if inst == problemInstance[q] {
						timeout[q] += SLEEP_TIME_NS
						if timeout[q] >= COMMIT_GRACE_PERIOD {
							for k := problemInstance[q]; k <= r.crtInstance[q]; k++ {
								r.instancesToRecover <- &instanceId{q, k}
							}
							timeout[q] = 0
						}
					} else {
						problemInstance[q] = inst
						timeout[q] = 0
					}
					break
				}
				if ok := r.exec.executeCommand(int32(q), inst); ok {
					executed = true
					if inst == r.ExecedUpTo[q]+1 {
						r.ExecedUpTo[q] = inst
					}
				}
			}
		}
		if !executed {
			r.M.Lock()
			r.M.Unlock() // FIXME for cache coherence
			time.Sleep(SLEEP_TIME_NS)
		}
	}
}

func isInitialBallot(ballot int32, replica int32, instance int32) bool {
	return ballot == replica
}

func (r *Replica) makeBallot(replica int32, instance int32) {
	lb := r.InstanceSpace[replica][instance].lb
	n := r.Id
	if r.Id != replica {
		n += int32(r.N)
	}
	if r.IsLeader {
		for n < r.maxRecvBallot {
			n += int32(r.N)
		}
	}
	lb.lastTriedBallot = n
}

func (r *Replica) replyPrepare(replicaId int32, reply *PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyPreAccept(replicaId int32, reply *PreAcceptReply) {
	r.SendMsg(replicaId, r.preAcceptReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *AcceptReply) {
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

func (r *Replica) replyTryPreAccept(replicaId int32, reply *TryPreAcceptReply) {
	r.SendMsg(replicaId, r.tryPreAcceptReplyRPC, reply)
}

func (r *Replica) bcastPrepare(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("Prepare bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	args := &Prepare{r.Id, replica, instance, lb.lastTriedBallot}

	n := r.N - 1
	q := r.Id
	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			continue
		}
		r.SendMsg(q, r.prepareRPC, args)
		sent++
	}

}

func (r *Replica) bcastPreAccept(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("PreAccept bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	pa := new(PreAccept)
	pa.LeaderId = r.Id
	pa.Replica = replica
	pa.Instance = instance
	pa.Ballot = lb.lastTriedBallot
	pa.Command = lb.cmds
	pa.Seq = lb.seq
	pa.Deps = lb.deps

	n := r.N - 1
	if r.Thrifty {
		n = r.Replica.FastQuorumSize() - 1
	}

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.preAcceptRPC, pa)
		sent++
		if sent >= n {
			break
		}
	}
}

func (r *Replica) bcastTryPreAccept(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("PreAccept bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	tpa := new(TryPreAccept)
	tpa.LeaderId = r.Id
	tpa.Replica = replica
	tpa.Instance = instance
	tpa.Ballot = lb.lastTriedBallot
	tpa.Command = lb.cmds
	tpa.Seq = lb.seq
	tpa.Deps = lb.deps

	for q := int32(0); q < int32(r.N); q++ {
		if q == r.Id {
			continue
		}
		if !r.Alive[q] {
			continue
		}
		r.SendMsg(q, r.tryPreAcceptRPC, tpa)
	}
}

func (r *Replica) bcastAccept(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("Accept bcast failed:", err)
		}
	}()

	lb := r.InstanceSpace[replica][instance].lb
	ea := new(Accept)
	ea.LeaderId = r.Id
	ea.Replica = replica
	ea.Instance = instance
	ea.Ballot = lb.lastTriedBallot
	ea.Seq = lb.seq
	ea.Deps = lb.deps

	n := r.N - 1
	if r.Thrifty {
		n = r.N / 2
	}

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.acceptRPC, ea)
		sent++
		if sent >= n {
			break
		}
	}
}

func (r *Replica) bcastCommit(replica int32, instance int32) {
	defer func() {
		if err := recover(); err != nil {
			r.Println("Commit bcast failed:", err)
		}
	}()
	lb := r.InstanceSpace[replica][instance].lb
	ec := new(Commit)
	ec.LeaderId = r.Id
	ec.Replica = replica
	ec.Instance = instance
	ec.Command = lb.cmds
	ec.Seq = lb.seq
	ec.Deps = lb.deps
	ec.Ballot = lb.ballot

	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.commitRPC, ec)
	}
}

func (r *Replica) clearHashtables() {
	for q := 0; q < r.N; q++ {
		r.conflicts[q] = make(map[state.Key]*InstPair, HT_INIT_SIZE)
	}
}

func (r *Replica) updateCommitted(replica int32) {
	r.M.Lock()
	for r.InstanceSpace[replica][r.CommittedUpTo[replica]+1] != nil &&
		(r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == COMMITTED ||
			r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == EXECUTED) {
		r.CommittedUpTo[replica] = r.CommittedUpTo[replica] + 1
	}
	r.M.Unlock()
}

func (r *Replica) updateConflicts(cmds []state.Command, replica int32, instance int32, seq int32) {
	for i := 0; i < len(cmds); i++ {
		if dpair, present := r.conflicts[replica][cmds[i].K]; present {
			if dpair.last < instance {
				r.conflicts[replica][cmds[i].K].last = instance
			}
			if dpair.lastWrite < instance && cmds[i].Op != state.GET {
				r.conflicts[replica][cmds[i].K].lastWrite = instance
			}
		} else {
			r.conflicts[replica][cmds[i].K] = &InstPair{
				last:      instance,
				lastWrite: -1,
			}
			if cmds[i].Op != state.GET {
				r.conflicts[replica][cmds[i].K].lastWrite = instance
			}
		}
		if s, present := r.maxSeqPerKey[cmds[i].K]; present {
			if s < seq {
				r.maxSeqPerKey[cmds[i].K] = seq
			}
		} else {
			r.maxSeqPerKey[cmds[i].K] = seq
		}
	}
}

func (r *Replica) updateAttributes(cmds []state.Command, seq int32, deps []int32, replica int32, instance int32) (int32, []int32, bool) {
	changed := false
	for q := 0; q < r.N; q++ {
		if r.Id != replica && int32(q) == replica {
			continue
		}
		for i := 0; i < len(cmds); i++ {
			if dpair, present := (r.conflicts[q])[cmds[i].K]; present {
				d := dpair.lastWrite
				if cmds[i].Op != state.GET {
					d = dpair.last
				}

				if d > deps[q] {
					deps[q] = d
					if seq <= r.InstanceSpace[q][d].Seq {
						seq = r.InstanceSpace[q][d].Seq + 1
					}
					changed = true
					break
				}
			}
		}
	}
	for i := 0; i < len(cmds); i++ {
		if s, present := r.maxSeqPerKey[cmds[i].K]; present {
			if seq <= s {
				changed = true
				seq = s + 1
			}
		}
	}

	return seq, deps, changed
}

func (r *Replica) mergeAttributes(seq1 int32, deps1 []int32, seq2 int32, deps2 []int32) (int32, []int32, bool) {
	equal := true
	if seq1 != seq2 {
		equal = false
		if seq2 > seq1 {
			seq1 = seq2
		}
	}
	for q := 0; q < r.N; q++ {
		if int32(q) == r.Id {
			continue
		}
		if deps1[q] != deps2[q] {
			equal = false
			if deps2[q] > deps1[q] {
				deps1[q] = deps2[q]
			}
		}
	}
	return seq1, deps1, equal
}

func equal(deps1 []int32, deps2 []int32) bool {
	for i := 0; i < len(deps1); i++ {
		if deps1[i] != deps2[i] {
			return false
		}
	}
	return true
}

func (r *Replica) handlePropose(propose *defs.GPropose) {
	//TODO!! Handle client retries

	batchSize := len(r.ProposeChan) + 1
	r.M.Lock()
	r.Stats.M["totalBatching"]++
	r.Stats.M["totalBatchingSize"] += batchSize
	r.M.Unlock()

	r.crtInstance[r.Id]++

	cmds := make([]state.Command, batchSize)
	proposals := make([]*defs.GPropose, batchSize)
	cmds[0] = propose.Command
	proposals[0] = propose
	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		cmds[i] = prop.Command
		proposals[i] = prop
	}

	r.startPhase1(cmds, r.Id, r.crtInstance[r.Id], r.Id, proposals)

	cpcounter += len(cmds)

}

func (r *Replica) startPhase1(cmds []state.Command, replica int32, instance int32, ballot int32, proposals []*defs.GPropose) {
	// init command attributes
	seq := int32(0)
	deps := make([]int32, r.N)
	for q := 0; q < r.N; q++ {
		deps[q] = -1
	}
	seq, deps, _ = r.updateAttributes(cmds, seq, deps, replica, instance)
	comDeps := make([]int32, r.N)
	for i := 0; i < r.N; i++ {
		comDeps[i] = -1
	}

	inst := r.newInstance(replica, instance, cmds, ballot, ballot, PREACCEPTED, seq, deps)
	inst.lb = r.newLeaderBookkeeping(proposals, deps, comDeps, deps, ballot, cmds, PREACCEPTED, -1)
	r.InstanceSpace[replica][instance] = inst

	r.updateConflicts(cmds, replica, instance, seq)

	if seq >= r.maxSeq {
		r.maxSeq = seq
	}

	r.recordInstanceMetadata(r.InstanceSpace[r.Id][instance])
	r.recordCommands(cmds)
	r.sync()

	r.bcastPreAccept(replica, instance)
}

func (r *Replica) handlePreAccept(preAccept *PreAccept) {
	inst := r.InstanceSpace[preAccept.Replica][preAccept.Instance]

	if preAccept.Seq >= r.maxSeq {
		r.maxSeq = preAccept.Seq + 1
	}

	if preAccept.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = preAccept.Ballot
	}

	if preAccept.Instance > r.crtInstance[preAccept.Replica] {
		r.crtInstance[preAccept.Replica] = preAccept.Instance
	}

	if inst == nil {
		inst = r.newInstanceDefault(preAccept.Replica, preAccept.Instance)
		r.InstanceSpace[preAccept.Replica][preAccept.Instance] = inst
	}

	if inst != nil && preAccept.Ballot < inst.bal {
		return
	}

	inst.bal = preAccept.Ballot

	if inst.Status >= ACCEPTED {
		if inst.Cmds == nil {
			r.InstanceSpace[preAccept.LeaderId][preAccept.Instance].Cmds = preAccept.Command
			r.updateConflicts(preAccept.Command, preAccept.Replica, preAccept.Instance, preAccept.Seq)
			r.recordCommands(preAccept.Command)
			r.sync()
		}

	} else {
		seq, deps, changed := r.updateAttributes(preAccept.Command, preAccept.Seq, preAccept.Deps, preAccept.Replica, preAccept.Instance)
		status := PREACCEPTED_EQ
		if changed {
			status = PREACCEPTED
		}
		inst.Cmds = preAccept.Command
		inst.Seq = seq
		inst.Deps = deps
		inst.bal = preAccept.Ballot
		inst.vbal = preAccept.Ballot
		inst.Status = status

		r.updateConflicts(preAccept.Command, preAccept.Replica, preAccept.Instance, preAccept.Seq)
		r.recordInstanceMetadata(r.InstanceSpace[preAccept.Replica][preAccept.Instance])
		r.recordCommands(preAccept.Command)
		r.sync()

	}

	reply := &PreAcceptReply{
		preAccept.Replica,
		preAccept.Instance,
		inst.bal,
		inst.vbal,
		inst.Seq,
		inst.Deps,
		r.CommittedUpTo,
		inst.Status}
	r.replyPreAccept(preAccept.LeaderId, reply)
}

func (r *Replica) handlePreAcceptReply(pareply *PreAcceptReply) {
	inst := r.InstanceSpace[pareply.Replica][pareply.Instance]
	lb := inst.lb

	if pareply.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = pareply.Ballot
	}

	if lb.lastTriedBallot > pareply.Ballot {
		return
	}

	if lb.lastTriedBallot < pareply.Ballot {
		lb.nacks++
		if lb.nacks+1 > r.N>>1 {
			if r.IsLeader {
				r.makeBallot(pareply.Replica, pareply.Instance)
				r.bcastPrepare(pareply.Replica, pareply.Instance)
			}
		}
		return
	}

	if lb.status != PREACCEPTED && lb.status != PREACCEPTED_EQ {
		return
	}

	inst.lb.preAcceptOKs++

	if pareply.VBallot > lb.ballot {
		lb.ballot = pareply.VBallot
		lb.seq = pareply.Seq
		lb.deps = pareply.Deps
		lb.status = pareply.Status
	}

	isInitialBallot := isInitialBallot(lb.lastTriedBallot, pareply.Replica, pareply.Instance)

	seq, deps, allEqual := r.mergeAttributes(lb.seq, lb.deps, pareply.Seq, pareply.Deps)
	if r.N <= 3 && r.Thrifty {
		// no need to check for equality
	} else {
		inst.lb.allEqual = inst.lb.allEqual && allEqual
		if !allEqual {
			r.M.Lock()
			r.Stats.M["conflicted"]++
			r.M.Unlock()
		}
	}

	allCommitted := true
	if r.N > 7 {
		for q := 0; q < r.N; q++ {
			if inst.lb.committedDeps[q] < pareply.CommittedDeps[q] {
				inst.lb.committedDeps[q] = pareply.CommittedDeps[q]
			}
			if inst.lb.committedDeps[q] < r.CommittedUpTo[q] {
				inst.lb.committedDeps[q] = r.CommittedUpTo[q]
			}
			if inst.lb.committedDeps[q] < inst.Deps[q] {
				allCommitted = false
			}
		}
	}

	if lb.status <= PREACCEPTED_EQ {
		lb.deps = deps
		lb.seq = seq
	}

	precondition := inst.lb.allEqual && allCommitted && isInitialBallot

	if inst.lb.preAcceptOKs >= (r.Replica.FastQuorumSize()-1) && precondition {
		lb.status = COMMITTED

		inst.Status = lb.status
		inst.bal = lb.ballot
		inst.Cmds = lb.cmds
		inst.Deps = lb.deps
		inst.Seq = lb.seq
		r.recordInstanceMetadata(inst)
		r.sync()

		r.updateCommitted(pareply.Replica)
		if inst.lb.clientProposals != nil && !r.Dreply {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ReplyProposeTS(
					&defs.ProposeReplyTS{
						TRUE,
						inst.lb.clientProposals[i].CommandId,
						state.NIL(),
						inst.lb.clientProposals[i].Timestamp},
					inst.lb.clientProposals[i].Reply,
					inst.lb.clientProposals[i].Mutex)
			}
		}

		r.bcastCommit(pareply.Replica, pareply.Instance)

		r.M.Lock()
		r.Stats.M["fast"]++
		if inst.proposeTime != 0 {
			r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
		}
		r.M.Unlock()
	} else if inst.lb.preAcceptOKs >= r.Replica.FastQuorumSize()-1 {
		lb.status = ACCEPTED

		inst.Status = lb.status
		inst.bal = lb.ballot
		inst.Cmds = lb.cmds
		inst.Deps = lb.deps
		inst.Seq = lb.seq
		r.recordInstanceMetadata(inst)
		r.sync()

		r.bcastAccept(pareply.Replica, pareply.Instance)

		r.M.Lock()
		r.Stats.M["slow"]++
		if !allCommitted {
			r.Stats.M["weird"]++
		}
		r.M.Unlock()
	}
}

func (r *Replica) handleAccept(accept *Accept) {
	inst := r.InstanceSpace[accept.Replica][accept.Instance]

	if accept.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = accept.Ballot
	}

	if accept.Instance > r.crtInstance[accept.Replica] {
		r.crtInstance[accept.Replica] = accept.Instance
	}

	if inst == nil {
		inst = r.newInstanceDefault(accept.Replica, accept.Instance)
		r.InstanceSpace[accept.Replica][accept.Instance] = inst
	}

	if accept.Ballot < inst.bal {
		r.Printf("Smaller ballot %d < %d\n", accept.Ballot, inst.bal)
	} else if inst.Status >= COMMITTED {
		r.Printf("Already committed / executed \n")
	} else {
		inst.Deps = accept.Deps
		inst.Seq = accept.Seq
		inst.bal = accept.Ballot
		inst.vbal = accept.Ballot
		r.recordInstanceMetadata(r.InstanceSpace[accept.Replica][accept.Instance])
		r.sync()
	}

	reply := &AcceptReply{accept.Replica, accept.Instance, inst.bal}
	r.replyAccept(accept.LeaderId, reply)

}

func (r *Replica) handleAcceptReply(areply *AcceptReply) {
	inst := r.InstanceSpace[areply.Replica][areply.Instance]
	lb := inst.lb

	if areply.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = areply.Ballot
	}

	if lb.status != ACCEPTED {
		return
	}

	if lb.lastTriedBallot != areply.Ballot {
		return
	}

	if areply.Ballot > lb.lastTriedBallot {
		lb.nacks++
		if lb.nacks+1 > r.N>>1 {
			if r.IsLeader {
				r.makeBallot(areply.Replica, areply.Instance)
				r.bcastPrepare(areply.Replica, areply.Instance)
			}
		}
		return
	}

	inst.lb.acceptOKs++

	if inst.lb.acceptOKs+1 > r.N/2 {
		lb.status = COMMITTED
		inst.Status = COMMITTED
		r.updateCommitted(areply.Replica)
		r.recordInstanceMetadata(inst)
		r.sync()

		if inst.lb.clientProposals != nil && !r.Dreply {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ReplyProposeTS(
					&defs.ProposeReplyTS{
						TRUE,
						inst.lb.clientProposals[i].CommandId,
						state.NIL(),
						inst.lb.clientProposals[i].Timestamp},
					inst.lb.clientProposals[i].Reply,
					inst.lb.clientProposals[i].Mutex)
			}
		}

		r.bcastCommit(areply.Replica, areply.Instance)
		r.M.Lock()
		if inst.proposeTime != 0 {
			r.Stats.M["totalCommitTime"] += int(time.Now().UnixNano() - inst.proposeTime)
		}
		r.M.Unlock()
	}
}

func (r *Replica) handleCommit(commit *Commit) {
	inst := r.InstanceSpace[commit.Replica][commit.Instance]

	if commit.Instance > r.crtInstance[commit.Replica] {
		r.crtInstance[commit.Replica] = commit.Instance
	}

	if commit.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = commit.Ballot
	}

	if inst == nil {
		r.InstanceSpace[commit.Replica][commit.Instance] = r.newInstanceDefault(commit.Replica, commit.Instance)
		inst = r.InstanceSpace[commit.Replica][commit.Instance]
	}

	if inst.Status >= COMMITTED {
		return
	}

	if commit.Ballot < inst.bal {
		return
	}

	// FIXME timeout on client side?
	if commit.Replica == r.Id {
		if len(commit.Command) == 1 && commit.Command[0].Op == state.NONE && inst.lb.clientProposals != nil {
			for _, p := range inst.lb.clientProposals {
				r.Printf("In %d.%d, re-proposing %s \n", commit.Replica, commit.Instance, p.Command.String())
				r.ProposeChan <- p
			}
			inst.lb.clientProposals = nil
		}
	}

	inst.bal = commit.Ballot
	inst.vbal = commit.Ballot
	inst.Cmds = commit.Command
	inst.Seq = commit.Seq
	inst.Deps = commit.Deps
	inst.Status = COMMITTED

	r.updateConflicts(commit.Command, commit.Replica, commit.Instance, commit.Seq)
	r.updateCommitted(commit.Replica)
	r.recordInstanceMetadata(r.InstanceSpace[commit.Replica][commit.Instance])
	r.recordCommands(commit.Command)

}

/**********************************************************************

                     RECOVERY ACTIONS

***********************************************************************/

func (r *Replica) BeTheLeader(args *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
	r.IsLeader = true
	r.Println("I am the leader")
	return nil
}

func (r *Replica) startRecoveryForInstance(replica int32, instance int32) {
	inst := r.InstanceSpace[replica][instance]
	if inst == nil {
		inst = r.newInstanceDefault(replica, instance)
		r.InstanceSpace[replica][instance] = inst
	} else if inst.Status >= COMMITTED && inst.Cmds != nil {
		r.Printf("No need to recover %d.%d", replica, instance)
		return
	}

	// no TLA guidance here (some difference with the original implementation)
	var proposals []*defs.GPropose = nil
	if inst.lb != nil {
		proposals = inst.lb.clientProposals
	}
	inst.lb = r.newLeaderBookkeepingDefault()
	lb := inst.lb
	lb.clientProposals = proposals
	lb.ballot = inst.vbal
	lb.seq = inst.Seq
	lb.cmds = inst.Cmds
	lb.deps = inst.Deps
	lb.status = inst.Status
	r.makeBallot(replica, instance)

	inst.bal = lb.lastTriedBallot
	inst.vbal = lb.lastTriedBallot
	preply := &PrepareReply{
		r.Id,
		replica,
		instance,
		inst.bal,
		inst.vbal,
		inst.Status,
		inst.Cmds,
		inst.Seq,
		inst.Deps}

	lb.prepareReplies = append(lb.prepareReplies, preply)
	lb.leaderResponded = r.Id == replica

	r.bcastPrepare(replica, instance)
}

func (r *Replica) handlePrepare(prepare *Prepare) {
	inst := r.InstanceSpace[prepare.Replica][prepare.Instance]
	var preply *PrepareReply

	if prepare.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = prepare.Ballot
	}

	if inst == nil {
		r.InstanceSpace[prepare.Replica][prepare.Instance] = r.newInstanceDefault(prepare.Replica, prepare.Instance)
		inst = r.InstanceSpace[prepare.Replica][prepare.Instance]
	}

	if prepare.Ballot < inst.bal {
		r.Printf("Joined higher ballot %d < %d", prepare.Ballot, inst.bal)
	} else if inst.bal < prepare.Ballot {
		r.Printf("Joining ballot %d ", prepare.Ballot)
		inst.bal = prepare.Ballot
	}

	preply = &PrepareReply{
		r.Id,
		prepare.Replica,
		prepare.Instance,
		inst.bal,
		inst.vbal,
		inst.Status,
		inst.Cmds,
		inst.Seq,
		inst.Deps}
	r.replyPrepare(prepare.LeaderId, preply)
}

func (r *Replica) handlePrepareReply(preply *PrepareReply) {
	inst := r.InstanceSpace[preply.Replica][preply.Instance]
	lb := inst.lb

	if preply.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = preply.Ballot
	}

	if inst == nil || lb == nil || !lb.preparing {
		return
	}

	if preply.Ballot != lb.lastTriedBallot {
		lb.nacks++
		return
	}

	lb.prepareReplies = append(lb.prepareReplies, preply)
	if len(lb.prepareReplies) < r.Replica.SlowQuorumSize() {
		return
	}

	lb.preparing = false

	// Deal with each sub-cases in order of the (corrected) TLA specification
	// only replies from the highest ballot are taken into account
	// 1 -> committed/executed
	// 2 -> accepted
	// 3 -> pre-accepted > f (not including the leader) and allEqual
	// 4 -> pre-accepted >= f/2 (not including the leader) and allEqual
	// 5 -> pre-accepted > 0 and (disagreeing or leader replied or pre-accepted < f/2)
	// 6 -> none of the above
	preAcceptCount := 0
	subCase := 0
	allEqual := true
	for _, element := range lb.prepareReplies {
		if element.VBallot >= lb.ballot {
			lb.ballot = element.VBallot
			lb.cmds = element.Command
			lb.seq = element.Seq
			lb.deps = element.Deps
			lb.status = element.Status
		}
		if element.AcceptorId == element.Replica {
			lb.leaderResponded = true
		}
		if element.Status == PREACCEPTED_EQ || element.Status == PREACCEPTED {
			preAcceptCount++
		}
	}

	if lb.status >= COMMITTED { // 1
		subCase = 1
	} else if lb.status == ACCEPTED { // 2
		subCase = 2
	} else if lb.status == PREACCEPTED || lb.status == PREACCEPTED_EQ {
		for _, element := range lb.prepareReplies {
			if element.VBallot == lb.ballot && element.Status >= PREACCEPTED {
				_, _, equal := r.mergeAttributes(lb.seq, lb.deps, element.Seq, element.Deps)
				if !equal {
					allEqual = false
					break
				}
			}
		}
		if preAcceptCount >= r.Replica.SlowQuorumSize()-1 && !lb.leaderResponded && allEqual {
			subCase = 3
		} else if preAcceptCount >= r.Replica.SlowQuorumSize()-1 && !lb.leaderResponded && allEqual {
			subCase = 4
		} else if preAcceptCount > 0 && (lb.leaderResponded || !allEqual || preAcceptCount < r.Replica.SlowQuorumSize()-1) {
			subCase = 5
		} else {
			panic("Cannot occur")
		}
	} else if lb.status == NONE {
		subCase = 6
	} else {
		panic("Status unknown")
	}

	// if subCase != 5 {
	// 	dlog.Printf("In %d.%d, sub-case %d\n", preply.Replica, preply.Instance, subCase)
	// } else {
	// 	dlog.Printf("In %d.%d, sub-case %d with (leaderResponded=%t, allEqual=%t, enough=%t)\n",
	// 		preply.Replica, preply.Instance, subCase, lb.leaderResponded, allEqual, preAcceptCount < r.Replica.SlowQuorumSize()-1)
	// }

	inst.Cmds = lb.cmds
	inst.bal = lb.lastTriedBallot
	inst.vbal = lb.lastTriedBallot
	inst.Seq = lb.seq
	inst.Deps = lb.deps
	inst.Status = lb.status

	if subCase == 1 {
		// nothing to do
	} else if subCase == 2 || subCase == 3 {
		inst.Status = ACCEPTED
		lb.status = ACCEPTED
		r.bcastAccept(preply.Replica, preply.Instance)
	} else if subCase == 4 {
		lb.tryingToPreAccept = true
		r.bcastTryPreAccept(preply.Replica, preply.Instance)
	} else { // subCase 5 and 6
		cmd := state.NOOP()
		if inst.lb.cmds != nil {
			cmd = inst.lb.cmds
		}
		r.startPhase1(cmd, preply.Replica, preply.Instance, lb.lastTriedBallot, lb.clientProposals)
	}
}

func (r *Replica) handleTryPreAccept(tpa *TryPreAccept) {
	inst := r.InstanceSpace[tpa.Replica][tpa.Instance]

	if inst == nil {
		r.InstanceSpace[tpa.Replica][tpa.Instance] = r.newInstanceDefault(tpa.Replica, tpa.Instance)
		inst = r.InstanceSpace[tpa.Replica][tpa.Instance]
	}

	if inst.bal > tpa.Ballot {
		r.Printf("Smaller ballot %d < %d\n", tpa.Ballot, inst.bal)
		return
	}
	inst.bal = tpa.Ballot

	confRep := int32(0)
	confInst := int32(0)
	confStatus := NONE
	if inst.Status == NONE { // missing in TLA spec.
		if conflict, cr, ci := r.findPreAcceptConflicts(tpa.Command, tpa.Replica, tpa.Instance, tpa.Seq, tpa.Deps); conflict {
			confRep = cr
			confInst = ci
		} else {
			if tpa.Instance > r.crtInstance[tpa.Replica] {
				r.crtInstance[tpa.Replica] = tpa.Instance
			}
			inst.Cmds = tpa.Command
			inst.Seq = tpa.Seq
			inst.Deps = tpa.Deps
			inst.Status = PREACCEPTED
		}
	}

	rtpa := &TryPreAcceptReply{r.Id, tpa.Replica, tpa.Instance, inst.bal, inst.vbal, confRep, confInst, confStatus}

	r.replyTryPreAccept(tpa.LeaderId, rtpa)

}

func (r *Replica) findPreAcceptConflicts(cmds []state.Command, replica int32, instance int32, seq int32, deps []int32) (bool, int32, int32) {
	inst := r.InstanceSpace[replica][instance]
	if inst != nil && len(inst.Cmds) > 0 {
		if inst.Status >= ACCEPTED {
			// already ACCEPTED or COMMITTED
			// we consider this a conflict because we shouldn't regress to PRE-ACCEPTED
			return true, replica, instance
		}
		if inst.Seq == seq && equal(inst.Deps, deps) {
			// already PRE-ACCEPTED, no point looking for conflicts again
			return false, replica, instance
		}
	}
	for q := int32(0); q < int32(r.N); q++ {
		for i := r.ExecedUpTo[q]; i <= r.crtInstance[q]; i++ { // FIXME this is not enough imho.
			if i == -1 {
				//do not check placeholder
				continue
			}
			if replica == q && instance == i {
				// no point checking past instance in replica's row, since replica would have
				// set the dependencies correctly for anything started after instance
				break
			}
			if i == deps[q] {
				//the instance cannot be a dependency for itself
				continue
			}
			inst := r.InstanceSpace[q][i]
			if inst == nil || inst.Cmds == nil || len(inst.Cmds) == 0 {
				continue
			}
			if inst.Deps[replica] >= instance {
				// instance q.i depends on instance replica.instance, it is not a conflict
				continue
			}
			if r.LRead || state.ConflictBatch(inst.Cmds, cmds) {
				if i > deps[q] ||
					(i < deps[q] && inst.Seq >= seq && (q != replica || inst.Status > PREACCEPTED_EQ)) {
					// this is a conflict
					return true, q, i
				}
			}
		}
	}
	return false, -1, -1
}

func (r *Replica) handleTryPreAcceptReply(tpar *TryPreAcceptReply) {
	inst := r.InstanceSpace[tpar.Replica][tpar.Instance]

	if tpar.Ballot > r.maxRecvBallot {
		r.maxRecvBallot = tpar.Ballot
	}

	if inst == nil {
		r.InstanceSpace[tpar.Replica][tpar.Instance] = r.newInstanceDefault(tpar.Replica, tpar.Instance)
		inst = r.InstanceSpace[tpar.Replica][tpar.Instance]
	}

	lb := inst.lb
	if lb == nil || !lb.tryingToPreAccept {
		return
	}

	if tpar.Ballot != lb.lastTriedBallot {
		return
	}

	lb.tpaReps++

	if tpar.VBallot == lb.lastTriedBallot {
		lb.preAcceptOKs++
		if lb.preAcceptOKs >= r.N/2 {
			//it's safe to start Accept phase
			lb.status = ACCEPTED
			lb.tryingToPreAccept = false
			lb.acceptOKs = 0

			inst.Cmds = lb.cmds
			inst.Seq = lb.seq
			inst.Deps = lb.deps
			inst.Status = lb.status
			inst.vbal = lb.lastTriedBallot
			inst.bal = lb.lastTriedBallot

			r.bcastAccept(tpar.Replica, tpar.Instance)
			return
		}
	} else {
		lb.nacks++
		lb.possibleQuorum[tpar.AcceptorId] = false
		lb.possibleQuorum[tpar.ConflictReplica] = false
	}

	lb.tpaAccepted = lb.tpaAccepted || (tpar.ConflictStatus >= ACCEPTED) // TLA spec. (page 39)

	if lb.tpaReps >= r.Replica.SlowQuorumSize()-1 && lb.tpaAccepted {
		//abandon recovery, restart from phase 1
		lb.tryingToPreAccept = false
		r.startPhase1(lb.cmds, tpar.Replica, tpar.Instance, lb.lastTriedBallot, lb.clientProposals)
		return
	}

	// the code below is not checked in TLA (liveness)
	notInQuorum := 0
	for q := 0; q < r.N; q++ {
		if !lb.possibleQuorum[tpar.AcceptorId] {
			notInQuorum++
		}
	}

	if notInQuorum == r.N/2 {
		//this is to prevent defer cycles
		if present, dq, _ := deferredByInstance(tpar.Replica, tpar.Instance); present {
			if lb.possibleQuorum[dq] {
				//an instance whose leader must have been in this instance's quorum has been deferred for this instance => contradiction
				//abandon recovery, restart from phase 1
				lb.tryingToPreAccept = false
				r.makeBallot(tpar.Replica, tpar.Instance)
				r.startPhase1(lb.cmds, tpar.Replica, tpar.Instance, lb.lastTriedBallot, lb.clientProposals)
				return
			}
		}
	}

	if lb.tpaReps >= r.N/2 {
		//defer recovery and update deferred information
		updateDeferred(tpar.Replica, tpar.Instance, tpar.ConflictReplica, tpar.ConflictInstance)
		lb.tryingToPreAccept = false
	}
}

// helper functions and structures to prevent defer cycles while recovering

var deferMap = make(map[uint64]uint64)

func updateDeferred(dr int32, di int32, r int32, i int32) {
	daux := (uint64(dr) << 32) | uint64(di)
	aux := (uint64(r) << 32) | uint64(i)
	deferMap[aux] = daux
}

func deferredByInstance(q int32, i int32) (bool, int32, int32) {
	aux := (uint64(q) << 32) | uint64(i)
	daux, present := deferMap[aux]
	if !present {
		return false, 0, 0
	}
	dq := int32(daux >> 32)
	di := int32(daux)
	return true, dq, di
}

func (r *Replica) newInstanceDefault(replica int32, instance int32) *Instance {
	return r.newInstance(replica, instance, nil, -1, -1, NONE, -1, nil)
}

func (r *Replica) newInstance(replica int32, instance int32, cmds []state.Command, cballot int32, lballot int32, status int8, seq int32, deps []int32) *Instance {
	return &Instance{cmds, cballot, lballot, status, seq, deps, nil, 0, 0, nil, time.Now().UnixNano(), &instanceId{replica, instance}}
}

func (r *Replica) newLeaderBookkeepingDefault() *LeaderBookkeeping {
	return r.newLeaderBookkeeping(nil, r.newNilDeps(), r.newNilDeps(), r.newNilDeps(), 0, nil, NONE, -1)
}

func (r *Replica) newLeaderBookkeeping(p []*defs.GPropose, originalDeps []int32, committedDeps []int32, deps []int32, lastTriedBallot int32, cmds []state.Command, status int8, seq int32) *LeaderBookkeeping {
	return &LeaderBookkeeping{p, -1, true, 0, 0, 0, originalDeps, committedDeps, nil, true, false, make([]bool, r.N), 0, false, lastTriedBallot, cmds, status, seq, deps, false}
}

func (r *Replica) newNilDeps() []int32 {
	nildeps := make([]int32, r.N)
	for i := 0; i < r.N; i++ {
		nildeps[i] = -1
	}
	return nildeps
}
