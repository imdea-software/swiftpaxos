package swift

import (
	"fmt"
	"sync"
	"time"

	"github.com/imdea-software/swiftpaxos/config"
	"github.com/imdea-software/swiftpaxos/dlog"
	"github.com/imdea-software/swiftpaxos/hook"
	"github.com/imdea-software/swiftpaxos/replica"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	"github.com/imdea-software/swiftpaxos/state"
	"github.com/orcaman/concurrent-map"
)

type Replica struct {
	*replica.Replica

	ballot  int32
	cballot int32
	status  int

	cmdDescs  cmap.ConcurrentMap
	delivered cmap.ConcurrentMap

	sender  replica.Sender
	batcher *Batcher
	repchan *replyChan

	keys map[state.Key]keyInfo
	hlog map[state.Key]*HashLog

	seqnum          int
	pendingHashUpds map[CommandId]*UpdateEntry

	history      []commandStaticDesc
	historySize  int
	historyStart int

	qs *replica.QuorumSystem
	SQ replica.QuorumI
	FQ replica.QuorumI
	cs CommunicationSupply

	fixedMajority bool

	optExec     bool
	fastRead    bool
	deliverChan chan CommandId

	descPool     sync.Pool
	poolLevel    int
	routineCount int

	recover        chan int32
	recStart       time.Time
	newLeaderAckNs *replica.MsgSet

	proposes map[CommandId]*defs.GPropose

	// take only slow paths for these addresses
	slowAddrs map[string]struct{}
}

type commandDesc struct {
	phase      int
	cmd        state.Command
	dep        Dep
	hs         []SHash
	propose    *defs.GPropose
	proposeDep Dep

	slowPathH      *replica.MsgSet
	fastPathH      *replica.MsgSet
	afterPropagate *hook.OptCondF

	msgs     chan interface{}
	active   bool
	slowPath bool
	seq      bool
	stopChan chan *sync.WaitGroup

	successors  []CommandId
	successorsL sync.Mutex

	// execute before sending an MSync message
	defered func()
}

type commandStaticDesc struct {
	cmdId    CommandId
	phase    int
	cmd      state.Command
	dep      Dep
	slowPath bool
	defered  func()
}

type readDesc struct {
	hs      []SHash
	dep     Dep
	propose *defs.GPropose
}

func New(alias string, rid int, addrs []string, exec, fastRead, optExec, AQreconf bool,
	pl, f int, conf *config.Config, l *dlog.Logger, slowAddrs []string) *Replica {
	cmap.SHARD_COUNT = 32768

	r := &Replica{
		Replica: replica.New(alias, rid, f, addrs, false, exec, false, conf, l),

		ballot:  0,
		cballot: 0,
		status:  NORMAL,

		cmdDescs:  cmap.New(),
		delivered: cmap.New(),

		keys:            make(map[state.Key]keyInfo),
		hlog:            make(map[state.Key]*HashLog),
		seqnum:          0,
		pendingHashUpds: make(map[CommandId]*UpdateEntry),

		history:      make([]commandStaticDesc, HISTORY_SIZE),
		historySize:  0,
		historyStart: 0,

		fixedMajority: false,

		optExec:     optExec,
		fastRead:    fastRead,
		deliverChan: make(chan CommandId, defs.CHAN_BUFFER_SIZE),

		poolLevel:    pl,
		routineCount: 0,

		recover: make(chan int32, 8),

		descPool: sync.Pool{
			New: func() interface{} {
				return &commandDesc{}
			},
		},

		proposes: make(map[CommandId]*defs.GPropose),

		slowAddrs: make(map[string]struct{}),
	}

	for _, addr := range slowAddrs {
		r.slowAddrs[addr] = struct{}{}
	}

	useFastAckPool = pl > 1

	r.SQ = replica.NewMajorityOf(r.N)
	r.FQ = replica.NewThreeQuartersOf(r.N)

	r.sender = replica.NewSender(r.Replica)
	r.batcher = NewBatcher(r, 16, releaseFastAck, func(_ *MLightSlowAck) {})
	r.repchan = NewReplyChan(r)

	qs, err := replica.NewQuorumSystem(r.N/2+1, r.Replica, conf.Quorum)
	if err != nil && err != replica.THREE_QUARTERS {
		r.Fatal(err)
	}
	r.qs = qs
	r.ballot = r.qs.BallotAt(0)
	if r.ballot == -1 {
		r.ballot = 0
	}
	r.cballot = r.ballot
	if err != replica.THREE_QUARTERS {
		r.fixedMajority = true
		r.FQ = r.qs.AQ(r.ballot)
	}

	initCs(&r.cs, r.RPC)

	r.Println("the leader is:", r.leader(), "ballot is:", r.ballot)

	hook.HookUser1(func() {
		totalNum := 0
		slowPaths := 0
		for i := 0; i < HISTORY_SIZE; i++ {
			if r.history[i].dep == nil {
				continue
			}
			totalNum++
			if r.history[i].slowPath {
				slowPaths++
			}
		}

		fmt.Printf("Total number of commands: %d\n", totalNum)
		fmt.Printf("Number of slow paths: %d\n", slowPaths)
	})

	r.Println("SQ:", r.SQ)
	r.Println("FQ:", r.FQ)

	go r.run()

	return r
}

// TODO: do something more elegant
func (r *Replica) BeTheLeader(_ *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
	if !r.delivered.IsEmpty() {
		b := r.qs.BallotAt(1)
		r.recover <- b
		reply.Leader = r.Id
	} else {
		reply.Leader = r.leader()
	}
	reply.NextLeader = replica.Leader(r.qs.BallotAt(1), r.N)
	if reply.Leader == 0 {
		reply.Leader = -2
	}
	if reply.NextLeader == 0 {
		reply.NextLeader = -2
	}
	return nil
}

func (r *Replica) run() {
	r.ConnectToPeers()
	latencies := r.ComputeClosestPeers()
	for _, l := range latencies {
		d := time.Duration(l*1000*1000) * time.Nanosecond
		if d > r.cs.maxLatency {
			r.cs.maxLatency = d
		}
	}

	go r.WaitForClientConnections()

	var cmdId CommandId

	for !r.Shutdown {
		select {
		case newBallot := <-r.recover:
			newLeader := &MNewLeader{
				Replica: r.Id,
				Ballot:  r.ballot,
			}
			if newBallot != -1 {
				if newBallot > newLeader.Ballot {
					newLeader.Ballot = newBallot
				} else {
					newLeader.Ballot = r.qs.SameHigher(newBallot, newLeader.Ballot)
				}
			} else {
				newLeader.Ballot = replica.NextBallotOf(r.Id, newLeader.Ballot, r.N)
			}
			for quorumIsAlive := false; r.fixedMajority && !quorumIsAlive; {
				quorumIsAlive = true
				for rid := range r.qs.AQ(newLeader.Ballot) {
					if rid != r.Id && !r.Alive[rid] {
						newLeader.Ballot = replica.NextBallotOf(r.Id, newLeader.Ballot, r.N)
						quorumIsAlive = false
						break
					}
				}
			}
			r.sender.SendToAll(newLeader, r.cs.newLeaderRPC)
			r.reinitNewLeaderAckNs()
			r.handleNewLeader(newLeader)

		case cmdId := <-r.deliverChan:
			r.getCmdDesc(cmdId, "deliver", nil)

		case propose := <-r.ProposeChan:
			cmdId.ClientId = propose.ClientId
			cmdId.SeqNum = propose.CommandId
			r.proposes[cmdId] = propose
			dep, hs := r.getDepAndHashes(propose.Command, cmdId)
			if upd, exists := r.pendingHashUpds[cmdId]; exists && r.leader() != r.Id {
				// TODO: when pipelining this can break ordering, disabling fast paths.
				delete(r.pendingHashUpds, cmdId)
				r.recordLeaderHash(cmdId, upd.seqnum, upd.hash)
			}
			// FIXME: leader can receive fastAck before Propose,
			//        in this case desc is not seq
			desc := r.getCmdDescSeq(cmdId, propose, dep, hs, r.leader() == r.Id)
			if desc == nil {
				r.Fatal("Got proposal for the delivered command", cmdId)
			}

		case m := <-r.cs.fastAckChan:
			fastAck := m.(*MFastAck)
			if fastAck.Replica == r.leader() && r.leader() != r.Id {
				// TODO: again, think about pipeline.
				r.recordLeaderHash(fastAck.CmdId, fastAck.Seqnum, fastAck.Checksum)
			}
			// This is a simple fix of the FIXME above:
			r.getCmdDescSeq(fastAck.CmdId, fastAck, nil, nil, r.leader() == r.Id)
			// r.getCmdDescSeq(fastAck.CmdId, fastAck, nil, nil, false)

		case m := <-r.cs.lightSlowAckChan:
			lightSlowAck := m.(*MLightSlowAck)
			r.getCmdDesc(lightSlowAck.CmdId, lightSlowAck, nil)

		case m := <-r.cs.acksChan:
			acks := m.(*MAcks)
			for _, f := range acks.FastAcks {
				if f.Replica == r.leader() && r.leader() != r.Id {
					// TODO: again, think about pipeline.
					r.recordLeaderHash(f.CmdId, f.Seqnum, f.Checksum)
				}
				// This is a simple fix of the FIXME above:
				r.getCmdDescSeq(f.CmdId, copyFastAck(&f), nil, nil, r.leader() == r.Id)
				// r.getCmdDescSeq(f.CmdId, copyFastAck(&f), nil, nil, false)
			}
			for _, s := range acks.LightSlowAcks {
				ls := s
				r.getCmdDesc(s.CmdId, &ls, nil)
			}

		case m := <-r.cs.optAcksChan:
			optAcks := m.(*MOptAcks)
			for _, ack := range optAcks.Acks {
				fastAck := newFastAck()
				fastAck.Replica = optAcks.Replica
				fastAck.Ballot = optAcks.Ballot
				fastAck.CmdId = ack.CmdId
				fastAck.Checksum = ack.Checksum
				fastAck.Seqnum = ack.Seqnum
				if !IsNilDepOfCmdId(ack.CmdId, ack.Dep) {
					fastAck.Dep = ack.Dep
				} else {
					fastAck.Dep = nil
				}
				if fastAck.Replica == r.leader() && r.leader() != r.Id {
					// TODO: again, think about pipeline.
					r.recordLeaderHash(fastAck.CmdId, fastAck.Seqnum, fastAck.Checksum)
				}
				// This is a simple fix of the FIXME above:
				r.getCmdDescSeq(fastAck.CmdId, fastAck, nil, nil, r.leader() == r.Id)
				// r.getCmdDescSeq(fastAck.CmdId, fastAck, nil, nil, false)
			}

		case m := <-r.cs.newLeaderChan:
			newLeader := m.(*MNewLeader)
			r.handleNewLeader(newLeader)

		case m := <-r.cs.syncChan:
			sync := m.(*MSync)
			r.handleSync(sync)
		}
	}
}

func (r *Replica) handlePropose(msg *defs.GPropose, desc *commandDesc, cmdId CommandId) {
	if r.status != NORMAL || desc.propose != nil {
		return
	}

	desc.propose = msg
	desc.cmd = msg.Command

	if !r.FQ.Contains(r.Id) {
		desc.afterPropagate.Recall()
		return
	}

	desc.dep = desc.proposeDep
	desc.phase = PRE_ACCEPT
	if (desc.afterPropagate.Recall() && desc.slowPath) ||
		r.delivered.Has(cmdId.String()) {
		// in this case a process already sent an MSlowAck
		// message, hence, no need to send MFastAck
		return
	}

	if _, exists := r.slowAddrs[msg.Addr]; exists && r.Id != r.leader() {
		return
	}

	fastAck := newFastAck()
	fastAck.Replica = r.Id
	fastAck.Ballot = r.ballot
	fastAck.CmdId = cmdId
	fastAck.Dep = desc.dep
	fastAck.Checksum = desc.hs
	if r.Id == r.leader() {
		fastAck.Seqnum = r.seqnum
		r.seqnum++
	}

	isRead := r.fastRead && msg.Command.Op != state.PUT

	fastAckSend := copyFastAck(fastAck)
	if !r.optExec {
		r.batcher.SendFastAck(fastAckSend)
	} else {
		if (r.Id == r.leader() && !isRead) || (isRead && desc.propose.Proxy) {
			r.batcher.SendFastAck(fastAckSend)
			// TODO: save old state
			r.deliver(desc, cmdId)
		} else {
			r.batcher.SendFastAckClient(fastAckSend, msg.ClientId)
		}
	}
	r.handleFastAck(fastAck, desc)
}

func (r *Replica) handleFastAck(msg *MFastAck, desc *commandDesc) {
	if msg.Replica == r.leader() {
		r.fastAckFromLeader(msg, desc)
	} else {
		r.commonCaseFastAck(msg, desc)
	}
}

func (r *Replica) fastAckFromLeader(msg *MFastAck, desc *commandDesc) {
	desc.afterPropagate.Call(func() {
		if r.status != NORMAL || r.ballot != msg.Ballot {
			return
		}

		// TODO: make sure that
		//    ∀ id' ∈ d. phase[id'] ∈ {ACCEPT, COMMIT}
		//
		// seems to be satisfied already

		desc.phase = ACCEPT
		dep := Dep(msg.Dep)
		hs := desc.hs
		neq := !desc.dep.Equals(dep)
		fast := r.FQ.Contains(r.Id)
		slow := r.SQ.Contains(r.Id)
		sendSlowAck := r.leader() != r.Id && (slow || (fast && neq))
		msgCmdId := msg.CmdId
		msgChecksum := msg.Checksum

		defer func() {
			if r.leader() == r.Id || r.delivered.Has(msgCmdId.String()) {
				return
			}
			if !sendSlowAck && r.optExec && !SHashesEq(hs, msgChecksum) {
				lightSlowAck := &MLightSlowAck{
					Replica: r.Id,
					Ballot:  r.ballot,
					CmdId:   msgCmdId,
				}

				r.sender.SendToClient(msgCmdId.ClientId, lightSlowAck, r.cs.lightSlowAckRPC)
			}
		}()

		desc.slowPathH.Add(msg.Replica, true, msg)
		delivered := r.delivered.Has(msgCmdId.String())
		if !delivered {
			desc.fastPathH.Add(msg.Replica, true, msg)
			delivered = r.delivered.Has(msgCmdId.String())
		}

		if sendSlowAck {
			if neq && !delivered {
				desc.dep = dep
			}
			desc.slowPath = true

			lightSlowAck := &MLightSlowAck{
				Replica: r.Id,
				Ballot:  r.ballot,
				CmdId:   msgCmdId,
			}

			if !r.optExec {
				r.batcher.SendLightSlowAck(lightSlowAck)
			} else {
				r.batcher.SendLightSlowAckClient(lightSlowAck, msgCmdId.ClientId)
			}
			if !delivered {
				r.handleLightSlowAck(lightSlowAck, desc)
			}
		}
	})
}

func (r *Replica) commonCaseFastAck(msg *MFastAck, desc *commandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	msgCmdId := msg.CmdId
	if msg.Dep == nil {
		desc.slowPathH.Add(msg.Replica, msg.Replica == r.leader(), msg)
		if r.delivered.Has(msgCmdId.String()) {
			return
		}
	}
	desc.fastPathH.Add(msg.Replica, msg.Replica == r.leader(), msg)
}

func getFastAndSlowAcksHandler(r *Replica, desc *commandDesc) replica.MsgSetHandler {
	return func(leaderMsg interface{}, msgs []interface{}) {

		if leaderMsg == nil {
			return
		}

		leaderFastAck := leaderMsg.(*MFastAck)

		desc.phase = COMMIT

		for _, depCmdId := range desc.dep {
			depDesc := r.getCmdDesc(depCmdId, nil, nil)
			if depDesc == nil {
				continue
			}
			depDesc.successorsL.Lock()
			depDesc.successors = append(depDesc.successors, leaderFastAck.CmdId)
			depDesc.successorsL.Unlock()
		}

		r.deliver(desc, leaderFastAck.CmdId)
	}
}

func (r *Replica) handleLightSlowAck(msg *MLightSlowAck, desc *commandDesc) {
	fastAck := newFastAck()
	fastAck.Replica = msg.Replica
	fastAck.Ballot = msg.Ballot
	fastAck.CmdId = msg.CmdId
	fastAck.Dep = nil
	r.commonCaseFastAck(fastAck, desc)
}

func (r *Replica) deliver(desc *commandDesc, cmdId CommandId) {
	// TODO: what if desc.propose is nil ?
	//       is that possible ?
	//
	//       Don't think so
	//       Now I do
	// TODO: How is that possible ?

	if desc.propose == nil || r.delivered.Has(cmdId.String()) || !r.Exec {
		return
	}

	if desc.phase != COMMIT && (!r.optExec || r.Id != r.leader()) {
		return
	}

	for _, cmdIdPrime := range desc.dep {
		if !r.delivered.Has(cmdIdPrime.String()) {
			return
		}
	}

	r.delivered.Set(cmdId.String(), struct{}{})
	v := desc.cmd.Execute(r.State)

	desc.successorsL.Lock()
	if desc.successors != nil {
		for _, sucCmdId := range desc.successors {
			go func(sucCmdId CommandId) {
				r.deliverChan <- sucCmdId
			}(sucCmdId)
		}
	}
	desc.successorsL.Unlock()

	if !r.Dreply {
		return
	}

	r.repchan.reply(desc, cmdId, v)
	if desc.seq {
		// wait for the slot number and ignore any other message
		for {
			switch slot := (<-desc.msgs).(type) {
			case int:
				r.handleMsg(slot, desc, cmdId)
				return
			}
		}
	}
}

func (r *Replica) getCmdDesc(cmdId CommandId, msg interface{}, dep Dep) *commandDesc {
	return r.getCmdDescSeq(cmdId, msg, dep, nil, false)
}

func (r *Replica) getCmdDescSeq(cmdId CommandId, msg interface{}, dep Dep, hs []SHash, seq bool) *commandDesc {
	key := cmdId.String()
	if r.delivered.Has(key) {
		return nil
	}

	var desc *commandDesc

	r.cmdDescs.Upsert(key, nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			defer func() {
				if dep != nil && desc.proposeDep == nil {
					desc.proposeDep = dep
					if hs != nil {
						desc.hs = hs
					}
				}
			}()

			if exists {
				desc = mapV.(*commandDesc)
				return desc
			}

			desc = r.newDesc()
			desc.seq = seq || desc.seq
			if !desc.seq {
				go r.handleDesc(desc, cmdId)
				r.routineCount++
			}

			return desc
		})

	if msg != nil {
		if desc.seq {
			r.handleMsg(msg, desc, cmdId)
		} else {
			desc.msgs <- msg
		}
	}

	return desc
}

func (r *Replica) newDesc() *commandDesc {
	desc := r.allocDesc()
	desc.dep = nil
	desc.hs = nil
	if desc.msgs == nil {
		desc.msgs = make(chan interface{}, 8)
	} else {
		for len(desc.msgs) != 0 {
			<-desc.msgs
		}
	}
	desc.active = true
	desc.phase = START
	desc.successors = nil
	desc.slowPath = false
	desc.seq = (r.routineCount >= MaxDescRoutines)
	desc.defered = func() {}
	desc.propose = nil
	desc.proposeDep = nil
	if desc.stopChan == nil {
		desc.stopChan = make(chan *sync.WaitGroup, 8)
	} else {
		for len(desc.stopChan) != 0 {
			<-desc.stopChan
		}
	}

	desc.afterPropagate = desc.afterPropagate.ReinitCondF(func() bool {
		return desc.propose != nil
	})

	acceptFastAndSlowAck := func(msg, leaderMsg interface{}) bool {
		if leaderMsg == nil {
			return true
		}
		leaderFastAck := leaderMsg.(*MFastAck)
		fastAck := msg.(*MFastAck)
		return fastAck.Dep == nil ||
			(Dep(leaderFastAck.Dep)).Equals(fastAck.Dep)
	}

	freeFastAck := func(msg interface{}) {
		switch f := msg.(type) {
		case *MFastAck:
			releaseFastAck(f)
		}
	}

	h := getFastAndSlowAcksHandler(r, desc)
	desc.slowPathH = desc.slowPathH.ReinitMsgSet(r.SQ, acceptFastAndSlowAck, freeFastAck, h)
	desc.fastPathH = desc.fastPathH.ReinitMsgSet(r.FQ, acceptFastAndSlowAck, freeFastAck, h)

	return desc
}

func (r *Replica) allocDesc() *commandDesc {
	if r.poolLevel > 0 {
		return r.descPool.Get().(*commandDesc)
	}
	return &commandDesc{}
}

func (r *Replica) freeDesc(desc *commandDesc) {
	if r.poolLevel > 0 {
		r.descPool.Put(desc)
	}
}

func (r *Replica) handleDesc(desc *commandDesc, cmdId CommandId) {
	defer func() {
		for len(desc.stopChan) != 0 {
			(<-desc.stopChan).Done()
		}
	}()

	for desc.active {
		select {
		case wg := <-desc.stopChan:
			desc.active = false
			wg.Done()
			return
		case msg := <-desc.msgs:
			if r.handleMsg(msg, desc, cmdId) {
				r.routineCount--
				return
			}
		}
	}
}

func (r *Replica) handleMsg(m interface{}, desc *commandDesc, cmdId CommandId) bool {
	switch msg := m.(type) {

	case *defs.GPropose:
		r.handlePropose(msg, desc, cmdId)

	case *MFastAck:
		if msg.CmdId == cmdId {
			r.handleFastAck(msg, desc)
		}

	case *MLightSlowAck:
		if msg.CmdId == cmdId {
			r.handleLightSlowAck(msg, desc)
		}

	case string:
		if msg == "deliver" {
			r.deliver(desc, cmdId)
		}

	case int:
		r.history[msg].cmdId = cmdId
		r.history[msg].phase = desc.phase
		r.history[msg].cmd = desc.cmd
		r.history[msg].dep = desc.dep
		r.history[msg].slowPath = desc.slowPath
		r.history[msg].defered = desc.defered
		desc.active = false
		desc.slowPathH.Free()
		desc.fastPathH.Free()
		r.cmdDescs.Remove(cmdId.String())
		r.freeDesc(desc)
		return true
	}

	return false
}

func (r *Replica) leader() int32 {
	return replica.Leader(r.ballot, r.N)
}

func (r *Replica) getDepAndHashes(cmd state.Command, cmdId CommandId) (Dep, []SHash) {
	dep := []CommandId{}
	hashes := []SHash{}
	keysOfCmd := keysOf(cmd)

	for _, key := range keysOfCmd {
		info, exists := r.keys[key]
		if exists {
			cdep := info.getConflictCmds(cmd)
			dep = append(dep, cdep...)
		} else {
			info = newLightKeyInfo()
			r.keys[key] = info
		}
		info.add(cmd, cmdId)

		l, exists := r.hlog[key]
		if !exists {
			l = NewHashLog()
			r.hlog[key] = l
		}
		hashes = append(hashes, l.Append(cmd, cmdId))
	}

	return dep, hashes
}

func (r *Replica) recordLeaderHash(cmdId CommandId, s int, hs []SHash) {
	p, exists := r.proposes[cmdId]
	if !exists {
		r.pendingHashUpds[cmdId] = &UpdateEntry{
			hash:   hs,
			seqnum: s,
		}
		return
	}
	r.updateLogs(p.Command, cmdId, s, hs)
}

func (r *Replica) updateLogs(cmd state.Command, cmdId CommandId, s int, hs []SHash) {
	keys := keysOf(cmd)

	if len(keys) != len(hs) {
		r.Fatal("the number of hashes does not match number of objects")
	}

	for i, key := range keys {
		l, exists := r.hlog[key]
		if !exists {
			l = NewHashLog()
			r.hlog[key] = l
		}
		l.Update(cmdId, s, hs[i])
	}
}
