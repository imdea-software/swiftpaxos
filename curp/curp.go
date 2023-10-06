package curp

import (
	"strconv"
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

	optimized      bool
	contactClients bool

	Q replica.Majority

	isLeader    bool
	lastCmdSlot int

	slots     map[CommandId]int
	synced    cmap.ConcurrentMap
	values    cmap.ConcurrentMap
	proposes  cmap.ConcurrentMap
	cmdDescs  cmap.ConcurrentMap
	unsynced  cmap.ConcurrentMap
	executed  cmap.ConcurrentMap
	committed cmap.ConcurrentMap
	delivered cmap.ConcurrentMap

	sender  replica.Sender
	batcher *Batcher
	history []commandStaticDesc

	cs CommunicationSupply

	deliverChan chan int

	descPool     sync.Pool
	poolLevel    int
	routineCount int
}

type commandDesc struct {
	cmdId CommandId

	cmd     state.Command
	phase   int
	cmdSlot int
	propose *defs.GPropose
	val     []byte

	dep        int
	successor  int
	successorL sync.Mutex

	acks         *replica.MsgSet
	afterPayload *hook.OptCondF

	msgs   chan interface{}
	active bool
	seq    bool

	accepted    bool
	pendingCall func()
}

type commandStaticDesc struct {
	cmdSlot int
	phase   int
	cmd     state.Command
}

func New(alias string, rid int, addrs []string, exec bool, pl, f int,
	opt bool, conf *config.Config, logger *dlog.Logger) *Replica {
	cmap.SHARD_COUNT = 32768

	r := &Replica{
		Replica: replica.New(alias, rid, f, addrs, false, exec, false, conf, logger),

		ballot:  0,
		cballot: 0,
		status:  NORMAL,

		optimized:      opt,
		contactClients: false,

		isLeader:    false,
		lastCmdSlot: 0,

		slots:     make(map[CommandId]int),
		synced:    cmap.New(),
		values:    cmap.New(),
		proposes:  cmap.New(),
		cmdDescs:  cmap.New(),
		unsynced:  cmap.New(),
		executed:  cmap.New(),
		committed: cmap.New(),
		delivered: cmap.New(),
		history:   make([]commandStaticDesc, HISTORY_SIZE),

		deliverChan: make(chan int, defs.CHAN_BUFFER_SIZE),

		poolLevel:    pl,
		routineCount: 0,

		descPool: sync.Pool{
			New: func() interface{} {
				return &commandDesc{}
			},
		},
	}

	r.Q = replica.NewMajorityOf(r.N)
	r.sender = replica.NewSender(r.Replica)
	r.batcher = NewBatcher(r, 8)

	_, leaderIds, err := replica.NewQuorumsFromFile(conf.Quorum, r.Replica)
	if err == nil && len(leaderIds) != 0 {
		r.ballot = leaderIds[0]
		r.cballot = leaderIds[0]
		r.isLeader = (leaderIds[0] == r.Id)
	} else if err == replica.NO_QUORUM_FILE {
		r.isLeader = (r.ballot == r.Id)
	} else {
		r.Fatal(err)
	}

	initCs(&r.cs, r.RPC)

	hook.HookUser1(func() {
		totalNum := 0
		for i := 0; i < HISTORY_SIZE; i++ {
			if r.history[i].phase == 0 {
				continue
			}
			totalNum++
		}

		r.Printf("Total number of commands: %d\n", totalNum)
	})

	go r.run()

	return r
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
		case int := <-r.deliverChan:
			r.getCmdDesc(int, "deliver", -1)

		case propose := <-r.ProposeChan:
			if r.isLeader {
				dep := r.leaderUnsync(propose.Command, r.lastCmdSlot)
				desc := r.getCmdDescSeq(r.lastCmdSlot, propose, dep, true) // why Seq?
				if desc == nil {
					r.Fatal("Got propose for the delivered command:",
						propose.ClientId, propose.CommandId)
				}
				r.lastCmdSlot++
			} else {
				cmdId.ClientId = propose.ClientId
				cmdId.SeqNum = propose.CommandId
				if r.values.Has(cmdId.String()) {
					continue
				}
				r.proposes.Set(cmdId.String(), propose)
				recAck := &MRecordAck{
					Replica: r.Id,
					Ballot:  r.ballot,
					CmdId:   cmdId,
					Ok:      r.ok(propose.Command),
				}
				r.sender.SendToClient(propose.ClientId, recAck, r.cs.recordAckRPC)
				r.unsync(propose.Command)
				slot, exists := r.slots[cmdId]
				if exists {
					r.getCmdDesc(slot, "deliver", -1)
				}
			}

		case m := <-r.cs.acceptChan:
			acc := m.(*MAccept)
			if r.values.Has(acc.CmdId.String()) {
				continue
			}
			r.slots[acc.CmdId] = acc.CmdSlot
			r.getCmdDesc(acc.CmdSlot, acc, -1)

		case m := <-r.cs.acceptAckChan:
			ack := m.(*MAcceptAck)
			r.getCmdDesc(ack.CmdSlot, ack, -1)

		case m := <-r.cs.aacksChan:
			aacks := m.(*MAAcks)
			for _, a := range aacks.Accepts {
				ta := a
				if r.values.Has(a.CmdId.String()) {
					continue
				}
				r.slots[a.CmdId] = a.CmdSlot
				r.getCmdDesc(a.CmdSlot, &ta, -1)
			}
			for _, b := range aacks.Acks {
				tb := b
				r.getCmdDesc(b.CmdSlot, &tb, -1)
			}

		case m := <-r.cs.commitChan:
			commit := m.(*MCommit)
			r.getCmdDesc(commit.CmdSlot, commit, -1)

		case m := <-r.cs.syncChan:
			sync := m.(*MSync)
			val, exists := r.values.Get(sync.CmdId.String())
			if exists {
				rep := &MSyncReply{
					Replica: r.Id,
					Ballot:  r.ballot,
					CmdId:   sync.CmdId,
					Rep:     val.([]byte),
				}
				r.sender.SendToClient(sync.CmdId.ClientId, rep, r.cs.syncReplyRPC)
			}
		}
	}
}

func (r *Replica) handlePropose(msg *defs.GPropose, desc *commandDesc, slot int, dep int) {
	if r.status != NORMAL || desc.propose != nil {
		return
	}

	desc.propose = msg
	desc.cmd = msg.Command
	desc.cmdId = CommandId{
		ClientId: msg.ClientId,
		SeqNum:   msg.CommandId,
	}
	desc.cmdSlot = slot
	desc.dep = dep
	if dep != -1 {
		depDesc := r.getCmdDesc(dep, nil, -1)
		if depDesc != nil {
			depDesc.successorL.Lock()
			depDesc.successor = slot
			depDesc.successorL.Unlock()
		}
	}

	acc := &MAccept{
		Replica: r.Id,
		Ballot:  r.ballot,
		CmdId:   desc.cmdId,
		CmdSlot: slot,
	}

	r.deliver(desc, slot)
	r.batcher.SendAccept(acc)
	r.handleAccept(acc, desc)
}

func (r *Replica) handleAccept(msg *MAccept, desc *commandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	slotStr := strconv.Itoa(msg.CmdSlot)
	if r.delivered.Has(slotStr) {
		return
	}

	desc.cmdId = msg.CmdId
	desc.cmdSlot = msg.CmdSlot

	desc.afterPayload.Call(func() {

		if desc.accepted {
			return
		}

		desc.accepted = true

		if desc.phase == START {
			desc.phase = ACCEPT
			desc.Call()
		}

		if r.contactClients && !r.isLeader {
			prop, exists := r.proposes.Get(desc.cmdId.String())
			if exists { // or if desc.propose != nil ?
				r.IfPreviousAreReady(desc, func() {
					propose := prop.(*defs.GPropose)
					recAck := &MRecordAck{
						Replica: r.Id,
						Ballot:  r.ballot,
						CmdId:   desc.cmdId,
						Ok:      ORDERED,
					}
					r.sender.SendToClient(propose.ClientId, recAck, r.cs.recordAckRPC)
				})
			}
		}

		ack := &MAcceptAck{
			Replica: r.Id,
			Ballot:  msg.Ballot,
			CmdSlot: msg.CmdSlot,
		}

		if r.optimized {
			r.batcher.SendAcceptAck(ack)
			r.handleAcceptAck(ack, desc)
		} else {
			if r.isLeader {
				r.handleAcceptAck(ack, desc)
			} else {
				r.sender.SendTo(msg.Replica, ack, r.cs.acceptAckRPC)
			}
		}
	})
}

func (r *Replica) handleAcceptAck(msg *MAcceptAck, desc *commandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	desc.acks.Add(msg.Replica, false, msg)
}

func getAcksHandler(r *Replica, desc *commandDesc) replica.MsgSetHandler {
	return func(_ interface{}, _ []interface{}) {
		commit := &MCommit{
			Replica: r.Id,
			Ballot:  r.ballot,
			CmdSlot: desc.cmdSlot,
		}
		if r.optimized {
			r.handleCommit(commit, desc)
		} else if r.isLeader {
			r.sender.SendToAll(commit, r.cs.commitRPC)
			r.handleCommit(commit, desc)
		}
	}
}

func (r *Replica) handleCommit(msg *MCommit, desc *commandDesc) {
	slotStr := strconv.Itoa(msg.CmdSlot)
	if r.delivered.Has(slotStr) {
		return
	}

	desc.afterPayload.Call(func() {
		if r.status != NORMAL || r.ballot != msg.Ballot || desc.phase == COMMIT {
			return
		}

		desc.phase = COMMIT
		if r.isLeader {
			r.committed.Set(strconv.Itoa(desc.cmdSlot), struct{}{})
		}

		defer func() {
			desc.successorL.Lock()
			succ := desc.successor
			desc.successorL.Unlock()
			if succ != -1 {
				go func() {
					r.deliverChan <- succ
				}()
			}
		}()
		r.deliver(desc, desc.cmdSlot)
	})
}

func (r *Replica) sync(cmdId CommandId, cmd state.Command) {
	if r.isLeader {
		return
	}
	key := strconv.FormatInt(int64(cmd.K), 10)
	r.unsynced.Upsert(key, nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			if exists {
				if r.synced.Has(cmdId.String()) {
					return mapV
				}
				r.synced.Set(cmdId.String(), struct{}{})
				v := mapV.(int) - 1
				if v < 0 {
					v = 0
				}
				return v
			}
			r.synced.Set(cmdId.String(), struct{}{})
			return 0
		})
}

func (r *Replica) unsync(cmd state.Command) {
	key := strconv.FormatInt(int64(cmd.K), 10)
	r.unsynced.Upsert(key, nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			if exists {
				return mapV.(int) + 1
			}
			return 1
		})
}

func (r *Replica) leaderUnsync(cmd state.Command, slot int) int {
	depSlot := -1
	key := strconv.FormatInt(int64(cmd.K), 10)
	r.unsynced.Upsert(key, nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			if exists {
				if mapV.(int) > slot {
					r.Fatal(mapV.(int), slot)
					return mapV
				}
				depSlot = mapV.(int)
			}
			return slot
		})
	return depSlot
}

func (r *Replica) ok(cmd state.Command) uint8 {
	key := strconv.FormatInt(int64(cmd.K), 10)
	v, exists := r.unsynced.Get(key)
	if exists && v.(int) > 0 {
		return FALSE
	}
	return TRUE
}

func (r *Replica) deliver(desc *commandDesc, slot int) {
	desc.afterPayload.Call(func() {
		slotStr := strconv.Itoa(slot)
		if r.delivered.Has(slotStr) || !r.Exec {
			return
		}

		if desc.phase != COMMIT && !r.isLeader {
			return
		}

		if slot > 0 && !r.executed.Has(strconv.Itoa(slot-1)) {
			return
		}

		p, exists := r.proposes.Get(desc.cmdId.String())
		if exists {
			desc.propose = p.(*defs.GPropose)
		}
		if desc.propose == nil {
			return
		}

		if !r.isLeader {
			desc.cmd = desc.propose.Command
			r.sync(desc.cmdId, desc.cmd)
		}

		if desc.val == nil {
			desc.val = desc.cmd.Execute(r.State)
			r.executed.Set(slotStr, struct{}{})
			go func(nextSlot int) {
				r.deliverChan <- nextSlot
			}(slot + 1)
		}

		if r.isLeader && desc.phase != COMMIT {
			rep := &MReply{
				Replica: r.Id,
				Ballot:  r.ballot,
				CmdId:   desc.cmdId,
				Rep:     desc.val,
			}
			if desc.dep != -1 && !r.committed.Has(strconv.Itoa(desc.dep)) {
				rep.Ok = FALSE
			} else {
				rep.Ok = TRUE
			}
			if rep.Ok == TRUE || r.contactClients {
				// if !r.contactClients then the client gets reply
				// from the leader or the closes replica after the command
				// gets committed and executed
				r.sender.SendToClient(desc.propose.ClientId, rep, r.cs.replyRPC)
			}
		}

		if desc.phase == COMMIT {
			if !r.contactClients {
				if (r.optimized && desc.propose.Proxy) ||
					(!r.optimized && r.isLeader) {
					rep := &MSyncReply{
						Replica: r.Id,
						Ballot:  r.ballot,
						CmdId:   desc.cmdId,
						Rep:     desc.val,
					}
					r.sender.SendToClient(desc.propose.ClientId, rep, r.cs.syncReplyRPC)
				}
			}
			desc.msgs <- slot
			r.delivered.Set(strconv.Itoa(slot), struct{}{})
			if desc.seq {
				for {
					switch hSlot := (<-desc.msgs).(type) {
					case int:
						r.handleMsg(hSlot, desc, slot, desc.dep)
						return
					}
				}
			}
		}
	})
}

func (r *Replica) getCmdDesc(slot int, msg interface{}, dep int) *commandDesc {
	return r.getCmdDescSeq(slot, msg, dep, false)
}

func (r *Replica) getCmdDescSeq(slot int, msg interface{}, dep int, seq bool) *commandDesc {
	slotStr := strconv.Itoa(slot)
	if r.delivered.Has(slotStr) {
		return nil
	}

	var desc *commandDesc

	r.cmdDescs.Upsert(slotStr, nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			if exists {
				desc = mapV.(*commandDesc)
				return desc
			}

			desc = r.newDesc()
			desc.seq = seq || desc.seq
			desc.cmdSlot = slot
			if !desc.seq {
				go r.handleDesc(desc, slot, dep)
				r.routineCount++
			}

			return desc
		})

	if msg != nil {
		if desc.seq {
			r.handleMsg(msg, desc, slot, dep)
		} else {
			desc.msgs <- msg
		}
	}

	return desc
}

func (r *Replica) newDesc() *commandDesc {
	desc := r.allocDesc()
	desc.cmdSlot = -1
	if desc.msgs == nil {
		desc.msgs = make(chan interface{}, 8)
	}
	desc.active = true
	desc.phase = START
	desc.seq = (r.routineCount >= MaxDescRoutines)
	desc.propose = nil
	desc.val = nil
	desc.cmdId.SeqNum = -42
	desc.dep = -1
	desc.successor = -1
	desc.successorL = sync.Mutex{}
	desc.accepted = false

	desc.afterPayload = desc.afterPayload.ReinitCondF(func() bool {
		return (desc.propose != nil || r.proposes.Has(desc.cmdId.String()))
	})

	desc.acks = desc.acks.ReinitMsgSet(r.Q, func(_, _ interface{}) bool {
		return true
	}, func(interface{}) {}, getAcksHandler(r, desc))

	return desc
}

func (desc *commandDesc) Call() {
	if desc == nil || desc.pendingCall == nil {
		return
	}
	desc.pendingCall()
	desc.pendingCall = nil
}

func (r *Replica) IfPreviousAreReady(desc *commandDesc, f func()) {
	var pdesc *commandDesc
	if s := desc.cmdSlot - 1; s < 0 {
		pdesc = nil
	} else {
		pdesc = r.getCmdDesc(s, nil, -1)
	}
	if pdesc == nil || pdesc.phase != START {
		f()
	} else {
		pdesc.pendingCall = f
	}
}

func (r *Replica) allocDesc() *commandDesc {
	if r.poolLevel > 0 {
		desc := r.descPool.Get().(*commandDesc)
		slotStr := strconv.Itoa(desc.cmdSlot)
		if r.delivered.Has(slotStr) && r.values.Has(desc.cmdId.String()) {
			return desc
		}
	}
	return &commandDesc{}
}

func (r *Replica) freeDesc(desc *commandDesc) {
	if r.poolLevel > 0 {
		r.descPool.Put(desc)
	}
}

func (r *Replica) handleDesc(desc *commandDesc, slot int, dep int) {
	for desc.active {
		if r.handleMsg(<-desc.msgs, desc, slot, dep) {
			r.routineCount--
			return
		}
	}
}

func (r *Replica) handleMsg(m interface{}, desc *commandDesc, slot int, dep int) bool {
	switch msg := m.(type) {

	case *defs.GPropose:
		r.handlePropose(msg, desc, slot, dep)

	case *MAccept:
		if msg.CmdSlot == slot {
			r.handleAccept(msg, desc)
		}

	case *MAcceptAck:
		if msg.CmdSlot == slot {
			r.handleAcceptAck(msg, desc)
		}

	case *MCommit:
		if msg.CmdSlot == slot {
			r.handleCommit(msg, desc)
		}

	case string:
		if msg == "deliver" {
			r.deliver(desc, slot)
		}

	case int:
		desc.Call()
		r.history[msg].cmdSlot = slot
		r.history[msg].phase = desc.phase
		r.history[msg].cmd = desc.cmd
		desc.active = false
		slotStr := strconv.Itoa(slot)
		r.values.Set(desc.cmdId.String(), desc.val)
		r.cmdDescs.Remove(slotStr)
		r.freeDesc(desc)
		return true
	}

	return false
}
