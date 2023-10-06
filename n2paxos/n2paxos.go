package n2paxos

import (
	"fmt"
	"log"
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

	isLeader    bool
	lastCmdSlot int

	slots     cmap.ConcurrentMap
	proposes  cmap.ConcurrentMap
	cmdDescs  cmap.ConcurrentMap
	delivered cmap.ConcurrentMap

	sender  replica.Sender
	batcher *Batcher
	history []commandStaticDesc

	AQ replica.QuorumI
	qs replica.QuorumSet
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

	twoBs        *replica.MsgSet
	afterPayload *hook.OptCondF

	msgs   chan interface{}
	active bool
	seq    bool
}

type commandStaticDesc struct {
	cmdSlot int
	phase   int
	cmd     state.Command
}

func New(alias string, rid int, addrs []string, exec bool, pl, f int,
	conf *config.Config, logger *dlog.Logger) *Replica {
	cmap.SHARD_COUNT = 32768

	r := &Replica{
		Replica: replica.New(alias, rid, f, addrs, false, exec, false, conf, logger),

		ballot:  0,
		cballot: 0,
		status:  NORMAL,

		isLeader:    false,
		lastCmdSlot: 0,

		slots:     cmap.New(),
		proposes:  cmap.New(),
		cmdDescs:  cmap.New(),
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

	r.sender = replica.NewSender(r.Replica)
	r.batcher = NewBatcher(r, 16)
	r.qs = replica.NewQuorumSet(r.N/2+1, r.N)

	AQs, leaderIds, err := replica.NewQuorumsFromFile(conf.Quorum, r.Replica)
	if err == nil && len(AQs) != 0 {
		r.ballot = leaderIds[0]
		r.cballot = leaderIds[0]
		r.isLeader = (leaderIds[0] == r.Id)
	} else if err == replica.NO_QUORUM_FILE {
		r.isLeader = (r.ballot == r.Id)
	} else {
		log.Fatal(err)
	}
	r.AQ = replica.NewMajorityOf(r.N)

	initCs(&r.cs, r.RPC)

	hook.HookUser1(func() {
		totalNum := 0
		for i := 0; i < HISTORY_SIZE; i++ {
			if r.history[i].phase == 0 {
				continue
			}
			totalNum++
		}

		fmt.Printf("Total number of commands: %d\n", totalNum)
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
			r.getCmdDesc(int, "deliver")

		case propose := <-r.ProposeChan:
			if r.isLeader {
				desc := r.getCmdDesc(r.lastCmdSlot, propose)
				if desc == nil {
					log.Fatal("Got propose for the delivered command:",
						propose.ClientId, propose.CommandId)
				}
				r.lastCmdSlot++
			} else {
				cmdId.ClientId = propose.ClientId
				cmdId.SeqNum = propose.CommandId
				r.proposes.Set(cmdId.String(), propose)
				slot, exists := r.slots.Get(cmdId.String())
				if exists {
					r.getCmdDesc(slot.(int), "deliver")
				}
			}

		case m := <-r.cs.twoAChan:
			twoA := m.(*M2A)
			r.getCmdDesc(twoA.CmdSlot, twoA)

		case m := <-r.cs.twoBChan:
			twoB := m.(*M2B)
			r.getCmdDesc(twoB.CmdSlot, twoB)

		case m := <-r.cs.twosChan:
			m2s := m.(*M2s)
			for _, a := range m2s.TwoAs {
				ta := a
				r.getCmdDesc(a.CmdSlot, &ta)
			}
			for _, b := range m2s.TwoBs {
				tb := b
				r.getCmdDesc(b.CmdSlot, &tb)
			}
		}
	}
}

func (r *Replica) handlePropose(msg *defs.GPropose, desc *commandDesc, slot int) {

	if r.status != NORMAL || desc.propose != nil {
		return
	}

	desc.propose = msg

	twoA := &M2A{
		Replica: r.Id,
		Ballot:  r.ballot,
		Cmd:     msg.Command,
		CmdId: CommandId{
			ClientId: msg.ClientId,
			SeqNum:   msg.CommandId,
		},
		CmdSlot: slot,
	}

	r.batcher.Send2A(twoA)
	r.handle2A(twoA, desc)
}

func (r *Replica) handle2A(msg *M2A, desc *commandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	desc.cmd = msg.Cmd
	desc.cmdId = msg.CmdId
	desc.cmdSlot = msg.CmdSlot

	r.slots.Set(desc.cmdId.String(), desc.cmdSlot)

	if !r.AQ.Contains(r.Id) {
		desc.afterPayload.Recall()
		return
	}

	twoB := &M2B{
		Replica: r.Id,
		Ballot:  msg.Ballot,
		CmdSlot: msg.CmdSlot,
	}

	r.batcher.Send2B(twoB)
	r.handle2B(twoB, desc)
}

func (r *Replica) handle2B(msg *M2B, desc *commandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	desc.twoBs.Add(msg.Replica, false, msg)
}

func get2BsHandler(r *Replica, desc *commandDesc) replica.MsgSetHandler {
	return func(leaderMsg interface{}, msgs []interface{}) {
		desc.phase = COMMIT
		r.deliver(desc, desc.cmdSlot)
	}
}

func (r *Replica) deliver(desc *commandDesc, slot int) {
	desc.afterPayload.Call(func() {

		if r.delivered.Has(strconv.Itoa(slot)) || !r.Exec {
			return
		}

		if desc.phase != COMMIT && !r.isLeader {
			return
		}

		if slot > 0 && !r.delivered.Has(strconv.Itoa(slot-1)) {
			return
		}

		p, exists := r.proposes.Get(desc.cmdId.String())
		if exists {
			desc.propose = p.(*defs.GPropose)
		}
		if desc.propose == nil {
			return
		}

		r.delivered.Set(strconv.Itoa(slot), struct{}{})
		dlog.Printf("Executing " + desc.cmd.String())
		v := desc.cmd.Execute(r.State)
		go func(nextSlot int) {
			r.deliverChan <- nextSlot
		}(slot + 1)
		desc.msgs <- slot

		if desc.propose.Proxy && r.Dreply {
			rep := &defs.ProposeReplyTS{
				OK:        defs.TRUE,
				CommandId: desc.propose.CommandId,
				Value:     v,
				Timestamp: desc.propose.Timestamp,
			}
			r.ReplyProposeTS(rep, desc.propose.Reply, desc.propose.Mutex)
		}

		if desc.seq {
			for {
				switch hSlot := (<-desc.msgs).(type) {
				case int:
					r.handleMsg(hSlot, desc, slot)
					return
				}
			}
		}
	})
}

func (r *Replica) getCmdDesc(slot int, msg interface{}) *commandDesc {
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
			desc.cmdSlot = slot
			if !desc.seq {
				go r.handleDesc(desc, slot)
				r.routineCount++
			}

			return desc
		})

	if msg != nil {
		if desc.seq {
			r.handleMsg(msg, desc, slot)
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
	desc.cmdId.SeqNum = -42

	desc.afterPayload = desc.afterPayload.ReinitCondF(func() bool {
		return desc.cmdId.SeqNum != -42
	})

	desc.twoBs = desc.twoBs.ReinitMsgSet(r.AQ, func(_, _ interface{}) bool {
		return true
	}, func(interface{}) {}, get2BsHandler(r, desc))

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

func (r *Replica) handleDesc(desc *commandDesc, slot int) {
	for desc.active {
		if r.handleMsg(<-desc.msgs, desc, slot) {
			r.routineCount--
			return
		}
	}
}

func (r *Replica) handleMsg(m interface{}, desc *commandDesc, slot int) bool {
	switch msg := m.(type) {

	case *defs.GPropose:
		r.handlePropose(msg, desc, slot)

	case *M2A:
		if msg.CmdSlot == slot {
			r.handle2A(msg, desc)
		}

	case *M2B:
		if msg.CmdSlot == slot {
			r.handle2B(msg, desc)
		}

	case string:
		if msg == "deliver" {
			r.deliver(desc, slot)
		}

	case int:
		r.history[msg].cmdSlot = slot
		r.history[msg].phase = desc.phase
		r.history[msg].cmd = desc.cmd
		desc.active = false
		r.cmdDescs.Remove(strconv.Itoa(slot))
		r.freeDesc(desc)
		return true
	}

	return false
}
