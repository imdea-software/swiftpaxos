package swift

import (
	"log"
	"sort"
	"sync"
	"time"

	"github.com/imdea-software/swiftpaxos/replica"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	"github.com/imdea-software/swiftpaxos/state"
	"github.com/orcaman/concurrent-map"
)

func (r *Replica) handleNewLeader(msg *MNewLeader) {
	if r.ballot >= msg.Ballot {
		return
	}
	log.Println("Recovering... with the ballot", msg.Ballot)

	r.status = RECOVERING
	r.ballot = msg.Ballot
	r.recStart = time.Now()

	r.repchan.stop()
	r.stopDescs()

	newLeaderAckN := &MNewLeaderAckN{
		Replica: r.Id,
		Ballot:  r.ballot,
		Cballot: r.cballot,
	}
	r.fillNewLeaderAckN(newLeaderAckN)

	if msg.Replica != r.Id {
		r.sender.SendTo(msg.Replica, newLeaderAckN, r.cs.newLeaderAckNRPC)
	} else {
		r.handleNewLeaderAckN(newLeaderAckN)
	}

	// stop processing normal channels:
	for r.status == RECOVERING {
		select {
		case m := <-r.cs.newLeaderChan:
			newLeader := m.(*MNewLeader)
			r.handleNewLeader(newLeader)

		case m := <-r.cs.newLeaderAckNChan:
			newLeaderAck := m.(*MNewLeaderAckN)
			r.handleNewLeaderAckN(newLeaderAck)

		case m := <-r.cs.syncChan:
			sync := m.(*MSync)
			r.handleSync(sync)
		}
	}
}

func (r *Replica) handleNewLeaderAckN(msg *MNewLeaderAckN) {
	if r.status != RECOVERING || r.ballot != msg.Ballot {
		return
	}

	r.newLeaderAckNs.Add(msg.Replica, false, msg)
}

func (r *Replica) handleNewLeaderAckNs(_ interface{}, msgs []interface{}) {
	maxCbal := int32(-1)
	var U map[*MNewLeaderAckN]struct{}

	for _, msg := range msgs {
		newLeaderAck := msg.(*MNewLeaderAckN)
		if maxCbal < newLeaderAck.Cballot {
			U = make(map[*MNewLeaderAckN]struct{})
			maxCbal = newLeaderAck.Cballot
		}
		if maxCbal == newLeaderAck.Cballot {
			U[newLeaderAck] = struct{}{}
		}
	}

	phases := make(map[CommandId]int)
	cmds := make(map[CommandId]state.Command)
	deps := make(map[CommandId]Dep)

	for newLeaderAckN := range U {
		for i, phase := range newLeaderAckN.Phases {
			if phase == COMMIT || phase == ACCEPT {
				cmdId := newLeaderAckN.CmdIds[i]
				phases[cmdId] = phase
				cmds[cmdId] = newLeaderAckN.Cmds[i]
				deps[cmdId] = Dep(newLeaderAckN.Deps[i].Dep)
			}
		}
	}

	sync := &MSync{
		Replica: r.Id,
		Ballot:  r.ballot,
		Phases:  phases,
		Cmds:    cmds,
		Deps:    deps,
	}
	r.sender.SendToAll(sync, r.cs.syncRPC)
	r.handleSync(sync)
}

func (r *Replica) fillNewLeaderAckN(newLeaderAckN *MNewLeaderAckN) {
	cmdIds := []CommandId{}
	phases := []int{}
	cmds := []state.Command{}
	deps := []SDep{}
	seen := make(map[CommandId]struct{})

	r.cmdDescs.IterCb(func(_ string, v interface{}) {
		desc := v.(*commandDesc)
		if desc.propose != nil {
			cmdId := CommandId{
				ClientId: desc.propose.ClientId,
				SeqNum:   desc.propose.CommandId,
			}
			if _, exists := seen[cmdId]; !exists {
				seen[cmdId] = struct{}{}
				cmdIds = append(cmdIds, cmdId)
				phases = append(phases, desc.phase)
				cmds = append(cmds, desc.cmd)
				deps = append(deps, SDep{desc.dep})
			}
		}
	})

	for cmdId := range r.proposes {
		if _, exists := seen[cmdId]; exists || r.delivered.Has(cmdId.String()) {
			continue
		}
		cmdIds = append(cmdIds, cmdId)
		phases = append(phases, ACCEPT)
		cmds = append(cmds, state.NOOP()[0])
		deps = append(deps, SDep{[]CommandId{}})
	}

	newLeaderAckN.CmdIds = cmdIds
	newLeaderAckN.Phases = phases
	newLeaderAckN.Cmds = cmds
	newLeaderAckN.Deps = deps
}

func (r *Replica) handleSync(msg *MSync) {
	if r.ballot > msg.Ballot || (r.ballot == msg.Ballot && r.status == NORMAL) {
		return
	}

	if r.status == NORMAL {
		r.recStart = time.Now()
		r.repchan.stop()
		r.stopDescs()
	}

	// clear cmdDescs:
	r.cmdDescs.IterCb(func(_ string, v interface{}) {
		desc := v.(*commandDesc)
		if desc.propose != nil {
			cmdId := CommandId{
				ClientId: desc.propose.ClientId,
				SeqNum:   desc.propose.CommandId,
			}
			if _, exists := msg.Phases[cmdId]; !exists {
				go func(propose *defs.GPropose) {
					r.ProposeChan <- propose
				}(desc.propose)
			}
		}
		desc.msgs = nil
		desc.stopChan = nil
		desc.slowPathH.Free()
		desc.fastPathH.Free()
		r.freeDesc(desc)
	})

	r.keys = make(map[state.Key]keyInfo)
	r.routineCount = 0
	r.cmdDescs = cmap.New()
	r.status = NORMAL
	r.ballot = msg.Ballot
	r.cballot = msg.Ballot
	if r.fixedMajority {
		r.FQ = r.qs.AQ(r.ballot)
	}
	r.repchan = NewReplyChan(r)
	r.historySize = 0

	i := 0
	sorted := make([]CommandId, len(msg.Phases))
	for cmdId := range msg.Phases {
		sorted[i] = cmdId
		i++
	}
	sort.Slice(sorted, func(i, j int) bool {
		return msg.Deps[sorted[j]].Contains(sorted[i])
	})

	for _, cmdId := range sorted {
		desc := r.getCmdDesc(cmdId, nil, nil)
		if desc != nil {
			desc.phase = msg.Phases[cmdId]
			desc.cmd = msg.Cmds[cmdId]
			desc.dep = msg.Deps[cmdId]
			desc.proposeDep = msg.Deps[cmdId]

			for _, cmdIdPrime := range msg.Deps[cmdId] {
				descPrime := r.getCmdDesc(cmdIdPrime, nil, nil)
				if descPrime != nil {
					descPrime.successors = append(descPrime.successors, cmdId)
				}
				go func() {
					r.deliverChan <- cmdIdPrime
				}()
			}

			go func() {
				r.deliverChan <- cmdId
			}()

			if desc.phase != COMMIT && desc.phase != ACCEPT {
				desc.phase = ACCEPT
			}
		}

		if propose, exists := r.proposes[cmdId]; exists {
			if desc != nil {
				desc.propose = propose
			}

			if !r.SQ.Contains(r.Id) {
				continue
			}

			// TODO: what if !r.optExec ?
			if r.Id == r.leader() {
				fastAck := newFastAck()
				fastAck.Replica = r.Id
				fastAck.Ballot = r.ballot
				fastAck.CmdId = cmdId
				fastAck.Dep = msg.Deps[cmdId]
				if fastAck.Dep == nil {
					fastAck.Dep = NilDepOfCmdId(cmdId)
				}
				r.batcher.SendFastAck(copyFastAck(fastAck))
				if desc != nil {
					defer r.handleFastAck(fastAck, desc)
				}
				reply := &MReply{
					Replica: r.Id,
					Ballot:  r.ballot,
					CmdId:   cmdId,
				}
				r.sender.SendToClient(propose.ClientId, reply, r.cs.replyRPC)
			} else {
				lightSlowAck := &MLightSlowAck{
					Replica: r.Id,
					Ballot:  r.ballot,
					CmdId:   cmdId,
				}
				r.batcher.SendLightSlowAckClient(lightSlowAck, propose.ClientId)
				if desc != nil {
					defer r.handleLightSlowAck(lightSlowAck, desc)
				}
			}
		}
	}

	for cmdId, propose := range r.proposes {
		if _, exists := msg.Phases[cmdId]; exists {
			continue
		}
		acc := &MAccept{
			Replica: r.Id,
			Ballot:  r.ballot,
			CmdId:   cmdId,
		}
		r.sender.SendToClient(propose.ClientId, acc, r.cs.acceptRPC)
	}

	log.Println("Recovered!")
	log.Println("Ballot:", r.ballot)
	log.Println("FQ:", r.FQ, "SQ:", r.SQ)
	log.Println("recovered in", time.Now().Sub(r.recStart))
}

func (r *Replica) stopDescs() {
	var wg sync.WaitGroup
	r.cmdDescs.IterCb(func(_ string, v interface{}) {
		desc := v.(*commandDesc)
		if desc.active && !desc.seq {
			wg.Add(1)
			desc.stopChan <- &wg
		}
	})
	wg.Wait()

	// TODO: maybe add to history even if stopped this way ?
}

func (r *Replica) reinitNewLeaderAckNs() {
	accept := func(_, _ interface{}) bool {
		return true
	}
	free := func(_ interface{}) {}
	Q := replica.NewMajorityOf(r.N)
	r.newLeaderAckNs = r.newLeaderAckNs.ReinitMsgSet(Q, accept, free, r.handleNewLeaderAckNs)
}
