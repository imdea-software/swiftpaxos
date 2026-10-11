// Package janus implements the Janus protocol (Mu et al., OSDI'16) for
// one-shot transactions over a sharded, replicated store.
//
// Each shard is replicated by a group of replicas (see Layout). A client sends
// a transaction (a list of single-key operations) to a replica, which hosts the
// coordinator of the transaction (the paper's coordinator, co-located with the
// closest server). The coordinator follows the failure-free path of the paper
// (Algorithms 1-5):
//
//   - PreAccept: sent to every replica of every shard the transaction touches.
//     Each replica records the transaction and answers with the conflicting
//     transactions it has already seen (dependencies).
//   - Fast path: if every replica of every shard (the fast quorum of Janus)
//     answers with identical dependencies, the coordinator commits with the
//     union of the dependencies.
//   - Slow path: otherwise, once a majority of each shard answered, the union
//     is sent with Accept (ballot 0) to every replica of every shard, and
//     committed once a majority of each shard accepted.
//   - Execution: a replica executes its part of a transaction once the
//     transaction and all its ancestors are committed. Strongly connected
//     components of the dependency graph are executed in a deterministic
//     order (transaction id). The dependencies of an ancestor that does not
//     touch the replica's shard are obtained with an Inquire to the nearest
//     replica that the ancestor involves, answered once it committed there.
//     Every replica returns its results to the coordinator, which answers the
//     client with the first results of each shard.
//
// Differences with the original EPaxos code base (epaxos/): dependencies are
// explicit sets of transactions (a replica never sees all the instances of
// other coordinators), quorums are computed per shard, and a transaction may
// span several shards.
//
// Limitations: recovery of a failed coordinator is not implemented (see
// README). Replicas keep the dependency graph of past transactions in memory.
package janus

import (
	"encoding/binary"
	"sort"
	"time"

	"github.com/imdea-software/swiftpaxos/config"
	"github.com/imdea-software/swiftpaxos/dlog"
	"github.com/imdea-software/swiftpaxos/replica"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
	"github.com/imdea-software/swiftpaxos/state"
)

const (
	NONE int8 = iota
	PREACCEPTED
	ACCEPTED
	COMMITTED
)

const (
	tickPeriod = 50 * time.Millisecond
	// a coordinator that has not heard from every replica of the shards of a
	// transaction after this delay gives up the fast path and settles for a
	// majority of each shard (slow path)
	fastTimeout = 2 * time.Second
)

// RPC codes of the client messages; the client registers them in the same order.
const (
	TxnProposeCode = defs.RPC_TABLE
	TxnReplyCode   = defs.RPC_TABLE + 1
)

// Test hooks.
var (
	// called (from the replica event loop) for every executed transaction
	execHook func(rep int32, id TxnId, cmds []state.Command, vals [][]byte)
	// measure latencies to peers at startup, to pick the closest quorums
	closestPeers = true
	// called when a replica computes the dependencies of a PreAccept
	paHook func(rep int32, id TxnId, deps []Dep)
)

// Node is what a replica knows about a transaction.
type Node struct {
	Id       TxnId
	Shards   uint64
	Cmds     []state.Command
	Status   int8
	Deps     []Dep
	bal      int32
	vbal     int32
	local    bool // touches the shard of this replica
	recorded bool // registered in the per-key conflict structures
	done     bool // executed here (local) or ordered by an SCC computation (non-local)
	inquired bool // an Inquire was sent for this (non-local) transaction

	// tarjan
	gen, index, low int
	onStack         bool
}

// fent is an access to a key that later conflicting transactions must depend on.
type fent struct {
	dep   Dep
	write bool
}

// keyDeps is the frontier of a key: accesses not yet covered by a committed
// write. A write covers the accesses that appear in its final dependencies, so
// a later transaction conflicting with a covered access reaches it through the
// write (this pruning is not part of the paper, which keeps every conflicting
// transaction as a direct dependency).
type keyDeps struct {
	ents []fent
}

type scanRec struct {
	dep    Dep
	lo, hi state.Key
}

type shardState struct {
	replies map[int32][]Dep
	accepts map[int32]bool
}

type coordState struct {
	propose  *TxnPropose
	shards   []int
	sh       map[int]*shardState
	phase    int // 0: preaccept, 1: accept, 2: committed
	start    time.Time
	timedOut bool // the fast path was given up (see fastTimeout)
	results  map[int][][]byte
	final    []Dep
}

type Replica struct {
	*replica.Replica

	lay     Layout
	shard   int
	members []int32
	maj     int // majority of a shard (slow quorum)

	proposeChan, preAcceptChan, preAcceptReplyChan chan fastrpc.Serializable
	acceptChan, acceptReplyChan, commitChan        chan fastrpc.Serializable
	inquireChan, execResultChan, dummyChan         chan fastrpc.Serializable
	preAcceptRPC, preAcceptReplyRPC, acceptRPC     uint8
	acceptReplyRPC, commitRPC, inquireRPC          uint8
	execResultRPC                                  uint8

	nodes   map[TxnId]*Node
	keys    map[state.Key]*keyDeps
	scans   []scanRec
	pending map[TxnId]*Node
	coords  map[TxnId]*coordState
	next    int32
	// replicas waiting for the commit of a transaction (Inquire)
	inquirers map[TxnId][]int32

	gen, idx int
	stack    []*Node
	dirty    bool

	fastPaths, slowPaths int
}

// New creates a Janus replica. shardSize is the number of replicas per shard
// (0 means that every replica replicates everything).
func New(alias string, id int, peerAddrList []string, shardSize int, conf *config.Config, logger *dlog.Logger) *Replica {
	lay := NewLayout(len(peerAddrList), shardSize)
	if lay.N%lay.ShardSize != 0 {
		logger.Fatal("janus: ", lay.N, " replicas cannot be split in shards of ", lay.ShardSize)
	}
	if lay.NumShards() > 64 {
		logger.Fatal("janus: at most 64 shards are supported")
	}
	mk := func() chan fastrpc.Serializable {
		return make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	}
	r := &Replica{
		Replica: replica.New(alias, id, lay.F(), peerAddrList, true, true, false, conf, logger),

		lay:     lay,
		shard:   lay.ShardOfReplica(int32(id)),
		maj:     lay.Majority(),
		members: lay.Replicas(lay.ShardOfReplica(int32(id))),

		proposeChan: mk(), preAcceptChan: mk(), preAcceptReplyChan: mk(),
		acceptChan: mk(), acceptReplyChan: mk(), commitChan: mk(),
		inquireChan: mk(), execResultChan: mk(), dummyChan: mk(),

		nodes:   make(map[TxnId]*Node),
		keys:    make(map[state.Key]*keyDeps),
		pending: make(map[TxnId]*Node),
		coords:  make(map[TxnId]*coordState),

		inquirers: make(map[TxnId][]int32),
	}
	// the first two codes are shared with clients
	if c := r.RPC.Register(new(TxnPropose), r.proposeChan); c != TxnProposeCode {
		panic("janus: unexpected rpc code")
	}
	if c := r.RPC.Register(new(TxnReply), r.dummyChan); c != TxnReplyCode {
		panic("janus: unexpected rpc code")
	}
	r.preAcceptRPC = r.RPC.Register(new(PreAccept), r.preAcceptChan)
	r.preAcceptReplyRPC = r.RPC.Register(new(PreAcceptReply), r.preAcceptReplyChan)
	r.acceptRPC = r.RPC.Register(new(Accept), r.acceptChan)
	r.acceptReplyRPC = r.RPC.Register(new(AcceptReply), r.acceptReplyChan)
	r.commitRPC = r.RPC.Register(new(Commit), r.commitChan)
	r.inquireRPC = r.RPC.Register(new(Inquire), r.inquireChan)
	r.execResultRPC = r.RPC.Register(new(ExecResult), r.execResultChan)

	go r.run()
	return r
}

func (r *Replica) run() {
	r.ConnectToPeers()
	if closestPeers {
		r.ComputeClosestPeers()
	}
	go r.WaitForClientConnections()

	tick := time.NewTicker(tickPeriod)
	for !r.Shutdown {
		select {
		case m := <-r.proposeChan:
			r.handlePropose(m.(*TxnPropose))
		case m := <-r.preAcceptChan:
			r.handlePreAccept(m.(*PreAccept))
		case m := <-r.preAcceptReplyChan:
			r.handlePreAcceptReply(m.(*PreAcceptReply))
		case m := <-r.acceptChan:
			r.handleAccept(m.(*Accept))
		case m := <-r.acceptReplyChan:
			r.handleAcceptReply(m.(*AcceptReply))
		case m := <-r.commitChan:
			r.handleCommit(m.(*Commit))
		case m := <-r.inquireChan:
			r.handleInquire(m.(*Inquire))
		case m := <-r.execResultChan:
			r.handleExecResult(m.(*ExecResult))
		case <-tick.C:
			r.onTick()
		}
		if r.dirty {
			r.dirty = false
			r.executePending()
		}
	}
}

// ---- helpers ----

func (r *Replica) node(id TxnId, shards uint64) *Node {
	n, ok := r.nodes[id]
	if !ok {
		n = &Node{Id: id, Shards: shards}
		r.nodes[id] = n
	}
	if n.Shards == 0 {
		n.Shards = shards
	}
	return n
}

func (r *Replica) inShard(c *state.Command, s int) bool {
	return c.Op == state.SCAN || r.lay.ShardOfKey(c.K) == s
}

func (r *Replica) localCmds(cmds []state.Command) []state.Command {
	res := make([]state.Command, 0, len(cmds))
	for i := range cmds {
		if r.inShard(&cmds[i], r.shard) {
			res = append(res, cmds[i])
		}
	}
	return res
}

func shardMask(ss []int) uint64 {
	var m uint64
	for _, s := range ss {
		m |= 1 << uint(s)
	}
	return m
}

func (r *Replica) touches(shards uint64) bool {
	return shards&(1<<uint(r.shard)) != 0
}

// nearest returns the closest other replica of one of the given shards, or -1.
func (r *Replica) nearest(shards uint64) int32 {
	for _, p := range r.PreferredPeerOrder {
		if p != r.Id && shards&(1<<uint(r.lay.ShardOfReplica(p))) != 0 {
			return p
		}
	}
	return -1
}

// sameReplies tells whether all the replies of a shard carry identical dependencies.
func sameReplies(replies map[int32][]Dep) bool {
	var first []Dep
	i := 0
	for _, ds := range replies {
		if i > 0 && !equalDeps(first, ds) {
			return false
		}
		first = ds
		i++
	}
	return true
}

func sortDeps(ds []Dep) {
	sort.Slice(ds, func(i, j int) bool { return ds[i].Id.less(ds[j].Id) })
}

func equalDeps(a, b []Dep) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Id != b[i].Id {
			return false
		}
	}
	return true
}

func unionDeps(sets ...[]Dep) []Dep {
	seen := make(map[TxnId]uint64)
	for _, s := range sets {
		for _, d := range s {
			if d.Shards != 0 || seen[d.Id] == 0 {
				seen[d.Id] = d.Shards
			}
		}
	}
	res := make([]Dep, 0, len(seen))
	for id, sh := range seen {
		res = append(res, Dep{id, sh})
	}
	sortDeps(res)
	return res
}

func scanRange(c *state.Command) (state.Key, state.Key) {
	return c.K, c.K + state.Key(binary.LittleEndian.Uint64(c.V))
}

// ---- conflicts ----

// computeDeps returns the transactions already recorded here that conflict
// with the given local operations.
func (r *Replica) computeDeps(id TxnId, local []state.Command) []Dep {
	set := make(map[TxnId]struct{})
	add := func(d *Dep) {
		if d.Id != id {
			set[d.Id] = struct{}{}
		}
	}
	for i := range local {
		c := &local[i]
		switch c.Op {
		case state.PUT:
			if kd := r.keys[c.K]; kd != nil {
				for j := range kd.ents {
					add(&kd.ents[j].dep)
				}
			}
			for j := range r.scans {
				if c.K >= r.scans[j].lo && c.K <= r.scans[j].hi {
					add(&r.scans[j].dep)
				}
			}
		case state.GET:
			if kd := r.keys[c.K]; kd != nil {
				for j := range kd.ents {
					if kd.ents[j].write {
						add(&kd.ents[j].dep)
					}
				}
			}
		case state.SCAN:
			lo, hi := scanRange(c)
			for k, kd := range r.keys {
				if k < lo || k > hi {
					continue
				}
				for j := range kd.ents {
					if kd.ents[j].write {
						add(&kd.ents[j].dep)
					}
				}
			}
		}
	}
	deps := make([]Dep, 0, len(set))
	for d := range set {
		deps = append(deps, Dep{d, r.nodes[d].Shards})
	}
	sortDeps(deps)
	return deps
}

// record registers the local operations of a transaction in the frontier of
// their keys.
func (r *Replica) record(n *Node) {
	if n.recorded {
		return
	}
	n.recorded = true
	self := Dep{n.Id, n.Shards}
	for _, c := range r.localCmds(n.Cmds) {
		c := c
		if c.Op == state.SCAN {
			lo, hi := scanRange(&c)
			r.scans = append(r.scans, scanRec{self, lo, hi})
			continue
		}
		kd := r.keys[c.K]
		if kd == nil {
			kd = &keyDeps{}
			r.keys[c.K] = kd
		}
		kd.ents = append(kd.ents, fent{self, c.Op == state.PUT})
	}
}

// cover removes from the frontiers of the keys written by the committed
// transaction n the accesses that appear in its (final) dependencies. A later
// transaction depending on n thus reaches them through n. To preserve this, n
// covers a key only if it is still in its frontier: two transactions of a
// cycle cannot cover each other.
func (r *Replica) cover(n *Node) {
	deps := make(map[TxnId]struct{}, len(n.Deps))
	for _, d := range n.Deps {
		deps[d.Id] = struct{}{}
	}
	for _, c := range r.localCmds(n.Cmds) {
		if c.Op != state.PUT {
			continue
		}
		kd := r.keys[c.K]
		present := false
		for _, e := range kd.ents {
			if e.dep.Id == n.Id {
				present = true
				break
			}
		}
		if !present {
			continue
		}
		keep := kd.ents[:0]
		for _, e := range kd.ents {
			if _, covered := deps[e.dep.Id]; !covered || e.dep.Id == n.Id {
				keep = append(keep, e)
			}
		}
		kd.ents = keep
	}
}

// ---- coordinator ----

func (r *Replica) handlePropose(m *TxnPropose) {
	id := TxnId{r.Id, r.next}
	r.next++
	ss := r.lay.ShardsOf(m.Cmds)
	if len(ss) == 0 {
		r.SendClientMsg(m.ClientId, TxnReplyCode, &TxnReply{CommandId: m.CommandId})
		return
	}
	cs := &coordState{
		propose: m,
		shards:  ss,
		sh:      make(map[int]*shardState),
		start:   time.Now(),
		results: make(map[int][][]byte),
	}
	r.coords[id] = cs
	mask := shardMask(ss)
	for _, s := range ss {
		cs.sh[s] = &shardState{
			replies: make(map[int32][]Dep),
			accepts: make(map[int32]bool),
		}
	}
	// Algorithm 1, line 3: PreAccept to all the participating servers
	for _, s := range ss {
		for _, p := range r.lay.Replicas(s) {
			r.sendPreAccept(id, cs, mask, p)
		}
	}
	r.checkPreAccepted(id, cs)
}

func (r *Replica) sendPreAccept(id TxnId, cs *coordState, mask uint64, p int32) {
	if p == r.Id {
		deps, _ := r.preAcceptLocal(id, mask, cs.propose.Cmds)
		cs.sh[r.shard].replies[r.Id] = deps
		return
	}
	r.SendMsg(p, r.preAcceptRPC, &PreAccept{Id: id, Shards: mask, Cmds: cs.propose.Cmds})
}

// checkPreAccepted implements Algorithm 1, lines 4-11. The fast quorum of a
// shard is all its replicas: the fast path is taken when every replica of every
// shard returned identical dependencies. Once this is excluded (some shard
// returned different dependencies, or fastTimeout expired), the coordinator
// goes to the Accept phase as soon as a majority of each shard answered.
func (r *Replica) checkPreAccepted(id TxnId, cs *coordState) {
	if cs.phase != 0 {
		return
	}
	fast := !cs.timedOut
	for _, s := range cs.shards {
		sh := cs.sh[s]
		if len(sh.replies) < r.maj {
			return
		}
		if !sameReplies(sh.replies) {
			fast = false
		}
	}
	if fast {
		for _, s := range cs.shards {
			if len(cs.sh[s].replies) < r.lay.FastQuorum() {
				return // the fast path is still possible
			}
		}
	}
	var sets [][]Dep
	for _, s := range cs.shards {
		for _, ds := range cs.sh[s].replies {
			sets = append(sets, ds)
		}
	}
	cs.final = unionDeps(sets...)
	if fast {
		r.fastPaths++
		r.commit(id, cs)
		return
	}
	// Algorithm 1, lines 14-16: Accept to all the participating servers
	r.slowPaths++
	cs.phase = 1
	mask := shardMask(cs.shards)
	for _, s := range cs.shards {
		sh := cs.sh[s]
		for _, p := range r.lay.Replicas(s) {
			if p == r.Id {
				if r.acceptLocal(id, 0, mask, cs.propose.Cmds, cs.final) {
					sh.accepts[p] = true
				}
				continue
			}
			r.SendMsg(p, r.acceptRPC, &Accept{Id: id, Ballot: 0, Shards: mask, Cmds: cs.propose.Cmds, Deps: cs.final})
		}
	}
	r.checkAccepted(id, cs)
}

// checkAccepted implements Algorithm 1, lines 17-18.
func (r *Replica) checkAccepted(id TxnId, cs *coordState) {
	if cs.phase != 1 {
		return
	}
	for _, s := range cs.shards {
		if len(cs.sh[s].accepts) < r.maj {
			return
		}
	}
	r.commit(id, cs)
}

func (r *Replica) commit(id TxnId, cs *coordState) {
	cs.phase = 2
	mask := shardMask(cs.shards)
	cm := &Commit{Id: id, Shards: mask, Cmds: cs.propose.Cmds, Deps: cs.final}
	local := false
	for _, s := range cs.shards {
		for _, p := range r.lay.Replicas(s) {
			if p == r.Id {
				local = true
				continue
			}
			r.SendMsg(p, r.commitRPC, cm)
		}
	}
	if local {
		r.handleCommit(cm)
	}
	if (r.fastPaths+r.slowPaths)%1000 == 0 {
		r.Printf("janus: %d fast, %d slow paths", r.fastPaths, r.slowPaths)
	}
}

func (r *Replica) handlePreAcceptReply(m *PreAcceptReply) {
	cs := r.coords[m.Id]
	if cs == nil || cs.phase != 0 || m.OK == 0 {
		return
	}
	if sh := cs.sh[r.lay.ShardOfReplica(m.Replica)]; sh != nil {
		sh.replies[m.Replica] = m.Deps
		r.checkPreAccepted(m.Id, cs)
	}
}

func (r *Replica) handleAcceptReply(m *AcceptReply) {
	cs := r.coords[m.Id]
	if cs == nil || cs.phase != 1 || m.OK == 0 {
		return
	}
	if sh := cs.sh[r.lay.ShardOfReplica(m.Replica)]; sh != nil {
		sh.accepts[m.Replica] = true
		r.checkAccepted(m.Id, cs)
	}
}

func (r *Replica) handleExecResult(m *ExecResult) {
	cs := r.coords[m.Id]
	if cs == nil {
		return
	}
	// every replica returns its results (Algorithm 4, line 57): use the
	// first ones received for each shard
	if _, ok := cs.results[int(m.Shard)]; ok {
		return
	}
	cs.results[int(m.Shard)] = m.Vals
	if len(cs.results) < len(cs.shards) {
		return
	}
	cmds := cs.propose.Cmds
	vals := make([][]byte, len(cmds))
	for i := range cmds {
		if cmds[i].Op == state.SCAN {
			for _, s := range cs.shards { // ascending shard order
				if v := cs.results[s]; i < len(v) {
					vals[i] = append(vals[i], v[i]...)
				}
			}
		} else if v := cs.results[r.lay.ShardOfKey(cmds[i].K)]; i < len(v) {
			vals[i] = v[i]
		}
	}
	delete(r.coords, m.Id)
	r.SendClientMsg(cs.propose.ClientId, TxnReplyCode, &TxnReply{CommandId: cs.propose.CommandId, Vals: vals})
}

// ---- replica side ----

func (r *Replica) preAcceptLocal(id TxnId, mask uint64, cmds []state.Command) ([]Dep, bool) {
	n := r.node(id, mask)
	n.Shards = mask
	if n.Status >= PREACCEPTED {
		return n.Deps, true
	}
	n.Cmds = cmds
	n.local = true
	n.Deps = r.computeDeps(id, r.localCmds(cmds))
	n.Status = PREACCEPTED
	if paHook != nil {
		paHook(r.Id, id, n.Deps)
	}
	r.record(n)
	r.registerDeps(n.Deps)
	return n.Deps, true
}

func (r *Replica) registerDeps(ds []Dep) {
	for _, d := range ds {
		r.node(d.Id, d.Shards)
	}
}

func (r *Replica) handlePreAccept(m *PreAccept) {
	deps, _ := r.preAcceptLocal(m.Id, m.Shards, m.Cmds)
	r.SendMsg(m.Id.R, r.preAcceptReplyRPC, &PreAcceptReply{Id: m.Id, Replica: r.Id, OK: 1, Deps: deps})
}

func (r *Replica) acceptLocal(id TxnId, ballot int32, mask uint64, cmds []state.Command, deps []Dep) bool {
	n := r.node(id, mask)
	n.Shards = mask
	if ballot < n.bal || n.Status == COMMITTED {
		return n.Status == COMMITTED
	}
	n.bal, n.vbal = ballot, ballot
	n.Cmds = cmds
	n.local = true
	n.Deps = deps
	n.Status = ACCEPTED
	r.record(n)
	r.registerDeps(deps)
	return true
}

func (r *Replica) handleAccept(m *Accept) {
	ok := uint8(0)
	if r.acceptLocal(m.Id, m.Ballot, m.Shards, m.Cmds, m.Deps) {
		ok = 1
	}
	r.SendMsg(m.Id.R, r.acceptReplyRPC, &AcceptReply{Id: m.Id, Replica: r.Id, Ballot: m.Ballot, OK: ok})
}

func (r *Replica) handleCommit(m *Commit) {
	n := r.node(m.Id, m.Shards)
	if n.Status == COMMITTED {
		return
	}
	n.Shards = m.Shards
	n.Cmds = m.Cmds
	n.Deps = m.Deps
	n.Status = COMMITTED
	n.local = r.touches(m.Shards)
	r.registerDeps(m.Deps)
	if n.local {
		r.record(n)
		r.cover(n)
		if !n.done {
			r.pending[n.Id] = n
		}
	}
	for _, p := range r.inquirers[n.Id] {
		r.SendMsg(p, r.commitRPC, m)
	}
	delete(r.inquirers, n.Id)
	r.dirty = true
}

// handleInquire answers with the dependencies of a transaction once it is
// committed here.
func (r *Replica) handleInquire(m *Inquire) {
	n := r.nodes[m.Id]
	if n == nil || n.Status != COMMITTED {
		r.inquirers[m.Id] = append(r.inquirers[m.Id], m.From)
		return
	}
	r.SendMsg(m.From, r.commitRPC, &Commit{Id: n.Id, Shards: n.Shards, Cmds: n.Cmds, Deps: n.Deps})
}

// ---- execution ----

func (r *Replica) executePending() {
	ids := make([]TxnId, 0, len(r.pending))
	for id := range r.pending {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].less(ids[j]) })
	for _, id := range ids {
		n := r.pending[id]
		if n == nil || n.done {
			delete(r.pending, id)
			continue
		}
		r.gen++
		r.idx = 0
		r.stack = r.stack[:0]
		r.strongConnect(n)
	}
}

// strongConnect is Tarjan's algorithm over the dependency graph. It returns
// false if some ancestor is not committed yet (in which case it is inquired,
// unless it involves this replica, which then waits for its Commit).
func (r *Replica) strongConnect(v *Node) bool {
	v.gen, v.index, v.low = r.gen, r.idx, r.idx
	r.idx++
	r.stack = append(r.stack, v)
	v.onStack = true

	for _, d := range v.Deps {
		w := r.node(d.Id, d.Shards)
		if w.done {
			continue
		}
		if w.Status != COMMITTED {
			r.inquire(w)
			return false
		}
		if w.gen != r.gen {
			if !r.strongConnect(w) {
				return false
			}
			if w.low < v.low {
				v.low = w.low
			}
		} else if w.onStack && w.index < v.low {
			v.low = w.index
		}
	}

	if v.low == v.index {
		i := len(r.stack) - 1
		for r.stack[i] != v {
			i--
		}
		scc := append([]*Node(nil), r.stack[i:]...)
		r.stack = r.stack[:i]
		sort.Slice(scc, func(a, b int) bool { return scc[a].Id.less(scc[b].Id) })
		for _, n := range scc {
			n.onStack = false
			n.done = true
			delete(r.pending, n.Id)
		}
		for _, n := range scc {
			if n.local {
				r.execute(n)
			}
		}
	}
	return true
}

func (r *Replica) execute(n *Node) {
	vals := make([][]byte, len(n.Cmds))
	for i := range n.Cmds {
		if r.inShard(&n.Cmds[i], r.shard) {
			vals[i] = n.Cmds[i].Execute(r.State)
		}
	}
	if execHook != nil {
		execHook(r.Id, n.Id, n.Cmds, vals)
	}
	res := &ExecResult{Id: n.Id, Shard: int32(r.shard), Vals: vals}
	if n.Id.R == r.Id {
		r.handleExecResult(res)
	} else {
		r.SendMsg(n.Id.R, r.execResultRPC, res)
	}
}

// inquire asks the nearest replica involved in w for its dependencies
// (Algorithm 4, lines 44-45). If w involves this replica, its Commit will
// arrive anyway.
func (r *Replica) inquire(w *Node) {
	if w.inquired || w.Shards == 0 || r.touches(w.Shards) {
		return
	}
	if p := r.nearest(w.Shards); p >= 0 {
		w.inquired = true
		r.SendMsg(p, r.inquireRPC, &Inquire{Id: w.Id, From: r.Id})
	}
}

// ---- timers ----

func (r *Replica) onTick() {
	if len(r.pending) > 0 {
		r.dirty = true
	}
	now := time.Now()
	for id, cs := range r.coords {
		if cs.phase != 0 || cs.timedOut || now.Sub(cs.start) < fastTimeout {
			continue
		}
		// some replicas did not answer: settle for a majority (slow path)
		cs.timedOut = true
		r.checkPreAccepted(id, cs)
	}
}
