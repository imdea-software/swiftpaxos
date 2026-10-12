package janus

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdea-software/swiftpaxos/client"
	"github.com/imdea-software/swiftpaxos/config"
	"github.com/imdea-software/swiftpaxos/dlog"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	"github.com/imdea-software/swiftpaxos/state"
)

type execRec struct {
	id   TxnId
	cmds []state.Command
	vals [][]byte
}

type paRec struct {
	id   TxnId
	deps []Dep
}

var pa map[int32][]paRec

type obs struct {
	cmds       []state.Command
	vals       [][]byte
	start, end time.Time
	pid        int64 // private key identifying the transaction
}

const privBase = 1 << 20

var portCounter int32 = 20000 + int32(os.Getpid()%2000)*10

func startCluster(t *testing.T, n, shardSize, _ int) ([]*Replica, []string, *sync.Mutex, map[int32][]execRec) {
	closestPeers = false
	var mu sync.Mutex
	traces := make(map[int32][]execRec)
	pa = make(map[int32][]paRec)
	paHook = func(rep int32, id TxnId, deps []Dep) {
		mu.Lock()
		pa[rep] = append(pa[rep], paRec{id, append([]Dep(nil), deps...)})
		mu.Unlock()
	}
	execHook = func(rep int32, id TxnId, cmds []state.Command, vals [][]byte) {
		mu.Lock()
		traces[rep] = append(traces[rep], execRec{id, cmds, vals})
		mu.Unlock()
	}
	basePort := int(atomic.AddInt32(&portCounter, 10))
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}
	reps := make([]*Replica, n)
	t.Cleanup(func() {
		execHook = nil
		for _, r := range reps {
			if r == nil {
				continue
			}
			r.Shutdown = true
			if r.Listener != nil {
				r.Listener.Close()
			}
			for _, p := range r.Peers {
				if p != nil {
					p.Close()
				}
			}
		}
	})
	conf := &config.Config{Proxy: &config.ProxyInfo{}}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reps[i] = New(fmt.Sprint("r", i), i, addrs, shardSize, conf, dlog.New("", false))
		}(i)
	}
	wg.Wait()
	// wait for the full mesh of peer connections (and the client listeners)
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		ready := true
		for i, r := range reps {
			for j := range reps {
				if j != i && r.PeerWriters[j] == nil {
					ready = false
				}
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cluster did not start")
		}
	}
	time.Sleep(500 * time.Millisecond)
	return reps, addrs, &mu, traces
}

type rawClient struct {
	id int32
	w  *bufio.Writer
	r  *bufio.Reader
}

// do runs a transaction (a TXN command) and returns the result of each command.
func (c *rawClient) do(cmds []state.Command, seq int32) ([][]byte, error) {
	v, err := c.send(state.Command{Op: state.TXN, V: state.EncodeTxn(cmds)}, seq)
	if err != nil {
		return nil, err
	}
	vals, err := state.DecodeValues(v)
	if err != nil {
		return nil, err
	}
	res := make([][]byte, len(vals))
	for i := range vals {
		res[i] = vals[i]
	}
	return res, nil
}

// send sends a command with the standard client protocol and returns its result.
func (c *rawClient) send(cmd state.Command, seq int32) (state.Value, error) {
	c.w.WriteByte(defs.PROPOSE)
	(&defs.Propose{CommandId: seq, ClientId: c.id, Command: cmd}).Marshal(c.w)
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	rep := &defs.ProposeReplyTS{}
	if err := rep.Unmarshal(c.r); err != nil {
		return nil, err
	}
	if rep.CommandId != seq {
		return nil, fmt.Errorf("unexpected reply %d != %d", rep.CommandId, seq)
	}
	return rep.Value, nil
}

func put(k int64, v string) state.Command {
	return state.Command{Op: state.PUT, K: state.Key(k), V: state.Value(v)}
}
func get(k int64) state.Command {
	return state.Command{Op: state.GET, K: state.Key(k), V: state.NIL()}
}

func TestTxnsBasic(t *testing.T) {
	reps, addrs, _, _ := startCluster(t, 6, 3, 17200)
	_ = reps
	conn, err := net.Dial("tcp", addrs[1])
	if err != nil {
		t.Fatal(err)
	}
	c := &rawClient{1, bufio.NewWriter(conn), bufio.NewReader(conn)}
	if _, err := c.do([]state.Command{put(1, "a"), put(2, "b"), put(3, "c")}, 0); err != nil {
		t.Fatal(err)
	}
	vals, err := c.do([]state.Command{get(1), get(2), get(3), get(4)}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(vals[0]) != "a" || string(vals[1]) != "b" || string(vals[2]) != "c" || len(vals[3]) != 0 {
		t.Fatalf("bad values %q", vals)
	}
	// a scan spans all shards
	scan := state.Command{Op: state.SCAN, K: 1, V: make([]byte, 8)}
	scan.V[0] = 2
	vals, err = c.do([]state.Command{scan}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals[0]) != 3 {
		t.Fatalf("scan returned %q", vals[0])
	}

	// single commands (not wrapped in a TXN) are one-command transactions
	if _, err := c.send(put(7, "x"), 3); err != nil {
		t.Fatal(err)
	}
	if v, err := c.send(get(7), 4); err != nil || string(v) != "x" {
		t.Fatalf("single GET returned %q, %v", v, err)
	}

	// column model, in a transaction spanning both shards (keys 4 and 5)
	if _, err := c.do([]state.Command{client.Update(4, state.Row{"a": []byte("1"), "b": []byte("x")})}, 5); err != nil {
		t.Fatal(err)
	}
	vals, err = c.do([]state.Command{client.Incr(4, "a", 1), client.Incr(5, "c", 3), client.Incr(5, "c", -1)}, 6)
	if err != nil {
		t.Fatal(err)
	}
	if string(vals[0]) != "2" || string(vals[1]) != "3" || string(vals[2]) != "2" {
		t.Fatalf("INCR returned %q", vals)
	}
	vals, err = c.do([]state.Command{get(4), client.Delete(5), get(5)}, 7)
	if err != nil {
		t.Fatal(err)
	}
	if row, err := state.DecodeRow(vals[0]); err != nil || string(row["a"]) != "2" || string(row["b"]) != "x" || len(row) != 2 {
		t.Fatalf("row %v, %v", row, err)
	}
	if len(vals[2]) != 0 {
		t.Fatalf("deleted key returned %q", vals[2])
	}

	// a malformed transaction has no result
	v, err := c.send(state.Command{Op: state.TXN, V: state.Value("junk")}, 8)
	if err != nil {
		t.Fatal(err)
	}
	if vals, err := state.DecodeValues(v); err != nil || len(vals) != 0 {
		t.Fatalf("malformed transaction returned %q, %v", vals, err)
	}
}

func TestConcurrentTxns(t *testing.T) {
	for _, cfg := range []struct{ n, shard, port int }{{6, 3, 17300}, {3, 0, 17400}, {4, 2, 17500}, {5, 1, 17600}} {
		cfg := cfg
		t.Run(fmt.Sprintf("n%d_shard%d", cfg.n, cfg.shard), func(t *testing.T) {
			testConcurrent(t, cfg.n, cfg.shard, cfg.port)
		})
	}
}

func testConcurrent(t *testing.T, n, shardSize, port int) {
	reps, addrs, mu, traces := startCluster(t, n, shardSize, port)
	lay := reps[0].lay
	const (
		clients = 6
		perCli  = 60
		keys    = 6
	)
	all := make([][]obs, clients)
	var wg sync.WaitGroup
	for ci := 0; ci < clients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addrs[ci%n])
			if err != nil {
				t.Error(err)
				return
			}
			c := &rawClient{int32(ci + 1), bufio.NewWriter(conn), bufio.NewReader(conn)}
			rnd := rand.New(rand.NewSource(int64(ci)))
			for i := 0; i < perCli; i++ {
				pid := int64(privBase + ci*1000 + i)
				cmds := []state.Command{put(pid, "x")}
				for j := 0; j < 1+rnd.Intn(3); j++ {
					k := int64(rnd.Intn(keys))
					if rnd.Intn(2) == 0 {
						cmds = append(cmds, put(k, fmt.Sprintf("v%d.%d", pid, j)))
					} else {
						cmds = append(cmds, get(k))
					}
				}
				if rnd.Intn(15) == 0 {
					s := state.Command{Op: state.SCAN, K: 0, V: make([]byte, 8)}
					s.V[0] = keys
					cmds = append(cmds, s)
				}
				o := obs{cmds: cmds, pid: pid, start: time.Now()}
				vals, err := c.do(cmds, int32(i))
				if err != nil {
					t.Error(err)
					return
				}
				o.end, o.vals = time.Now(), vals
				all[ci] = append(all[ci], o)
			}
		}(ci)
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	total := clients * perCli

	// wait until every replica executed all the transactions of its shard
	want := make(map[int32]int)
	for _, os := range all {
		for _, o := range os {
			for _, s := range lay.ShardsOf(o.cmds) {
				for _, r := range lay.Replicas(s) {
					want[r]++
				}
			}
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		mu.Lock()
		ok := true
		for r, w := range want {
			if len(traces[r]) < w {
				ok = false
			}
		}
		mu.Unlock()
		if ok || time.Now().After(deadline) {
			if !ok {
				t.Fatalf("replicas did not execute everything: %v", want)
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()

	// identify transactions by their private key
	byPid := make(map[int64]*obs)
	for _, os := range all {
		for i := range os {
			byPid[os[i].pid] = &os[i]
		}
	}
	pidOf := func(cmds []state.Command) int64 {
		for _, c := range cmds {
			if c.K >= privBase {
				return int64(c.K)
			}
		}
		t.Fatal("no private key")
		return 0
	}

	// direct dependency check: conflicting txns must be related by a direct dependency
	for s := 0; s < lay.NumShards(); s++ {
		rr := reps[lay.Replicas(s)[0]]
		wr := make(map[state.Key][]TxnId)
		for _, e := range traces[rr.Id] {
			for _, c := range e.cmds {
				if c.Op == state.PUT && c.K < privBase && lay.ShardOfKey(c.K) == s {
					wr[c.K] = append(wr[c.K], e.id)
				}
			}
		}
		global := make(map[TxnId][]Dep) // committed dependencies known by any replica
		for _, r := range reps {
			for id, n := range r.nodes {
				if n.Status == COMMITTED {
					global[id] = n.Deps
				}
			}
		}
		has := func(a, b TxnId) bool { // b reachable from a
			seen := map[TxnId]bool{}
			var walk func(TxnId) bool
			walk = func(x TxnId) bool {
				if x == b {
					return true
				}
				if seen[x] {
					return false
				}
				seen[x] = true
				for _, d := range global[x] {
					if walk(d.Id) {
						return true
					}
				}
				return false
			}
			return walk(a)
		}
		for k, ids := range wr {
			for i := range ids {
				for j := i + 1; j < len(ids); j++ {
					i2, j2 := i, j
					if ids[i] != ids[j] && !has(ids[i], ids[j]) && !has(ids[j], ids[i]) {
						t.Errorf("shard %d key %d: txns %v and %v conflict without direct dependency", s, k, ids[i], ids[j])
						for _, rid := range lay.Replicas(s) {
							na, nb := reps[rid].nodes[ids[i]], reps[rid].nodes[ids[j]]
							t.Logf("  replica %d: A=%v st=%d deps=%v | B=%v st=%d deps=%v", rid, ids[i], na.Status, na.Deps, ids[j], nb.Status, nb.Deps)
						}
						for _, rid := range lay.Replicas(s) {
							for i, e := range pa[rid] {
								if e.id == ids[i2] || e.id == ids[j2] {
									t.Logf("  replica %d preaccept #%d of %v deps=%v", rid, i, e.id, e.deps)
								}
							}
						}
						return
					}
				}
			}
		}
	}
	// 1. replicas of a shard agree on the order of conflicting operations
	// 2. reads return the latest preceding write in the shard order
	// 3. the union of shard orders and of the real-time order is acyclic
	succ := make(map[int64]map[int64]bool)
	edge := func(a, b int64) {
		if a == b {
			return
		}
		if succ[a] == nil {
			succ[a] = make(map[int64]bool)
		}
		succ[a][b] = true
	}
	for s := 0; s < lay.NumShards(); s++ {
		perKeyOrder := func(r int32) map[state.Key][]string {
			res := make(map[state.Key][]string)
			for _, e := range traces[r] {
				pid := pidOf(e.cmds)
				for _, c := range e.cmds {
					if c.Op != state.SCAN && lay.ShardOfKey(c.K) == s && c.K < privBase {
						tag := "R"
						if c.Op == state.PUT {
							tag = "W"
						}
						res[c.K] = append(res[c.K], fmt.Sprint(tag, pid))
					}
				}
			}
			return res
		}
		reps0 := lay.Replicas(s)
		ref := perKeyOrder(reps0[0])
		for _, r := range reps0[1:] {
			got := perKeyOrder(r)
			for k, seq := range ref {
				if len(got[k]) != len(seq) {
					t.Fatalf("shard %d key %d: replicas diverge in length", s, k)
				}
				// writes must be in the same order; reads may commute with each other
				var w1, w2 []string
				for i := range seq {
					if seq[i][0] == 'W' {
						w1 = append(w1, seq[i])
					}
					if got[k][i][0] == 'W' {
						w2 = append(w2, got[k][i])
					}
				}
				if fmt.Sprint(w1) != fmt.Sprint(w2) {
					t.Fatalf("shard %d key %d: write order differs\n%v\n%v", s, k, w1, w2)
				}
			}
		}
		// replay the reference trace
		store := make(map[state.Key]string)
		lastTxn := make(map[state.Key]int64)
		readers := make(map[state.Key][]int64)
		for _, e := range traces[reps0[0]] {
			pid := pidOf(e.cmds)
			for i, c := range e.cmds {
				if c.Op == state.SCAN || lay.ShardOfKey(c.K) != s || c.K >= privBase {
					continue
				}
				if c.Op == state.PUT {
					if w, ok := lastTxn[c.K]; ok {
						edge(w, pid)
					}
					for _, rd := range readers[c.K] {
						edge(rd, pid)
					}
					readers[c.K] = nil
					store[c.K] = string(c.V)
					lastTxn[c.K] = pid
				} else {
					if w, ok := lastTxn[c.K]; ok {
						edge(w, pid)
					}
					readers[c.K] = append(readers[c.K], pid)
					// what the client saw (responder executes the same order)
					if o := byPid[pid]; o != nil {
						if string(o.vals[i]) != store[c.K] {
							t.Fatalf("txn %d read %q on key %d, expected %q", pid, o.vals[i], c.K, store[c.K])
						}
					}
				}
			}
		}
	}
	// real time order
	pids := make([]int64, 0, total)
	for p := range byPid {
		pids = append(pids, p)
	}
	for _, a := range pids {
		for _, b := range pids {
			if byPid[a].end.Before(byPid[b].start) {
				edge(a, b)
			}
		}
	}
	// cycle detection
	color := make(map[int64]int)
	var dfs func(int64) bool
	dfs = func(u int64) bool {
		color[u] = 1
		for v := range succ[u] {
			if color[v] == 1 || (color[v] == 0 && dfs(v)) {
				return true
			}
		}
		color[u] = 2
		return false
	}
	for _, p := range pids {
		if color[p] == 0 && dfs(p) {
			t.Fatal("not strictly serializable: cycle in the precedence graph")
		}
	}
	fast, slow := 0, 0
	for _, r := range reps {
		fast += r.fastPaths
		slow += r.slowPaths
	}
	t.Logf("%d txns: %d fast, %d slow", total, fast, slow)
}
