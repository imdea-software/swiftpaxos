package janus

import (
	"encoding/binary"
	"errors"
	"io"

	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
	"github.com/imdea-software/swiftpaxos/state"
)

// Minimal binary encoding helpers (little endian). Marshal methods cannot
// report errors (see rpc.Serializable); the underlying writers are buffered
// and errors surface on flush/read at the peer.

const maxLen = 1 << 28

type enc struct {
	w   io.Writer
	buf [8]byte
}

func (e *enc) u8(v uint8) {
	e.buf[0] = v
	e.w.Write(e.buf[:1])
}

func (e *enc) i32(v int32) {
	binary.LittleEndian.PutUint32(e.buf[:4], uint32(v))
	e.w.Write(e.buf[:4])
}

func (e *enc) u64(v uint64) {
	binary.LittleEndian.PutUint64(e.buf[:8], v)
	e.w.Write(e.buf[:8])
}

func (e *enc) bytes(b []byte) {
	e.i32(int32(len(b)))
	e.w.Write(b)
}

func (e *enc) id(t TxnId) {
	e.i32(t.R)
	e.i32(t.I)
}

func (e *enc) deps(ds []Dep) {
	e.i32(int32(len(ds)))
	for _, d := range ds {
		e.id(d.Id)
		e.u64(d.Shards)
	}
}

func (e *enc) cmds(cs []state.Command) {
	e.i32(int32(len(cs)))
	for _, c := range cs {
		e.u8(uint8(c.Op))
		e.u64(uint64(c.K))
		e.bytes(c.V)
	}
}

func (e *enc) vals(vs [][]byte) {
	e.i32(int32(len(vs)))
	for _, v := range vs {
		e.bytes(v)
	}
}

type dec struct {
	r   io.Reader
	buf [8]byte
	err error
}

func (d *dec) fill(n int) bool {
	if d.err != nil {
		return false
	}
	_, d.err = io.ReadFull(d.r, d.buf[:n])
	return d.err == nil
}

func (d *dec) u8() uint8 {
	if !d.fill(1) {
		return 0
	}
	return d.buf[0]
}

func (d *dec) i32() int32 {
	if !d.fill(4) {
		return 0
	}
	return int32(binary.LittleEndian.Uint32(d.buf[:4]))
}

func (d *dec) u64() uint64 {
	if !d.fill(8) {
		return 0
	}
	return binary.LittleEndian.Uint64(d.buf[:8])
}

func (d *dec) length() int {
	n := d.i32()
	if d.err != nil {
		return 0
	}
	if n < 0 || n > maxLen {
		d.err = errors.New("janus: bad length")
		return 0
	}
	return int(n)
}

func (d *dec) bytes() []byte {
	n := d.length()
	b := make([]byte, n)
	if d.err == nil {
		_, d.err = io.ReadFull(d.r, b)
	}
	return b
}

func (d *dec) id() TxnId {
	r := d.i32()
	return TxnId{r, d.i32()}
}

func (d *dec) deps() []Dep {
	n := d.length()
	ds := make([]Dep, 0, n)
	for i := 0; i < n && d.err == nil; i++ {
		id := d.id()
		ds = append(ds, Dep{id, d.u64()})
	}
	return ds
}

func (d *dec) cmds() []state.Command {
	n := d.length()
	cs := make([]state.Command, 0, n)
	for i := 0; i < n && d.err == nil; i++ {
		op := state.Operation(d.u8())
		k := state.Key(d.u64())
		cs = append(cs, state.Command{Op: op, K: k, V: state.Value(d.bytes())})
	}
	return cs
}

func (d *dec) vals() [][]byte {
	n := d.length()
	vs := make([][]byte, 0, n)
	for i := 0; i < n && d.err == nil; i++ {
		vs = append(vs, d.bytes())
	}
	return vs
}

// TxnId identifies a transaction: coordinator replica and a per-coordinator counter.
type TxnId struct {
	R, I int32
}

func (t TxnId) less(o TxnId) bool {
	return t.I < o.I || (t.I == o.I && t.R < o.R)
}

// Dep is a dependency edge, annotated with the shards the target touches.
type Dep struct {
	Id     TxnId
	Shards uint64
}

// ---- client <-> coordinator ----

// TxnPropose is a one-shot transaction: a list of single-key operations.
type TxnPropose struct {
	CommandId int32
	ClientId  int32
	Cmds      []state.Command
}

func (m *TxnPropose) GetClientId() int32        { return m.ClientId }
func (m *TxnPropose) New() fastrpc.Serializable { return new(TxnPropose) }
func (m *TxnPropose) Marshal(w io.Writer) {
	e := &enc{w: w}
	e.i32(m.CommandId)
	e.i32(m.ClientId)
	e.cmds(m.Cmds)
}
func (m *TxnPropose) Unmarshal(r io.Reader) error {
	d := &dec{r: r}
	m.CommandId = d.i32()
	m.ClientId = d.i32()
	m.Cmds = d.cmds()
	return d.err
}

// TxnReply carries one result per operation, in the order of the transaction.
type TxnReply struct {
	CommandId int32
	Vals      [][]byte
}

func (m *TxnReply) New() fastrpc.Serializable { return new(TxnReply) }
func (m *TxnReply) Marshal(w io.Writer) {
	e := &enc{w: w}
	e.i32(m.CommandId)
	e.vals(m.Vals)
}
func (m *TxnReply) Unmarshal(r io.Reader) error {
	d := &dec{r: r}
	m.CommandId = d.i32()
	m.Vals = d.vals()
	return d.err
}

// ---- replica <-> replica ----

// PreAccept: the coordinator submits a transaction to the replicas of a shard.
type PreAccept struct {
	Id     TxnId
	Shards uint64
	Cmds   []state.Command
}

func (m *PreAccept) New() fastrpc.Serializable { return new(PreAccept) }
func (m *PreAccept) Marshal(w io.Writer) {
	e := &enc{w: w}
	e.id(m.Id)
	e.u64(m.Shards)
	e.cmds(m.Cmds)
}
func (m *PreAccept) Unmarshal(r io.Reader) error {
	d := &dec{r: r}
	m.Id = d.id()
	m.Shards = d.u64()
	m.Cmds = d.cmds()
	return d.err
}

// PreAcceptReply carries the dependencies observed at a replica.
type PreAcceptReply struct {
	Id      TxnId
	Replica int32
	Ballot  int32
	OK      uint8
	Deps    []Dep
}

func (m *PreAcceptReply) New() fastrpc.Serializable { return new(PreAcceptReply) }
func (m *PreAcceptReply) Marshal(w io.Writer) {
	e := &enc{w: w}
	e.id(m.Id)
	e.i32(m.Replica)
	e.i32(m.Ballot)
	e.u8(m.OK)
	e.deps(m.Deps)
}
func (m *PreAcceptReply) Unmarshal(r io.Reader) error {
	d := &dec{r: r}
	m.Id = d.id()
	m.Replica = d.i32()
	m.Ballot = d.i32()
	m.OK = d.u8()
	m.Deps = d.deps()
	return d.err
}

// Accept fixes the (merged) dependencies of a transaction at a shard (Paxos phase 2).
type Accept struct {
	Id     TxnId
	Ballot int32
	Shards uint64
	Cmds   []state.Command
	Deps   []Dep
}

func (m *Accept) New() fastrpc.Serializable { return new(Accept) }
func (m *Accept) Marshal(w io.Writer) {
	e := &enc{w: w}
	e.id(m.Id)
	e.i32(m.Ballot)
	e.u64(m.Shards)
	e.cmds(m.Cmds)
	e.deps(m.Deps)
}
func (m *Accept) Unmarshal(r io.Reader) error {
	d := &dec{r: r}
	m.Id = d.id()
	m.Ballot = d.i32()
	m.Shards = d.u64()
	m.Cmds = d.cmds()
	m.Deps = d.deps()
	return d.err
}

type AcceptReply struct {
	Id      TxnId
	Replica int32
	Ballot  int32
	OK      uint8
}

func (m *AcceptReply) New() fastrpc.Serializable { return new(AcceptReply) }
func (m *AcceptReply) Marshal(w io.Writer) {
	e := &enc{w: w}
	e.id(m.Id)
	e.i32(m.Replica)
	e.i32(m.Ballot)
	e.u8(m.OK)
}
func (m *AcceptReply) Unmarshal(r io.Reader) error {
	d := &dec{r: r}
	m.Id = d.id()
	m.Replica = d.i32()
	m.Ballot = d.i32()
	m.OK = d.u8()
	return d.err
}

// Commit carries the final dependencies of a transaction.
type Commit struct {
	Id     TxnId
	Shards uint64
	Cmds   []state.Command
	Deps   []Dep
}

func (m *Commit) New() fastrpc.Serializable { return new(Commit) }
func (m *Commit) Marshal(w io.Writer) {
	e := &enc{w: w}
	e.id(m.Id)
	e.u64(m.Shards)
	e.cmds(m.Cmds)
	e.deps(m.Deps)
}
func (m *Commit) Unmarshal(r io.Reader) error {
	d := &dec{r: r}
	m.Id = d.id()
	m.Shards = d.u64()
	m.Cmds = d.cmds()
	m.Deps = d.deps()
	return d.err
}

// Inquire asks a replica to resend the Commit of a transaction (if committed there).
type Inquire struct {
	Id   TxnId
	From int32
}

func (m *Inquire) New() fastrpc.Serializable { return new(Inquire) }
func (m *Inquire) Marshal(w io.Writer) {
	e := &enc{w: w}
	e.id(m.Id)
	e.i32(m.From)
}
func (m *Inquire) Unmarshal(r io.Reader) error {
	d := &dec{r: r}
	m.Id = d.id()
	m.From = d.i32()
	return d.err
}

// ExecResult is sent by the designated replica of a shard to the coordinator
// once the shard's part of the transaction is executed. Vals has one entry per
// operation of the transaction (empty for operations of other shards).
type ExecResult struct {
	Id    TxnId
	Shard int32
	Vals  [][]byte
}

func (m *ExecResult) New() fastrpc.Serializable { return new(ExecResult) }
func (m *ExecResult) Marshal(w io.Writer) {
	e := &enc{w: w}
	e.id(m.Id)
	e.i32(m.Shard)
	e.vals(m.Vals)
}
func (m *ExecResult) Unmarshal(r io.Reader) error {
	d := &dec{r: r}
	m.Id = d.id()
	m.Shard = d.i32()
	m.Vals = d.vals()
	return d.err
}
