package curp

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/imdea-software/swiftpaxos/replica/defs"
	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
	"github.com/imdea-software/swiftpaxos/state"
)

// status
const (
	NORMAL = iota
	RECOVERING
)

// phase
const (
	START = iota
	ACCEPT
	COMMIT
)

const (
	HISTORY_SIZE = 10010001
	TRUE         = uint8(1)
	FALSE        = uint8(0)
	ORDERED      = uint8(2)
)

var MaxDescRoutines = 100

type CommandId struct {
	ClientId int32
	SeqNum   int32
}

func (cmdId CommandId) String() string {
	return fmt.Sprintf("%v,%v", cmdId.ClientId, cmdId.SeqNum)
}

type MReply struct {
	Replica int32
	Ballot  int32
	CmdId   CommandId
	Rep     []byte
	Ok      uint8
}

type MAccept struct {
	Replica int32
	Ballot  int32
	Cmd     state.Command
	CmdId   CommandId
	CmdSlot int
}

type MAcceptAck struct {
	Replica int32
	Ballot  int32
	CmdSlot int
}

type MAAcks struct {
	Acks    []MAcceptAck
	Accepts []MAccept
}

type MRecordAck struct {
	Replica int32
	Ballot  int32
	CmdId   CommandId
	Ok      uint8
}

type MCommit struct {
	Replica int32
	Ballot  int32
	CmdSlot int
}

type MSync struct {
	CmdId CommandId
}

type MSyncReply struct {
	Replica int32
	Ballot  int32
	CmdId   CommandId
	Rep     []byte
}

func (m *MReply) New() fastrpc.Serializable {
	return new(MReply)
}

func (m *MAccept) New() fastrpc.Serializable {
	return new(MAccept)
}

func (m *MAcceptAck) New() fastrpc.Serializable {
	return new(MAcceptAck)
}

func (m *MAAcks) New() fastrpc.Serializable {
	return new(MAAcks)
}

func (m *MRecordAck) New() fastrpc.Serializable {
	return new(MRecordAck)
}

func (m *MCommit) New() fastrpc.Serializable {
	return new(MCommit)
}

func (m *MSync) New() fastrpc.Serializable {
	return new(MSync)
}

func (m *MSyncReply) New() fastrpc.Serializable {
	return new(MSyncReply)
}

type CommunicationSupply struct {
	maxLatency time.Duration

	replyChan     chan fastrpc.Serializable
	acceptChan    chan fastrpc.Serializable
	acceptAckChan chan fastrpc.Serializable
	aacksChan     chan fastrpc.Serializable
	recordAckChan chan fastrpc.Serializable
	commitChan    chan fastrpc.Serializable
	syncChan      chan fastrpc.Serializable
	syncReplyChan chan fastrpc.Serializable

	replyRPC     uint8
	acceptRPC    uint8
	acceptAckRPC uint8
	aacksRPC     uint8
	recordAckRPC uint8
	commitRPC    uint8
	syncRPC      uint8
	syncReplyRPC uint8
}

func initCs(cs *CommunicationSupply, t *fastrpc.Table) {
	cs.maxLatency = 0

	cs.replyChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.acceptChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.acceptAckChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.aacksChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.recordAckChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.commitChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.syncChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.syncReplyChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)

	cs.replyRPC = t.Register(new(MReply), cs.replyChan)
	cs.acceptRPC = t.Register(new(MAccept), cs.acceptChan)
	cs.acceptAckRPC = t.Register(new(MAcceptAck), cs.acceptAckChan)
	cs.aacksRPC = t.Register(new(MAAcks), cs.aacksChan)
	cs.recordAckRPC = t.Register(new(MRecordAck), cs.recordAckChan)
	cs.commitRPC = t.Register(new(MCommit), cs.commitChan)
	cs.syncRPC = t.Register(new(MSync), cs.syncChan)
	cs.syncReplyRPC = t.Register(new(MSyncReply), cs.syncReplyChan)
}

type byteReader interface {
	io.Reader
	ReadByte() (c byte, err error)
}

func (t *MCommit) BinarySize() (nbytes int, sizeKnown bool) {
	return 16, true
}

type MCommitCache struct {
	mu    sync.Mutex
	cache []*MCommit
}

func NewMCommitCache() *MCommitCache {
	c := &MCommitCache{}
	c.cache = make([]*MCommit, 0)
	return c
}

func (p *MCommitCache) Get() *MCommit {
	var t *MCommit
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MCommit{}
	}
	return t
}
func (p *MCommitCache) Put(t *MCommit) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MCommit) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp64 := t.CmdSlot
	bs[8] = byte(tmp64)
	bs[9] = byte(tmp64 >> 8)
	bs[10] = byte(tmp64 >> 16)
	bs[11] = byte(tmp64 >> 24)
	bs[12] = byte(tmp64 >> 32)
	bs[13] = byte(tmp64 >> 40)
	bs[14] = byte(tmp64 >> 48)
	bs[15] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *MCommit) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdSlot = int((uint64(bs[8]) | (uint64(bs[9]) << 8) | (uint64(bs[10]) << 16) | (uint64(bs[11]) << 24) | (uint64(bs[12]) << 32) | (uint64(bs[13]) << 40) | (uint64(bs[14]) << 48) | (uint64(bs[15]) << 56)))
	return nil
}

func (t *MSyncReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type MSyncReplyCache struct {
	mu    sync.Mutex
	cache []*MSyncReply
}

func NewMSyncReplyCache() *MSyncReplyCache {
	c := &MSyncReplyCache{}
	c.cache = make([]*MSyncReply, 0)
	return c
}

func (p *MSyncReplyCache) Get() *MSyncReply {
	var t *MSyncReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MSyncReply{}
	}
	return t
}
func (p *MSyncReplyCache) Put(t *MSyncReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MSyncReply) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.ClientId
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.SeqNum
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Rep))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		bs = b[:1]
		bs[0] = byte(t.Rep[i])
		wire.Write(bs)
	}
}

func (t *MSyncReply) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdId.ClientId = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.CmdId.SeqNum = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Rep = make([]byte, alen1)
	for i := int64(0); i < alen1; i++ {
		bs = b[:1]
		if _, err := io.ReadAtLeast(wire, bs, 1); err != nil {
			return err
		}
		t.Rep[i] = byte(bs[0])
	}
	return nil
}

func (t *MAccept) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type MAcceptCache struct {
	mu    sync.Mutex
	cache []*MAccept
}

func NewMAcceptCache() *MAcceptCache {
	c := &MAcceptCache{}
	c.cache = make([]*MAccept, 0)
	return c
}

func (p *MAcceptCache) Get() *MAccept {
	var t *MAccept
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MAccept{}
	}
	return t
}
func (p *MAcceptCache) Put(t *MAccept) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MAccept) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Cmd.Marshal(wire)
	bs = b[:16]
	tmp32 = t.CmdId.ClientId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.SeqNum
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp64 := t.CmdSlot
	bs[8] = byte(tmp64)
	bs[9] = byte(tmp64 >> 8)
	bs[10] = byte(tmp64 >> 16)
	bs[11] = byte(tmp64 >> 24)
	bs[12] = byte(tmp64 >> 32)
	bs[13] = byte(tmp64 >> 40)
	bs[14] = byte(tmp64 >> 48)
	bs[15] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *MAccept) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Cmd.Unmarshal(wire)
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.CmdId.ClientId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.CmdId.SeqNum = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdSlot = int((uint64(bs[8]) | (uint64(bs[9]) << 8) | (uint64(bs[10]) << 16) | (uint64(bs[11]) << 24) | (uint64(bs[12]) << 32) | (uint64(bs[13]) << 40) | (uint64(bs[14]) << 48) | (uint64(bs[15]) << 56)))
	return nil
}

func (t *MReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type MReplyCache struct {
	mu    sync.Mutex
	cache []*MReply
}

func NewMReplyCache() *MReplyCache {
	c := &MReplyCache{}
	c.cache = make([]*MReply, 0)
	return c
}

func (p *MReplyCache) Get() *MReply {
	var t *MReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MReply{}
	}
	return t
}
func (p *MReplyCache) Put(t *MReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MReply) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.ClientId
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.SeqNum
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Rep))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		bs = b[:1]
		bs[0] = byte(t.Rep[i])
		wire.Write(bs)
	}
	bs[0] = byte(t.Ok)
	wire.Write(bs)
}

func (t *MReply) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdId.ClientId = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.CmdId.SeqNum = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Rep = make([]byte, alen1)
	for i := int64(0); i < alen1; i++ {
		bs = b[:1]
		if _, err := io.ReadAtLeast(wire, bs, 1); err != nil {
			return err
		}
		t.Rep[i] = byte(bs[0])
	}
	if _, err := io.ReadAtLeast(wire, bs, 1); err != nil {
		return err
	}
	t.Ok = uint8(bs[0])
	return nil
}

func (t *MAcceptAck) BinarySize() (nbytes int, sizeKnown bool) {
	return 16, true
}

type MAcceptAckCache struct {
	mu    sync.Mutex
	cache []*MAcceptAck
}

func NewMAcceptAckCache() *MAcceptAckCache {
	c := &MAcceptAckCache{}
	c.cache = make([]*MAcceptAck, 0)
	return c
}

func (p *MAcceptAckCache) Get() *MAcceptAck {
	var t *MAcceptAck
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MAcceptAck{}
	}
	return t
}
func (p *MAcceptAckCache) Put(t *MAcceptAck) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MAcceptAck) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp64 := t.CmdSlot
	bs[8] = byte(tmp64)
	bs[9] = byte(tmp64 >> 8)
	bs[10] = byte(tmp64 >> 16)
	bs[11] = byte(tmp64 >> 24)
	bs[12] = byte(tmp64 >> 32)
	bs[13] = byte(tmp64 >> 40)
	bs[14] = byte(tmp64 >> 48)
	bs[15] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *MAcceptAck) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdSlot = int((uint64(bs[8]) | (uint64(bs[9]) << 8) | (uint64(bs[10]) << 16) | (uint64(bs[11]) << 24) | (uint64(bs[12]) << 32) | (uint64(bs[13]) << 40) | (uint64(bs[14]) << 48) | (uint64(bs[15]) << 56)))
	return nil
}

func (t *MAAcks) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type MAAcksCache struct {
	mu    sync.Mutex
	cache []*MAAcks
}

func NewMAAcksCache() *MAAcksCache {
	c := &MAAcksCache{}
	c.cache = make([]*MAAcks, 0)
	return c
}

func (p *MAAcksCache) Get() *MAAcks {
	var t *MAAcks
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MAAcks{}
	}
	return t
}
func (p *MAAcksCache) Put(t *MAAcks) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MAAcks) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:]
	alen1 := int64(len(t.Acks))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		bs = b[:4]
		tmp32 := t.Acks[i].Replica
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
		tmp32 = t.Acks[i].Ballot
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
		bs = b[:8]
		tmp64 := t.Acks[i].CmdSlot
		bs[0] = byte(tmp64)
		bs[1] = byte(tmp64 >> 8)
		bs[2] = byte(tmp64 >> 16)
		bs[3] = byte(tmp64 >> 24)
		bs[4] = byte(tmp64 >> 32)
		bs[5] = byte(tmp64 >> 40)
		bs[6] = byte(tmp64 >> 48)
		bs[7] = byte(tmp64 >> 56)
		wire.Write(bs)
	}
	bs = b[:]
	alen2 := int64(len(t.Accepts))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		t.Accepts[i].Marshal(wire)
	}
}

func (t *MAAcks) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Acks = make([]MAcceptAck, alen1)
	for i := int64(0); i < alen1; i++ {
		bs = b[:4]
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		t.Acks[i].Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		t.Acks[i].Ballot = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
		bs = b[:8]
		if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
			return err
		}
		t.Acks[i].CmdSlot = int((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	}
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Accepts = make([]MAccept, alen2)
	for i := int64(0); i < alen2; i++ {
		t.Accepts[i].Unmarshal(wire)
	}
	return nil
}

func (t *MRecordAck) BinarySize() (nbytes int, sizeKnown bool) {
	return 17, true
}

type MRecordAckCache struct {
	mu    sync.Mutex
	cache []*MRecordAck
}

func NewMRecordAckCache() *MRecordAckCache {
	c := &MRecordAckCache{}
	c.cache = make([]*MRecordAck, 0)
	return c
}

func (p *MRecordAckCache) Get() *MRecordAck {
	var t *MRecordAck
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MRecordAck{}
	}
	return t
}
func (p *MRecordAckCache) Put(t *MRecordAck) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MRecordAck) Marshal(wire io.Writer) {
	var b [17]byte
	var bs []byte
	bs = b[:17]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.ClientId
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.SeqNum
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	bs[16] = byte(t.Ok)
	wire.Write(bs)
}

func (t *MRecordAck) Unmarshal(wire io.Reader) error {
	var b [17]byte
	var bs []byte
	bs = b[:17]
	if _, err := io.ReadAtLeast(wire, bs, 17); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdId.ClientId = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.CmdId.SeqNum = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	t.Ok = uint8(bs[16])
	return nil
}

func (t *MSync) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type MSyncCache struct {
	mu    sync.Mutex
	cache []*MSync
}

func NewMSyncCache() *MSyncCache {
	c := &MSyncCache{}
	c.cache = make([]*MSync, 0)
	return c
}

func (p *MSyncCache) Get() *MSync {
	var t *MSync
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MSync{}
	}
	return t
}
func (p *MSyncCache) Put(t *MSync) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MSync) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.CmdId.ClientId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.SeqNum
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *MSync) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.CmdId.ClientId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.CmdId.SeqNum = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	return nil
}

func (t *CommandId) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type CommandIdCache struct {
	mu    sync.Mutex
	cache []*CommandId
}

func NewCommandIdCache() *CommandIdCache {
	c := &CommandIdCache{}
	c.cache = make([]*CommandId, 0)
	return c
}

func (p *CommandIdCache) Get() *CommandId {
	var t *CommandId
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &CommandId{}
	}
	return t
}
func (p *CommandIdCache) Put(t *CommandId) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *CommandId) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.ClientId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.SeqNum
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *CommandId) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.ClientId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.SeqNum = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	return nil
}
