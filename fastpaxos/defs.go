package fastpaxos

import (
	"io"
	"sync"

	"github.com/imdea-software/swiftpaxos/replica/defs"
	fastrpc "github.com/imdea-software/swiftpaxos/rpc"
)

type CommandId struct {
	ClientId int32
	SeqNum   int32
}

type M2B struct {
	CmdId      CommandId
	Ballot     int32
	Cballot    int32
	Replica    int32
	InstanceId int
}

type CommunicationSupply struct {
	m2BChan chan fastrpc.Serializable
	m2BRPC  uint8
}

func initCs(cs *CommunicationSupply, t *fastrpc.Table) {
	cs.m2BChan = make(chan fastrpc.Serializable, defs.CHAN_BUFFER_SIZE)
	cs.m2BRPC = t.Register(new(M2B), cs.m2BChan)
}

func (m *M2B) New() fastrpc.Serializable {
	return new(M2B)
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

func (t *M2B) BinarySize() (nbytes int, sizeKnown bool) {
	return 28, true
}

type M2BCache struct {
	mu    sync.Mutex
	cache []*M2B
}

func NewM2BCache() *M2BCache {
	c := &M2BCache{}
	c.cache = make([]*M2B, 0)
	return c
}

func (p *M2BCache) Get() *M2B {
	var t *M2B
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &M2B{}
	}
	return t
}
func (p *M2BCache) Put(t *M2B) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *M2B) Marshal(wire io.Writer) {
	var b [28]byte
	var bs []byte
	bs = b[:28]
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
	tmp32 = t.Ballot
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.Cballot
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	tmp32 = t.Replica
	bs[16] = byte(tmp32)
	bs[17] = byte(tmp32 >> 8)
	bs[18] = byte(tmp32 >> 16)
	bs[19] = byte(tmp32 >> 24)
	tmp64 := t.InstanceId
	bs[20] = byte(tmp64)
	bs[21] = byte(tmp64 >> 8)
	bs[22] = byte(tmp64 >> 16)
	bs[23] = byte(tmp64 >> 24)
	bs[24] = byte(tmp64 >> 32)
	bs[25] = byte(tmp64 >> 40)
	bs[26] = byte(tmp64 >> 48)
	bs[27] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *M2B) Unmarshal(wire io.Reader) error {
	var b [28]byte
	var bs []byte
	bs = b[:28]
	if _, err := io.ReadAtLeast(wire, bs, 28); err != nil {
		return err
	}
	t.CmdId.ClientId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.CmdId.SeqNum = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Ballot = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.Cballot = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	t.Replica = int32((uint32(bs[16]) | (uint32(bs[17]) << 8) | (uint32(bs[18]) << 16) | (uint32(bs[19]) << 24)))
	t.InstanceId = int((uint64(bs[20]) | (uint64(bs[21]) << 8) | (uint64(bs[22]) << 16) | (uint64(bs[23]) << 24) | (uint64(bs[24]) << 32) | (uint64(bs[25]) << 40) | (uint64(bs[26]) << 48) | (uint64(bs[27]) << 56)))
	return nil
}
