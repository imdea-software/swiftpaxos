package defs

import (
	"bufio"
	"io"
	"sync"

	"github.com/imdea-software/swiftpaxos/state"
)

// master RPC definitions

type RegisterArgs struct {
	Addr string
	Port int
}

type RegisterReply struct {
	ReplicaId int
	NodeList  []string
	Ready     bool
	IsLeader  bool
}

type GetLeaderArgs struct{}

type GetLeaderReply struct {
	LeaderId int
}

type GetReplicaListArgs struct{}

type GetReplicaListReply struct {
	ReplicaList []string
	AliveList   []bool
	Ready       bool
}

// replica and client definitions

const (
	PROPOSE uint8 = iota
	PROPOSE_REPLY
	READ
	READ_REPLY
	PROPOSE_AND_READ
	PROPOSE_AND_READ_REPLY
	GENERIC_SMR_BEACON
	GENERIC_SMR_BEACON_REPLY
	STATS
	RPC_TABLE
)

type Propose struct {
	CommandId int32
	ClientId  int32
	Command   state.Command
	Timestamp int64
}

type ProposeReply struct {
	OK        uint8
	CommandId int32
}

type ProposeReplyTS struct {
	OK        uint8
	CommandId int32
	Value     state.Value
	Timestamp int64
}

type Read struct {
	CommandId int32
	Key       state.Key
}

type ReadReply struct {
	CommandId int32
	Value     state.Value
}

type ProposeAndRead struct {
	CommandId int32
	Command   state.Command
	Key       state.Key
}

type ProposeAndReadReply struct {
	OK        uint8
	CommandId int32
	Value     state.Value
}

type Beacon struct {
	Timestamp int64
}

type BeaconReply struct {
	Timestamp int64
}

type PingArgs struct {
	ActAsLeader uint8
}

type PingReply struct{}

type BeTheLeaderArgs struct{}

type BeTheLeaderReply struct {
	Leader     int32
	NextLeader int32
}

type GPropose struct {
	*Propose
	Reply *bufio.Writer
	Mutex *sync.Mutex
	Proxy bool
	Addr  string
}

type GBeacon struct {
	Rid       int32
	Timestamp int64
}

const (
	CHAN_BUFFER_SIZE = 2000000
	TRUE             = uint8(1)
	FALSE            = uint8(0)
)

var (
	Storage      = ""
	StoreFilname = "stable_store"
)

func NewBeTheLeaderReply() *BeTheLeaderReply {
	return &BeTheLeaderReply{
		Leader:     -1,
		NextLeader: -1,
	}
}

func UpdateBeTheLeaderReply(btlr *BeTheLeaderReply) {
	if btlr.Leader == -2 {
		btlr.Leader = 0
	}
	if btlr.NextLeader == -2 {
		btlr.NextLeader = 0
	}
}

func (r *BeTheLeaderReply) IsDefault() bool {
	return r.Leader == -1 && r.NextLeader == -1
}

type Stats struct {
	M map[string]int `json:"stats"`
}

///////////////////////////////////////////////////////////////////////////////
//                                                                           //
//  Generated with gobin-codegen [https://code.google.com/p/gobin-codegen/]  //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////

func (t *PingArgs) BinarySize() (nbytes int, sizeKnown bool) {
	return 1, true
}

type PingArgsCache struct {
	mu    sync.Mutex
	cache []*PingArgs
}

func NewPingArgsCache() *PingArgsCache {
	c := &PingArgsCache{}
	c.cache = make([]*PingArgs, 0)
	return c
}

func (p *PingArgsCache) Get() *PingArgs {
	var t *PingArgs
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &PingArgs{}
	}
	return t
}
func (p *PingArgsCache) Put(t *PingArgs) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *PingArgs) Marshal(wire io.Writer) {
	var b [1]byte
	var bs []byte
	bs = b[:1]
	bs[0] = byte(t.ActAsLeader)
	wire.Write(bs)
}

func (t *PingArgs) Unmarshal(wire io.Reader) error {
	var b [1]byte
	var bs []byte
	bs = b[:1]
	if _, err := io.ReadAtLeast(wire, bs, 1); err != nil {
		return err
	}
	t.ActAsLeader = uint8(bs[0])
	return nil
}

func (t *PingReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, true
}

type PingReplyCache struct {
	mu    sync.Mutex
	cache []*PingReply
}

func NewPingReplyCache() *PingReplyCache {
	c := &PingReplyCache{}
	c.cache = make([]*PingReply, 0)
	return c
}

func (p *PingReplyCache) Get() *PingReply {
	var t *PingReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &PingReply{}
	}
	return t
}
func (p *PingReplyCache) Put(t *PingReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *PingReply) Marshal(wire io.Writer) {
}

func (t *PingReply) Unmarshal(wire io.Reader) error {
	return nil
}

func (t *BeTheLeaderReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type BeTheLeaderReplyCache struct {
	mu    sync.Mutex
	cache []*BeTheLeaderReply
}

func NewBeTheLeaderReplyCache() *BeTheLeaderReplyCache {
	c := &BeTheLeaderReplyCache{}
	c.cache = make([]*BeTheLeaderReply, 0)
	return c
}

func (p *BeTheLeaderReplyCache) Get() *BeTheLeaderReply {
	var t *BeTheLeaderReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &BeTheLeaderReply{}
	}
	return t
}
func (p *BeTheLeaderReplyCache) Put(t *BeTheLeaderReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *BeTheLeaderReply) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.Leader
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.NextLeader
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *BeTheLeaderReply) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Leader = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.NextLeader = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	return nil
}

func (t *Propose) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ProposeCache struct {
	mu    sync.Mutex
	cache []*Propose
}

func NewProposeCache() *ProposeCache {
	c := &ProposeCache{}
	c.cache = make([]*Propose, 0)
	return c
}

func (p *ProposeCache) Get() *Propose {
	var t *Propose
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Propose{}
	}
	return t
}
func (p *ProposeCache) Put(t *Propose) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Propose) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.ClientId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Command.Marshal(wire)
	tmp64 := t.Timestamp
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

func (t *Propose) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.CommandId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.ClientId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Command.Unmarshal(wire)
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Timestamp = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	return nil
}

func (t *ProposeReplyTS) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ProposeReplyTSCache struct {
	mu    sync.Mutex
	cache []*ProposeReplyTS
}

func NewProposeReplyTSCache() *ProposeReplyTSCache {
	c := &ProposeReplyTSCache{}
	c.cache = make([]*ProposeReplyTS, 0)
	return c
}

func (p *ProposeReplyTSCache) Get() *ProposeReplyTS {
	var t *ProposeReplyTS
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ProposeReplyTS{}
	}
	return t
}
func (p *ProposeReplyTSCache) Put(t *ProposeReplyTS) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ProposeReplyTS) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:5]
	bs[0] = byte(t.OK)
	tmp32 := t.CommandId
	bs[1] = byte(tmp32)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32 >> 16)
	bs[4] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Value.Marshal(wire)
	bs = b[:8]
	tmp64 := t.Timestamp
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

func (t *ProposeReplyTS) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:5]
	if _, err := io.ReadAtLeast(wire, bs, 5); err != nil {
		return err
	}
	t.OK = uint8(bs[0])
	t.CommandId = int32((uint32(bs[1]) | (uint32(bs[2]) << 8) | (uint32(bs[3]) << 16) | (uint32(bs[4]) << 24)))
	t.Value.Unmarshal(wire)
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Timestamp = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	return nil
}

func (t *Read) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ReadCache struct {
	mu    sync.Mutex
	cache []*Read
}

func NewReadCache() *ReadCache {
	c := &ReadCache{}
	c.cache = make([]*Read, 0)
	return c
}

func (p *ReadCache) Get() *Read {
	var t *Read
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Read{}
	}
	return t
}
func (p *ReadCache) Put(t *Read) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Read) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Key.Marshal(wire)
}

func (t *Read) Unmarshal(wire io.Reader) error {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.CommandId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Key.Unmarshal(wire)
	return nil
}

func (t *ProposeAndRead) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ProposeAndReadCache struct {
	mu    sync.Mutex
	cache []*ProposeAndRead
}

func NewProposeAndReadCache() *ProposeAndReadCache {
	c := &ProposeAndReadCache{}
	c.cache = make([]*ProposeAndRead, 0)
	return c
}

func (p *ProposeAndReadCache) Get() *ProposeAndRead {
	var t *ProposeAndRead
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ProposeAndRead{}
	}
	return t
}
func (p *ProposeAndReadCache) Put(t *ProposeAndRead) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ProposeAndRead) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Command.Marshal(wire)
	t.Key.Marshal(wire)
}

func (t *ProposeAndRead) Unmarshal(wire io.Reader) error {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.CommandId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Command.Unmarshal(wire)
	t.Key.Unmarshal(wire)
	return nil
}

func (t *BeaconReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type BeaconReplyCache struct {
	mu    sync.Mutex
	cache []*BeaconReply
}

func NewBeaconReplyCache() *BeaconReplyCache {
	c := &BeaconReplyCache{}
	c.cache = make([]*BeaconReply, 0)
	return c
}

func (p *BeaconReplyCache) Get() *BeaconReply {
	var t *BeaconReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &BeaconReply{}
	}
	return t
}
func (p *BeaconReplyCache) Put(t *BeaconReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *BeaconReply) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp64 := t.Timestamp
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

func (t *BeaconReply) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Timestamp = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	return nil
}

func (t *BeTheLeaderArgs) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, true
}

type BeTheLeaderArgsCache struct {
	mu    sync.Mutex
	cache []*BeTheLeaderArgs
}

func NewBeTheLeaderArgsCache() *BeTheLeaderArgsCache {
	c := &BeTheLeaderArgsCache{}
	c.cache = make([]*BeTheLeaderArgs, 0)
	return c
}

func (p *BeTheLeaderArgsCache) Get() *BeTheLeaderArgs {
	var t *BeTheLeaderArgs
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &BeTheLeaderArgs{}
	}
	return t
}
func (p *BeTheLeaderArgsCache) Put(t *BeTheLeaderArgs) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *BeTheLeaderArgs) Marshal(wire io.Writer) {
}

func (t *BeTheLeaderArgs) Unmarshal(wire io.Reader) error {
	return nil
}

func (t *ProposeReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 5, true
}

type ProposeReplyCache struct {
	mu    sync.Mutex
	cache []*ProposeReply
}

func NewProposeReplyCache() *ProposeReplyCache {
	c := &ProposeReplyCache{}
	c.cache = make([]*ProposeReply, 0)
	return c
}

func (p *ProposeReplyCache) Get() *ProposeReply {
	var t *ProposeReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ProposeReply{}
	}
	return t
}
func (p *ProposeReplyCache) Put(t *ProposeReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ProposeReply) Marshal(wire io.Writer) {
	var b [5]byte
	var bs []byte
	bs = b[:5]
	bs[0] = byte(t.OK)
	tmp32 := t.CommandId
	bs[1] = byte(tmp32)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32 >> 16)
	bs[4] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *ProposeReply) Unmarshal(wire io.Reader) error {
	var b [5]byte
	var bs []byte
	bs = b[:5]
	if _, err := io.ReadAtLeast(wire, bs, 5); err != nil {
		return err
	}
	t.OK = uint8(bs[0])
	t.CommandId = int32((uint32(bs[1]) | (uint32(bs[2]) << 8) | (uint32(bs[3]) << 16) | (uint32(bs[4]) << 24)))
	return nil
}

func (t *ReadReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ReadReplyCache struct {
	mu    sync.Mutex
	cache []*ReadReply
}

func NewReadReplyCache() *ReadReplyCache {
	c := &ReadReplyCache{}
	c.cache = make([]*ReadReply, 0)
	return c
}

func (p *ReadReplyCache) Get() *ReadReply {
	var t *ReadReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ReadReply{}
	}
	return t
}
func (p *ReadReplyCache) Put(t *ReadReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ReadReply) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	tmp32 := t.CommandId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Value.Marshal(wire)
}

func (t *ReadReply) Unmarshal(wire io.Reader) error {
	var b [4]byte
	var bs []byte
	bs = b[:4]
	if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
		return err
	}
	t.CommandId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Value.Unmarshal(wire)
	return nil
}

func (t *ProposeAndReadReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type ProposeAndReadReplyCache struct {
	mu    sync.Mutex
	cache []*ProposeAndReadReply
}

func NewProposeAndReadReplyCache() *ProposeAndReadReplyCache {
	c := &ProposeAndReadReplyCache{}
	c.cache = make([]*ProposeAndReadReply, 0)
	return c
}

func (p *ProposeAndReadReplyCache) Get() *ProposeAndReadReply {
	var t *ProposeAndReadReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &ProposeAndReadReply{}
	}
	return t
}
func (p *ProposeAndReadReplyCache) Put(t *ProposeAndReadReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *ProposeAndReadReply) Marshal(wire io.Writer) {
	var b [5]byte
	var bs []byte
	bs = b[:5]
	bs[0] = byte(t.OK)
	tmp32 := t.CommandId
	bs[1] = byte(tmp32)
	bs[2] = byte(tmp32 >> 8)
	bs[3] = byte(tmp32 >> 16)
	bs[4] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Value.Marshal(wire)
}

func (t *ProposeAndReadReply) Unmarshal(wire io.Reader) error {
	var b [5]byte
	var bs []byte
	bs = b[:5]
	if _, err := io.ReadAtLeast(wire, bs, 5); err != nil {
		return err
	}
	t.OK = uint8(bs[0])
	t.CommandId = int32((uint32(bs[1]) | (uint32(bs[2]) << 8) | (uint32(bs[3]) << 16) | (uint32(bs[4]) << 24)))
	t.Value.Unmarshal(wire)
	return nil
}

func (t *Beacon) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type BeaconCache struct {
	mu    sync.Mutex
	cache []*Beacon
}

func NewBeaconCache() *BeaconCache {
	c := &BeaconCache{}
	c.cache = make([]*Beacon, 0)
	return c
}

func (p *BeaconCache) Get() *Beacon {
	var t *Beacon
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Beacon{}
	}
	return t
}
func (p *BeaconCache) Put(t *Beacon) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Beacon) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp64 := t.Timestamp
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

func (t *Beacon) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Timestamp = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	return nil
}
