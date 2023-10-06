package rpc

import "io"

type Serializable interface {
	Marshal(io.Writer)
	Unmarshal(io.Reader) error
	New() Serializable
}

type Pair struct {
	Obj  Serializable
	Chan chan Serializable
}

type Table struct {
	id    uint8
	pairs map[uint8]Pair
}

func NewTable() *Table {
	return &Table{
		id:    0,
		pairs: make(map[uint8]Pair),
	}
}

func NewTableId(id uint8) *Table {
	return &Table{
		id:    id,
		pairs: make(map[uint8]Pair),
	}
}

func (t *Table) Register(obj Serializable, notify chan Serializable) uint8 {
	id := t.id
	t.id++
	t.pairs[id] = Pair{
		Obj:  obj,
		Chan: notify,
	}
	return id
}

func (t *Table) Get(id uint8) (Pair, bool) {
	p, exists := t.pairs[id]
	return p, exists
}
