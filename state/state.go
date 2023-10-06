package state

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/emirpasic/gods/maps/treemap"
)

type Operation uint8

const (
	NONE Operation = iota
	PUT
	GET
	SCAN
)

type Value []byte

func NIL() Value { return Value([]byte{}) }

type Key int64

type Command struct {
	Op Operation
	K  Key
	V  Value
}

type Id int64
type Phase int8

func NOOP() []Command { return []Command{{NONE, 0, NIL()}} }

type State struct {
	mutex *sync.Mutex
	Store *treemap.Map
}

func KeyComparator(a, b interface{}) int {
	aAsserted := a.(Key)
	bAsserted := b.(Key)
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

func concat(slices []Value) Value {
	var totalLen int
	for _, s := range slices {
		totalLen += len(s)
	}
	tmp := make([]byte, totalLen)
	var i int
	for _, s := range slices {
		i += copy(tmp[i:], s)
	}
	return tmp
}

func InitState() *State {
	return &State{new(sync.Mutex), treemap.NewWith(KeyComparator)}
}

func Conflict(gamma *Command, delta *Command) bool {
	key := gamma.K
	lb := delta.K
	ub := delta.K

	if gamma.Op == SCAN && delta.Op == SCAN {
		return false
	}

	if gamma.Op == SCAN {
		key = delta.K
		lb = gamma.K
		ub = gamma.K + Key(binary.LittleEndian.Uint64(gamma.V))
	} else if delta.Op == SCAN {
		ub = delta.K + Key(binary.LittleEndian.Uint64(delta.V))
	}

	if key >= lb && key <= ub {
		if gamma.Op == PUT || delta.Op == PUT {
			return true
		}
	}
	return false
}

func ConflictBatch(batch1 []Command, batch2 []Command) bool {
	for i := 0; i < len(batch1); i++ {
		for j := 0; j < len(batch2); j++ {
			if Conflict(&batch1[i], &batch2[j]) {
				return true
			}
		}
	}
	return false
}

func IsRead(command *Command) bool {
	return command.Op == GET
}

func (c *Command) Execute(st *State) Value {

	st.mutex.Lock()
	defer st.mutex.Unlock()

	switch c.Op {
	case PUT:
		st.Store.Put(c.K, c.V)

	case GET:
		if value, present := st.Store.Get(c.K); present {
			valAsserted := value.(Value)
			return valAsserted
		}

	case SCAN:
		found := make([]Value, 0)
		count := binary.LittleEndian.Uint64(c.V)
		it := st.Store.Select(func(index interface{}, value interface{}) bool {
			keyAsserted := index.(Key)
			return keyAsserted >= c.K && keyAsserted <= c.K+Key(count)
		}).Iterator()
		for it.Next() {
			valAsserted := it.Value().(Value)
			found = append(found, valAsserted)
		}
		ret := concat(found)
		return ret
	}

	return NIL()
}

func (t *Value) String() string {
	if t == nil || len(*t) == 0 {
		return "(void)"
	}
	return hex.EncodeToString(*t)
}

func (t *Key) String() string {
	return strconv.FormatInt(int64(*t), 16)
}

func (t *Command) String() string {
	ret := ""
	if t.Op == PUT {
		ret = "PUT( " + t.K.String() + " , " + t.V.String() + " )"
	} else if t.Op == GET {
		ret = "GET( " + t.K.String() + " )"
	} else if t.Op == SCAN {
		count := binary.LittleEndian.Uint64(t.V)
		ret = "SCAN( " + t.K.String() + " , " + fmt.Sprint(count) + " )"
	} else {
		ret = "UNKNOWN( " + t.V.String() + " , " + t.K.String() + " )"
	}
	return ret
}

func (t *Command) Marshal(w io.Writer) {
	t.Op.Marshal(w)
	t.K.Marshal(w)
	t.V.Marshal(w)
}

func (t *Command) Unmarshal(r io.Reader) error {

	err := t.Op.Unmarshal(r)
	if err != nil {
		return err
	}

	err = t.K.Unmarshal(r)
	if err != nil {
		return err
	}

	err = t.V.Unmarshal(r)
	if err != nil {
		return err
	}

	return nil
}

func (t *Operation) Marshal(w io.Writer) {
	bs := make([]byte, 1)
	bs[0] = byte(*t)
	w.Write(bs)
}

func (t *Operation) Unmarshal(r io.Reader) error {
	bs := make([]byte, 1)
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*t = Operation(bs[0])
	return nil
}

func (t *Key) Marshal(w io.Writer) {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, uint64(*t))
	w.Write(bs)
}

func (t *Key) Unmarshal(r io.Reader) error {
	bs := make([]byte, 8)
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*t = Key(binary.LittleEndian.Uint64(bs))
	return nil
}

func (t *Value) Marshal(w io.Writer) {
	bs := make([]byte, 4)
	if t == nil {
		binary.LittleEndian.PutUint16(bs, 0)
		w.Write(bs)
	} else {
		binary.LittleEndian.PutUint16(bs, uint16(len(*t)))
		w.Write(bs)
		w.Write(*t)
	}
}

func (t *Value) Unmarshal(r io.Reader) error {
	bs := make([]byte, 4)
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	len := binary.LittleEndian.Uint16(bs)
	bs = make([]byte, len)
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*t = Value(bs)
	return nil
}
