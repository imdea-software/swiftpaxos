package state

import (
	"encoding/binary"
	"errors"
	"sort"
	"strconv"
)

// Column model. Besides opaque values (PUT/GET/SCAN), a value can be a row,
// i.e., a set of named columns. UPDATE merges columns into a row and INCR adds
// to a counter column, which holds a signed decimal number (as in YCSB, where
// fields are strings). A value that is not a valid row is read as an empty row.
//
// Encodings (little endian, lengths on 4 bytes):
//   - row:          ncols, then (len, name, len, value) per column, sorted by name
//   - INCR command: delta (8 bytes, signed), then the column name
//   - TXN command:  ncmds, then (op (1 byte), key (8 bytes), len, value) per command
//   - results:      nvals, then (len, value) per value
//
// Rows are self-delimiting: the result of a SCAN over rows (the concatenation
// of the scanned values) is decoded with DecodeRows.

// Row is a set of named columns.
type Row map[string][]byte

var errEncoding = errors.New("state: malformed encoding")

type wbuf []byte

func (b *wbuf) u32(v int) {
	*b = binary.LittleEndian.AppendUint32(*b, uint32(v))
}

func (b *wbuf) bytes(v []byte) {
	b.u32(len(v))
	*b = append(*b, v...)
}

type rbuf struct {
	b   []byte
	err error
}

func (r *rbuf) take(n int) []byte {
	if r.err != nil || n < 0 || n > len(r.b) {
		r.err = errEncoding
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func (r *rbuf) u32() int {
	v := r.take(4)
	if v == nil {
		return 0
	}
	return int(binary.LittleEndian.Uint32(v))
}

func (r *rbuf) bytes() []byte {
	return r.take(r.u32())
}

// Encode returns the encoding of the row (columns sorted by name).
func (row Row) Encode() Value {
	names := make([]string, 0, len(row))
	for n := range row {
		names = append(names, n)
	}
	sort.Strings(names)
	b := make(wbuf, 0, 64)
	b.u32(len(names))
	for _, n := range names {
		b.bytes([]byte(n))
		b.bytes(row[n])
	}
	return Value(b)
}

func decodeRow(r *rbuf) Row {
	n := r.u32()
	if r.err != nil || n > len(r.b) {
		r.err = errEncoding
		return nil
	}
	row := make(Row, n)
	for i := 0; i < n && r.err == nil; i++ {
		name := r.bytes()
		row[string(name)] = append([]byte(nil), r.bytes()...)
	}
	return row
}

// DecodeRow decodes a row.
func DecodeRow(v []byte) (Row, error) {
	r := &rbuf{b: v}
	row := decodeRow(r)
	if r.err == nil && len(r.b) != 0 {
		r.err = errEncoding
	}
	return row, r.err
}

// DecodeRows decodes a sequence of rows (e.g., the result of a SCAN).
func DecodeRows(v []byte) ([]Row, error) {
	r := &rbuf{b: v}
	var rows []Row
	for len(r.b) > 0 && r.err == nil {
		rows = append(rows, decodeRow(r))
	}
	return rows, r.err
}

// rowOf reads a stored value as a row.
func rowOf(v Value) Row {
	if row, err := DecodeRow(v); err == nil {
		return row
	}
	return Row{}
}

// IncrArg returns the argument of an INCR command.
func IncrArg(column string, delta int64) Value {
	b := binary.LittleEndian.AppendUint64(make([]byte, 0, 8+len(column)), uint64(delta))
	return Value(append(b, column...))
}

func decodeIncr(v Value) (string, int64, error) {
	if len(v) < 8 {
		return "", 0, errEncoding
	}
	return string(v[8:]), int64(binary.LittleEndian.Uint64(v)), nil
}

// increment adds delta to a counter column (absent or not a number: 0) and
// returns the new value.
func increment(row Row, column string, delta int64) Value {
	cur, _ := strconv.ParseInt(string(row[column]), 10, 64)
	v := Value(strconv.AppendInt(nil, cur+delta, 10))
	row[column] = v
	return v
}

// EncodeTxn returns the argument of a TXN command.
func EncodeTxn(cmds []Command) Value {
	b := make(wbuf, 0, 64)
	b.u32(len(cmds))
	for _, c := range cmds {
		b = append(b, byte(c.Op))
		b = binary.LittleEndian.AppendUint64(b, uint64(c.K))
		b.bytes(c.V)
	}
	return Value(b)
}

// DecodeTxn decodes the argument of a TXN command.
func DecodeTxn(v Value) ([]Command, error) {
	r := &rbuf{b: v}
	n := r.u32()
	if r.err != nil || n > len(r.b) {
		return nil, errEncoding
	}
	cmds := make([]Command, 0, n)
	for i := 0; i < n && r.err == nil; i++ {
		op := r.take(1)
		k := r.take(8)
		val := r.bytes()
		if r.err == nil {
			cmds = append(cmds, Command{Operation(op[0]), Key(binary.LittleEndian.Uint64(k)), append(Value(nil), val...)})
		}
	}
	if r.err == nil && len(r.b) != 0 {
		r.err = errEncoding
	}
	return cmds, r.err
}

// EncodeValues encodes the results of a transaction.
func EncodeValues(vals []Value) Value {
	b := make(wbuf, 0, 64)
	b.u32(len(vals))
	for _, v := range vals {
		b.bytes(v)
	}
	return Value(b)
}

// DecodeValues decodes the results of a transaction.
func DecodeValues(v []byte) ([]Value, error) {
	r := &rbuf{b: v}
	n := r.u32()
	if r.err != nil || n > len(r.b) {
		return nil, errEncoding
	}
	vals := make([]Value, 0, n)
	for i := 0; i < n && r.err == nil; i++ {
		vals = append(vals, Value(append([]byte{}, r.bytes()...)))
	}
	if r.err == nil && len(r.b) != 0 {
		r.err = errEncoding
	}
	return vals, r.err
}
