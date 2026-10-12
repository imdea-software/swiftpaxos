package state

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestRowEncoding(t *testing.T) {
	r1 := Row{"b": []byte("2"), "a": []byte("1"), "": nil}
	r2 := Row{"x": bytes.Repeat([]byte("y"), 300)}
	got, err := DecodeRow(r1.Encode())
	if err != nil || len(got) != 3 || string(got["a"]) != "1" || string(got["b"]) != "2" {
		t.Fatal(got, err)
	}
	if !bytes.Equal(r1.Encode(), got.Encode()) {
		t.Fatal("the encoding is not deterministic")
	}
	rows, err := DecodeRows(append(r1.Encode(), r2.Encode()...))
	if err != nil || len(rows) != 2 || len(rows[1]["x"]) != 300 {
		t.Fatal(rows, err)
	}
	if _, err := DecodeRow([]byte("junk")); err == nil {
		t.Fatal("decoded junk")
	}
}

func TestTxnEncoding(t *testing.T) {
	cmds := []Command{{PUT, 1, Value("a")}, {GET, -2, NIL()}, {INCR, 3, IncrArg("c", -5)}}
	got, err := DecodeTxn(EncodeTxn(cmds))
	if err != nil || len(got) != 3 || got[1].K != -2 || got[0].Op != PUT || string(got[0].V) != "a" {
		t.Fatal(got, err)
	}
	vals, err := DecodeValues(EncodeValues([]Value{Value("x"), NIL()}))
	if err != nil || len(vals) != 2 || string(vals[0]) != "x" || len(vals[1]) != 0 {
		t.Fatal(vals, err)
	}
	if _, err := DecodeTxn(Value("junk")); err == nil {
		t.Fatal("decoded junk")
	}
}

func TestColumnOps(t *testing.T) {
	st := InitState()
	exec := func(op Operation, k Key, v Value) Value {
		c := Command{op, k, v}
		return c.Execute(st)
	}
	exec(UPDATE, 1, Row{"a": []byte("x"), "n": []byte("7")}.Encode())
	exec(UPDATE, 1, Row{"b": []byte("y")}.Encode())
	if v := exec(INCR, 1, IncrArg("n", 3)); string(v) != "10" {
		t.Fatalf("INCR returned %q", v)
	}
	if v := exec(INCR, 2, IncrArg("n", -1)); string(v) != "-1" {
		t.Fatalf("INCR on an absent key returned %q", v)
	}
	row, err := DecodeRow(exec(GET, 1, NIL()))
	if err != nil || string(row["a"]) != "x" || string(row["b"]) != "y" || string(row["n"]) != "10" {
		t.Fatal(row, err)
	}
	// a PUT value that is not a row is read as an empty row
	exec(PUT, 3, Value("raw"))
	if v := exec(INCR, 3, IncrArg("n", 1)); string(v) != "1" {
		t.Fatalf("INCR on a raw value returned %q", v)
	}
	count := make([]byte, 8)
	binary.LittleEndian.PutUint64(count, 10)
	if rows, err := DecodeRows(exec(SCAN, 0, count)); err != nil || len(rows) != 3 {
		t.Fatal(rows, err)
	}
	exec(DELETE, 1, NIL())
	if v := exec(GET, 1, NIL()); len(v) != 0 {
		t.Fatalf("deleted key returned %q", v)
	}
}

func TestConflicts(t *testing.T) {
	incr := Command{INCR, 1, IncrArg("n", 1)}
	get := Command{GET, 1, NIL()}
	get2 := Command{GET, 1, NIL()}
	if !Conflict(&incr, &get) || Conflict(&get, &get2) {
		t.Fatal("bad conflict relation")
	}
	for _, op := range []Operation{PUT, UPDATE, INCR, DELETE} {
		if !IsWrite(op) {
			t.Fatal(op)
		}
	}
	if IsWrite(GET) || IsWrite(SCAN) || IsWrite(TXN) {
		t.Fatal("reads are not writes")
	}
}

func TestLargeValue(t *testing.T) {
	v := Value(bytes.Repeat([]byte("z"), 100000))
	var b bytes.Buffer
	v.Marshal(&b)
	var got Value
	if err := got.Unmarshal(&b); err != nil || !bytes.Equal(got, v) {
		t.Fatal("values larger than 64KB are truncated")
	}
}
