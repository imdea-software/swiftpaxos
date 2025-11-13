package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"log"
)

import (
	"sync"
	"unsafe"

	"github.com/imdea-software/swiftpaxos/client"
)

var (
	mu         sync.Mutex
	clients          = map[int64]*client.BufferClient{}
	nextHandle int64 = 1
)

func goBool(i C.int) bool { return i != 0 }

//export Client_New
func Client_New(cserver, cmaddr *C.char, cmport C.int, cfast, cleaderless, cverbose C.int) C.longlong {
	server := C.GoString(cserver)
	maddr := C.GoString(cmaddr)
	mport := int(cmport)
	fast := goBool(cfast)
	leaderless := goBool(cleaderless)
	verbose := goBool(cverbose)

	cl := client.NewClient(server, maddr, mport, fast, leaderless, verbose)
	b := client.NewBufferClient(cl, 0, 0, 0, 0, 0)

	mu.Lock()
	h := nextHandle
	nextHandle++
	clients[h] = b
	mu.Unlock()
	return C.longlong(h)
}

//export Client_Connect
func Client_Connect(h C.longlong) {
	mu.Lock()
	cl := clients[int64(h)]
	mu.Unlock()
	if cl == nil {
		log.Fatal("Client does not exist.")
	}
	if err := cl.Connect(); err != nil {
		log.Fatal("Cannot connect.")
	}
}

//export Client_Disconnect
func Client_Disconnect(h C.longlong) {
	mu.Lock()
	cl := clients[int64(h)]
	delete(clients, int64(h))
	mu.Unlock()
	if cl != nil {
		cl.Disconnect()
	}
}

//export Client_Reconnect
func Client_Reconnect(h C.longlong) {
	mu.Lock()
	cl := clients[int64(h)]
	mu.Unlock()
	if cl == nil {
		log.Fatal("Client does not exist.")
	}
	if err := cl.Reconnect(); err != nil {
		log.Fatal("Cannot reconnect.")
	}
}

//export Client_SendWrite
func Client_SendWrite(h C.longlong, key C.longlong, data *C.char, length C.int) {
	mu.Lock()
	cl := clients[int64(h)]
	mu.Unlock()
	if cl == nil {
		log.Fatal("Client does not exist.")
	}
	goBytes := C.GoBytes(unsafe.Pointer(data), length) // FIXME copies into Go heap
	cl.Write(int64(key), goBytes)
}

//export Client_SendRead
func Client_SendRead(h C.longlong, key C.longlong, outBuf unsafe.Pointer, maxLen C.int) C.int {
	cl := clients[int64(h)]
	if cl == nil {
		log.Fatal("Client does not exist.")
	}
	b := cl.Read(int64(key))
	if len(b) > int(maxLen) {
		log.Fatal("Buffer is too small.")
	}
	dst := unsafe.Slice((*byte)(outBuf), int(maxLen))
	copy(dst, b)
	return C.int(len(b))
}

//export Client_SendScan
func Client_SendScan(h C.longlong, key C.longlong, count C.longlong, outBuf unsafe.Pointer, maxLen C.int) C.int {
	mu.Lock()
	cl := clients[int64(h)]
	mu.Unlock()
	if cl == nil {
		log.Fatal("Client does not exist.")
	}
	b := cl.Scan(int64(key), int64(count))
	if len(b) > int(maxLen) {
		log.Fatal("Buffer is too small.")
	}
	dst := unsafe.Slice((*byte)(outBuf), int(maxLen))
	copy(dst, b)
	return C.int(len(b))
}

//export Client_FreeBuffer
func Client_FreeBuffer(p *C.char) {
	if p == nil {
		return
	}
	C.free(unsafe.Pointer(p))
}

func main() {
	// required for -buildmode=c-shared
}
