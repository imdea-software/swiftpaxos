package swift

import (
	"encoding/binary"

	"github.com/imdea-software/swiftpaxos/state"
)

type keyInfo interface {
	add(state.Command, CommandId)
	remove(state.Command, CommandId)
	getConflictCmds(cmd state.Command) []CommandId
}

type fullKeyInfo struct {
	clientLastWrite []CommandId
	clientLastCmd   []CommandId
	lastWriteIndex  map[int32]int
	lastCmdIndex    map[int32]int
}

func newFullKeyInfo() *fullKeyInfo {
	return &fullKeyInfo{
		clientLastWrite: []CommandId{},
		clientLastCmd:   []CommandId{},
		lastWriteIndex:  make(map[int32]int),
		lastCmdIndex:    make(map[int32]int),
	}
}

func keysOf(cmd state.Command) []state.Key {
	switch cmd.Op {
	case state.SCAN:
		count := binary.LittleEndian.Uint64(cmd.V)
		ks := make([]state.Key, count)
		for i := range ks {
			ks[i] = cmd.K + state.Key(i)
		}
		return ks
	default:
		return []state.Key{cmd.K}
	}
}

func (ki *fullKeyInfo) add(cmd state.Command, cmdId CommandId) {
	cmdIndex, exists := ki.lastCmdIndex[cmdId.ClientId]

	if exists {
		ki.clientLastCmd[cmdIndex] = cmdId
	} else {
		ki.lastCmdIndex[cmdId.ClientId] = len(ki.clientLastCmd)
		ki.clientLastCmd = append(ki.clientLastCmd, cmdId)
	}

	if cmd.Op == state.PUT {
		writeIndex, exists := ki.lastWriteIndex[cmdId.ClientId]

		if exists {
			ki.clientLastWrite[writeIndex] = cmdId
		} else {
			ki.lastWriteIndex[cmdId.ClientId] = len(ki.clientLastWrite)
			ki.clientLastWrite = append(ki.clientLastWrite, cmdId)
		}
	}
}

func (ki *fullKeyInfo) remove(cmd state.Command, cmdId CommandId) {
	cmdIndex, exists := ki.lastCmdIndex[cmdId.ClientId]

	if exists {
		lastCmdIndex := len(ki.clientLastCmd) - 1
		lastCmdId := ki.clientLastCmd[lastCmdIndex]
		ki.lastCmdIndex[lastCmdId.ClientId] = cmdIndex
		ki.clientLastCmd[cmdIndex] = lastCmdId
		ki.clientLastCmd = ki.clientLastCmd[0:lastCmdIndex]
		delete(ki.lastCmdIndex, cmdId.ClientId)
	}

	if cmd.Op == state.PUT {
		writeIndex, exists := ki.lastWriteIndex[cmdId.ClientId]

		if exists {
			lastWriteIndex := len(ki.clientLastWrite) - 1
			lastWriteId := ki.clientLastWrite[lastWriteIndex]
			ki.lastWriteIndex[lastWriteId.ClientId] = writeIndex
			ki.clientLastWrite[writeIndex] = lastWriteId
			ki.clientLastWrite = ki.clientLastWrite[0:lastWriteIndex]
			delete(ki.lastWriteIndex, cmdId.ClientId)
		}
	}
}

func (ki *fullKeyInfo) getConflictCmds(cmd state.Command) []CommandId {
	if cmd.Op == state.GET {
		return ki.clientLastWrite
	} else {
		return ki.clientLastCmd
	}
}

type lightKeyInfo struct {
	lastWrite []CommandId
	lastCmd   []CommandId
}

func newLightKeyInfo() *lightKeyInfo {
	return &lightKeyInfo{
		lastWrite: []CommandId{},
		lastCmd:   []CommandId{},
	}
}

func (ki *lightKeyInfo) add(cmd state.Command, cmdId CommandId) {
	ki.lastCmd = []CommandId{cmdId}

	if cmd.Op == state.PUT {
		ki.lastWrite = []CommandId{cmdId}
	}
}

func (ki *lightKeyInfo) remove(_ state.Command, cmdId CommandId) {
	if len(ki.lastCmd) > 0 && ki.lastCmd[0] == cmdId {
		ki.lastCmd = []CommandId{}
	}

	if len(ki.lastWrite) > 0 && ki.lastWrite[0] == cmdId {
		ki.lastWrite = []CommandId{}
	}
}

func (ki *lightKeyInfo) getConflictCmds(cmd state.Command) []CommandId {
	if cmd.Op == state.GET {
		return ki.lastWrite
	} else {
		return ki.lastCmd
	}
}
