package replica

type MsgSetHandler func(interface{}, []interface{})

type MsgSet struct {
	q         QuorumI
	msgs      map[int32]interface{}
	leaderMsg interface{}
	accept    func(interface{}, interface{}) bool
	freeMsg   func(interface{})
	handler   MsgSetHandler
}

func NewMsgSet(q QuorumI, accept func(interface{}, interface{}) bool,
	freeMsg func(interface{}), handler MsgSetHandler) *MsgSet {

	return &MsgSet{
		q:         q,
		msgs:      map[int32]interface{}{},
		leaderMsg: nil,
		accept:    accept,
		freeMsg:   freeMsg,
		handler:   handler,
	}
}

func (ms *MsgSet) ReinitMsgSet(q QuorumI, accept func(interface{}, interface{}) bool,
	freeMsg func(interface{}), handler MsgSetHandler) *MsgSet {

	if ms == nil {
		return NewMsgSet(q, accept, freeMsg, handler)
	}

	ms.q = q
	ms.leaderMsg = nil
	ms.accept = accept
	ms.freeMsg = freeMsg
	ms.handler = handler
	for rep := range ms.msgs {
		delete(ms.msgs, rep)
	}
	return ms
}

func (ms *MsgSet) Add(repId int32, isLeader bool, msg interface{}) bool {
	if !ms.q.Contains(repId) {
		return false
	}

	if _, seen := ms.msgs[repId]; ms.leaderMsg != nil && seen {
		return false
	}

	added := false

	if isLeader {
		ms.leaderMsg = msg
		newMsgs := map[int32]interface{}{}
		for frepId, fmsg := range ms.msgs {
			if _, seen := newMsgs[frepId]; !seen && ms.accept(fmsg, ms.leaderMsg) {
				newMsgs[frepId] = fmsg
			} else {
				ms.freeMsg(fmsg)
			}
		}
		ms.msgs = newMsgs
		added = true
	} else if ms.accept(msg, ms.leaderMsg) {
		ms.msgs[repId] = msg
		added = true
	} else {
		ms.freeMsg(msg)
	}

	if len(ms.msgs) >= ms.q.Size() ||
		(len(ms.msgs) >= ms.q.Size()-1 && ms.leaderMsg != nil) {
		m := make([]interface{}, len(ms.msgs))
		i := 0
		for _, v := range ms.msgs {
			m[i] = v
			i++
		}
		ms.handler(ms.leaderMsg, m)
	}

	return added
}

func (ms *MsgSet) Free() {
	if ms == nil {
		return
	}
	if ms.leaderMsg != nil {
		ms.freeMsg(ms.leaderMsg)
	}
	for _, fmsg := range ms.msgs {
		if fmsg != nil {
			ms.freeMsg(fmsg)
		}
	}
}
