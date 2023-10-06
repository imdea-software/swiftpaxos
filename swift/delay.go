package swift

import "time"

const (
	BAD_CONT   = 3
	THRESHOLD  = time.Duration(210 * time.Millisecond)
	PING_DELAY = time.Duration(10 * time.Millisecond)
)

type DelayEntry struct {
	now      time.Time
	badCount int
}

type SwapValue struct {
	oldFast int32
	newFast int32
}

type DelayLog struct {
	id            int32
	log           []DelayEntry
	swap          chan SwapValue
	ping          MPing
	ballot        int32
	lastValue     SwapValue
	fastestSlowD  time.Duration
	fastestSlowId int32
}

func NewDelayLog(r *Replica) *DelayLog {
	dl := &DelayLog{
		id:            r.Id,
		log:           make([]DelayEntry, r.N),
		swap:          make(chan SwapValue, 2),
		ping:          MPing{Replica: r.Id, Ballot: r.ballot},
		ballot:        r.ballot,
		lastValue:     SwapValue{oldFast: -1, newFast: -1},
		fastestSlowId: -1,
	}
	for i := range dl.log {
		dl.log[i].badCount = -1
	}
	return dl
}

func (dl *DelayLog) Reinit(r *Replica) {
	if dl == nil {
		return
	}

	dl.swap = make(chan SwapValue, 2)
	dl.ballot = r.ballot
	dl.fastestSlowId = -1
	dl.ping.Ballot = r.ballot
	r.sender.SendToAll(&dl.ping, r.cs.pingRPC)
}

func (dl *DelayLog) Tick(id int32, fast bool) int32 {
	if dl == nil {
		return -1
	}

	i := int32(id)

	if dl.log[i].badCount == -1 {
		dl.log[i].badCount = 0
		dl.log[i].now = time.Now()
		return id
	}

	now := time.Now()
	d := now.Sub(dl.log[i].now)
	dl.log[i].now = now

	if d > THRESHOLD+PING_DELAY {
		dl.log[i].badCount++
	} else if dl.log[i].badCount > 0 {
		dl.log[i].badCount--
	}

	if fast {
		if dl.log[i].badCount == BAD_CONT && dl.fastestSlowId != -1 {
			return dl.fastestSlowId
		}
		if dl.lastValue.newFast == id && dl.log[dl.lastValue.oldFast].badCount == 0 {
			return dl.lastValue.oldFast
		}
	}
	if !fast && (d < dl.fastestSlowD || dl.fastestSlowId == -1) {
		dl.fastestSlowId = id
	}
	return id
}

func (dl *DelayLog) BTick(ballot, id int32, fast bool) {
	if dl == nil || dl.id == id || dl.ballot != ballot {
		return
	}
	nid := dl.Tick(id, fast)
	if nid != id {
		dl.lastValue = SwapValue{
			oldFast: id,
			newFast: nid,
		}
		dl.swap <- dl.lastValue
	}
}
