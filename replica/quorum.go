package replica

import (
	"bufio"
	"errors"
	"os"
	"sort"
	"strings"
)

type QuorumI interface {
	Size() int
	Contains(int32) bool
}

type Majority int

func NewMajorityOf(N int) Majority {
	return Majority(N/2 + 1)
}

func (m Majority) Size() int {
	return int(m)
}

func (m Majority) Contains(int32) bool {
	return true
}

type ThreeQuarters int

func NewThreeQuartersOf(N int) ThreeQuarters {
	return ThreeQuarters((3*N)/4 + 1)
}

func (m ThreeQuarters) Size() int {
	return int(m)
}

func (m ThreeQuarters) Contains(int32) bool {
	return true
}

type Quorum map[int32]struct{}

type QuorumsOfLeader map[int32]Quorum

type QuorumSet map[int32]QuorumsOfLeader

var (
	NO_QUORUM_FILE = errors.New("Quorum file is not provided")
	THREE_QUARTERS = errors.New("ThreeQuarters")
)

func NewQuorum(size int) Quorum {
	return make(map[int32]struct{}, size)
}

func NewQuorumOfAll(size int) Quorum {
	q := NewQuorum(size)

	for i := int32(0); i < int32(size); i++ {
		q[i] = struct{}{}
	}

	return q
}

func (q Quorum) Size() int {
	return len(q)
}

func (q Quorum) Contains(repId int32) bool {
	_, exists := q[repId]
	return exists
}

func (q Quorum) copy() Quorum {
	nq := NewQuorum(len(q))

	for cmdId := range q {
		nq[cmdId] = struct{}{}
	}

	return nq
}

func (q1 Quorum) Equals(q2 Quorum) bool {
	if len(q1) != len(q2) {
		return false
	}
	for r := range q1 {
		if !q2.Contains(r) {
			return false
		}
	}
	return true
}

type QuorumSystem struct {
	qs      QuorumSet
	ballots []int32
}

func NewQuorumSystem(quorumSize int, r *Replica, qfile string) (*QuorumSystem, error) {
	AQs, leaders, err := NewQuorumsFromFile(qfile, r)
	if err == NO_QUORUM_FILE {
		return &QuorumSystem{
			qs:      NewQuorumSet(quorumSize, r.N),
			ballots: nil,
		}, nil
	} else if err != nil && err != THREE_QUARTERS {
		return nil, err
	}

	sys := &QuorumSystem{
		ballots: nil,
	}
	ids := make(map[int32]int)
	qs := newQuorumSetAdvance(quorumSize, r.N, func(l, qid int32, q Quorum) {
		for i, leader := range leaders {
			if leader != l || !q.Equals(AQs[i]) {
				continue
			}
			ballot := qid*int32(r.N) + l
			ids[ballot] = i
			sys.ballots = append(sys.ballots, ballot)
		}
	})
	sort.Slice(sys.ballots, func(i, j int) bool {
		return ids[sys.ballots[i]] < ids[sys.ballots[j]]
	})

	sys.qs = qs
	return sys, err
}

func (sys *QuorumSystem) SameHigher(sameAs, higherThan int32) int32 {
	l := Leader(sameAs, len(sys.qs))
	k := higherThan / int32(len(sys.qs[l]))
	return sameAs + k*int32(len(sys.qs[l]))*int32(len(sys.qs))
}

func (sys *QuorumSystem) BallotAt(i int) int32 {
	if len(sys.ballots) > i {
		return sys.ballots[i]
	}
	return -1
}

func (sys *QuorumSystem) BallotOf(leader int32, q Quorum) int32 {
	return sys.qs.BallotOf(leader, q)
}

func (sys QuorumSystem) AQ(ballot int32) Quorum {
	return sys.qs.AQ(ballot)
}

func NewQuorumsFromFile(qfile string, r *Replica) ([]Quorum, []int32, error) {
	if qfile == "" {
		return nil, nil, NO_QUORUM_FILE
	}

	f, err := os.Open(qfile)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	i := 0
	leaders := []int32{0}
	AQs := []Quorum{NewQuorum(r.N/2 + 1)}
	s := bufio.NewScanner(f)
	for s.Scan() {
		id := int32(-1)
		isLeader := false
		addr := ""

		data := strings.Split(s.Text(), " ")
		if len(data) == 1 {
			if data[0] == "---" {
				i++
				leaders = append(leaders, 0)
				AQs = append(AQs, NewQuorum(r.N/2+1))
				continue
			} else if data[0] == "3/4" {
				err = THREE_QUARTERS
				continue
			}
			addr = data[0]
		} else {
			isLeader = true
			addr = data[1]
		}

		addr = r.Config.ReplicaAddrs[addr]

		for rid := int32(0); rid < int32(r.N); rid++ {
			paddr := strings.Split(r.PeerAddrList[rid], ":")[0]
			if addr == paddr {
				id = rid
				break
			}
		}

		if id != -1 {
			AQs[i][id] = struct{}{}
			if isLeader {
				leaders[i] = id
			}
		}
	}

	if serr := s.Err(); serr != nil {
		err = serr
	}
	return AQs, leaders, err
}

func NewQuorumsOfLeader() QuorumsOfLeader {
	return make(map[int32]Quorum)
}

func NewQuorumSet(quorumSize, repNum int) QuorumSet {
	return newQuorumSetAdvance(quorumSize, repNum, func(int32, int32, Quorum) {})
}

func newQuorumSetAdvance(quorumSize, repNum int, treat func(int32, int32, Quorum)) QuorumSet {
	ids := make([]int32, repNum)
	q := NewQuorum(quorumSize)
	qs := make(map[int32]QuorumsOfLeader, repNum)

	for id := range ids {
		ids[id] = int32(id)
		qs[int32(id)] = NewQuorumsOfLeader()
	}

	subsets(ids, repNum, quorumSize, 0, q, qs, treat)

	return qs
}

func (qs QuorumSet) AQ(ballot int32) Quorum {
	l := Leader(ballot, len(qs))
	lqs := qs[l]
	qid := (ballot / int32(len(qs))) % int32(len(lqs))
	return lqs[qid]
}

func (qs QuorumSet) BallotOf(leader int32, q Quorum) int32 {
	for qid, qj := range qs[leader] {
		if qj.Equals(q) {
			return qid*int32(len(qs)) + leader
		}
	}
	return -1
}

func subsets(ids []int32, repNum, quorumSize, i int, q Quorum,
	qs QuorumSet, treat func(int32, int32, Quorum)) {

	if quorumSize == 0 {
		for repId := int32(0); repId < int32(repNum); repId++ {
			length := int32(len(qs[repId]))
			_, exists := q[repId]
			if exists {
				qs[repId][length] = q.copy()
				treat(repId, length, q)
			}
		}
	}

	for j := i; j < repNum; j++ {
		q[ids[j]] = struct{}{}
		subsets(ids, repNum, quorumSize-1, j+1, q, qs, treat)
		delete(q, ids[j])
	}
}
