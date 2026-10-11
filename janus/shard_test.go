package janus

import (
	"testing"

	"github.com/imdea-software/swiftpaxos/state"
)

func TestLayout(t *testing.T) {
	l := NewLayout(6, 3)
	if l.NumShards() != 2 || l.ShardOfReplica(4) != 1 || l.F() != 1 {
		t.Fatal("bad layout")
	}
	if l.FastQuorum() != 3 || l.Majority() != 2 {
		t.Fatal("the fast quorum of Janus is every replica of the shard")
	}
	if NewLayout(5, 0).NumShards() != 1 {
		t.Fatal("0 must mean full replication")
	}
	if s := l.ShardsOf([]state.Command{{Op: state.PUT, K: 1}, {Op: state.GET, K: 3}}); len(s) != 1 || s[0] != 1 {
		t.Fatal(s)
	}
	if s := l.ShardsOf([]state.Command{{Op: state.SCAN, K: 1}}); len(s) != 2 {
		t.Fatal(s)
	}
}
