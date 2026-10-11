package janus

import "github.com/imdea-software/swiftpaxos/state"

// Sharding layout: replica ids [s*k, (s+1)*k) form shard s (k = shard size).
type Layout struct {
	N         int // total number of replicas
	ShardSize int // replicas per shard
}

func NewLayout(n, shardSize int) Layout {
	if shardSize <= 0 || shardSize > n {
		shardSize = n
	}
	return Layout{n, shardSize}
}

func (l Layout) NumShards() int { return l.N / l.ShardSize }

// ShardOfKey maps a key to the shard replicating it.
func (l Layout) ShardOfKey(k state.Key) int {
	m := int64(l.NumShards())
	return int(((int64(k) % m) + m) % m)
}

// ShardOfReplica returns the shard that replica r belongs to.
func (l Layout) ShardOfReplica(r int32) int { return int(r) / l.ShardSize }

// Replicas returns the ids of the replicas of shard s.
func (l Layout) Replicas(s int) []int32 {
	ids := make([]int32, l.ShardSize)
	for i := range ids {
		ids[i] = int32(s*l.ShardSize + i)
	}
	return ids
}

// F is the number of tolerated failures within a shard.
func (l Layout) F() int { return (l.ShardSize - 1) / 2 }

// Majority is the size of a slow (Accept) quorum within a shard.
func (l Layout) Majority() int { return l.ShardSize/2 + 1 }

// FastQuorum is the number of identical PreAccept replies needed on a shard
// to take the fast path. In Janus, it is every replica of the shard (Section
// 3.1 of the paper), so that a majority always witnesses a fast-path decision.
func (l Layout) FastQuorum() int { return l.ShardSize }

// ShardsOf returns the (sorted, distinct) shards touched by a transaction.
// A SCAN touches all shards.
func (l Layout) ShardsOf(cmds []state.Command) []int {
	seen := make([]bool, l.NumShards())
	for _, c := range cmds {
		if c.Op == state.SCAN {
			for i := range seen {
				seen[i] = true
			}
			break
		}
		seen[l.ShardOfKey(c.K)] = true
	}
	res := []int{}
	for s, ok := range seen {
		if ok {
			res = append(res, s)
		}
	}
	return res
}
