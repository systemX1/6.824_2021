package shardkv

import (
	"fmt"
	"strings"
)

type KVMap            map[string]string
type clntIdOpCtxMap   map[int64]*OpContext // (key: clientId, val: lastAppliedOpContext)

func (kv *KVMap) String() string {
	var sb strings.Builder
	sb.WriteString("m[")
	for k := range *kv {
		sb.WriteString(k)
		sb.WriteByte(':')
		if len((*kv)[k]) >= 3 {
			sb.WriteString((*kv)[k][:3])
		} else {
		sb.WriteString((*kv)[k])
		}
		sb.WriteByte(' ')
	}
	sb.WriteString("]")
	return sb.String()
}

type Shard struct {
	Num           int
	Stat          ShardStat
	Storage       KVMap
	LastClntOpMap clntIdOpCtxMap
}

func NewShard(num int, stat ShardStat) *Shard {
	return &Shard{
		Num:           num,
		Stat:          stat,
		Storage:       make(KVMap),
		LastClntOpMap: make(clntIdOpCtxMap),
	}
}

func (s *Shard) String() string {
	return fmt.Sprintf("[n:%v %v stor:%v]",
		s.Num, s.Stat, &s.Storage)
}

func (s *Shard) isDuplicated(clntId, seqId int64) (*OpContext, bool) {
	lastAppliedOp, ok := s.LastClntOpMap[clntId]
	return lastAppliedOp, ok && seqId <= lastAppliedOp.Seq
}

