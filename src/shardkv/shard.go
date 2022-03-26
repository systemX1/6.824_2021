package shardkv

import "fmt"

type clntIdOpCtxMap   map[int64]*OpContext // (key: clientId, val: lastAppliedOpContext)

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

