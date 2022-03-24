package shardkv

import "fmt"

type clntIdOpCtxMap   map[int64]*OpContext // (key: clientId, val: lastAppliedOpContext)

type Shard struct {
	Num           int
	Stat          ShardStat
	Storage       KVMap
	LastClntOpSet clntIdOpCtxMap
}

func NewShard(num int) Shard {
	return Shard{
		Num:           num,
		Stat:          Available,
		Storage:       make(KVMap),
		LastClntOpSet: make(clntIdOpCtxMap),
	}
}

func (s *Shard) String() string {
	return fmt.Sprintf("[n:%v %v stor:%v]",
		s.Num, s.Stat, s.Storage)
}

func (s *Shard) isDuplicated(clntId, seqId int64) (*OpContext, bool) {
	lastAppliedOp, ok := s.LastClntOpSet[clntId]
	return lastAppliedOp, ok && seqId <= lastAppliedOp.Seq
}

