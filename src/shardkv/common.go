package shardkv

import (
	"../shardctrler"
	"fmt"
	"time"
)

// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.

const (
	ClerkRetryTimeout        = 20 * time.Millisecond
	ClerkWrongGroupInterval  = 20 * time.Millisecond
	ClerkWrongLeaderInterval = ClerkWrongGroupInterval
	ServerApplyTimeout       = 2000 * time.Millisecond
	ServerSnapshotInterval   = 200 * time.Millisecond
	ServerPullConfigInterval = 100 * time.Millisecond
	clntIdDebugMod           = 100000
)

type Err uint8
const (
	OK             Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrWrongGroup
	ErrShardPreparing
	ErrTimeout
)
var errStr = []string{ "OK", "ErrNoKey", "ErrWrongLeader", "ErrWrongGroup", "ErrTimeout" }
func (e Err) String() string {
	return errStr[e]
}

type OPType uint8
const (
	OPPullConf OPType = iota
	OPKV
	OPAddShard
	OPDelShard
	NOOP
)
var opTypeStr = []string{ "OPPullConf", "OPKV", "OPAddShard", "OPDelShard", "NOOP" }
func (o OPType) String() string {
	return opTypeStr[o]
}

type OPKVType uint8
const (
	OPGet    OPKVType = iota
	OPPut
	OPAppend
)
var opKvTypeStr = []string{ "Get", "Put", "Append" }
func (o OPKVType) String() string {
	return opKvTypeStr[o]
}

type ShardStat uint8
const (
	Available    ShardStat = iota
	Preparing
	Removing
)
var shardStatStr = []string{ "Available", "Preparing", "Removing" }
func (s ShardStat) String() string {
	return shardStatStr[s]
}

type OpContext struct {
	OpArgs
	OpReply
}
func (oc *OpContext) String() string {
	return fmt.Sprintf("\n[CTX %v %v]",
		&oc.OpArgs, &oc.OpReply)
}

// OpArgs Put or Append
type OpArgs struct {
	OpType   OPType
	OpKVType OPKVType
	Key      string
	Value    string
	shardctrler.Config
	Shard
	Seq	     int64
	Clnt     int64
}

func (op *OpArgs) String() string {
	switch op.OpType {
	case OPKV:
		return fmt.Sprintf("\n[%v %v k:%v v:%v seq:%v clnt:%v]",
			op.OpType, op.OpKVType, op.Key, op.Value, op.Seq, op.Clnt % clntIdDebugMod,
		)
	case OPPullConf:
		return fmt.Sprintf("\n[%v cfg:%v]",
			op.OpType, op.Config,
		)
	default:
		return fmt.Sprintf("\n[NO-OP]")
	}
}

type OpReply struct {
	RlyErr Err
	RlyVal string
}

func (reply *OpReply) String() string {
	return fmt.Sprintf("\n[REPLY %v v:%v]",
		reply.RlyErr, reply.RlyVal,
	)
}

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

