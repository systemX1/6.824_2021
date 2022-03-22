package shardkv

import (
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
	ClerkWrongGroupInterval  = 50 * time.Millisecond
	ClerkWrongLeaderInterval = ClerkWrongGroupInterval
	ServerApplyTimeout       = 2000 * time.Millisecond
	ServerSnapshotInterval   = 200 * time.Millisecond
	clntIdDebugMod           = 100000
)

type Err string
const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrWrongGroup  Err = "ErrWrongGroup"
	ErrTimeout     Err = "ErrTimeout"
)

type OPType string
const (
	OPGet    OPType = "Get"
	OPPut    OPType = "Put"
	OPAppend OPType = "Append"
)

// OpArgs Put or Append
type OpArgs struct {
	Key    string
	Value  string
	OpType OPType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq	   int64
	Clnt   int64
}

func (op *OpArgs) String() string {
	return fmt.Sprintf("[Op %v k:%v v:%v seq:%v clnt:%v]",
		op.OpType, op.Key, op.Value, op.Seq, op.Clnt % clntIdDebugMod,
	)
}

type OpReply struct {
	Err   Err
	Value string
}

func (reply *OpReply) String() string {
	return fmt.Sprintf("[rly %v v:%v]",
		reply.Err, reply.Value,
	)
}