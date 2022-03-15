package kvraft

import (
	"fmt"
	"time"
)

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
)

const (
	ClerkRetryTimeout   = 200 * time.Millisecond
	ClerkWLeaderTimeout = 100 * time.Millisecond
	ServerApplyTimeout  = 2000 * time.Millisecond
	clntIdDebugMod      = 1000000
)

type Err string

// OpArgs Put or Append
type OpArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq	  int64
	Clnt  int64
}

func (op *OpArgs) String() string {
	return fmt.Sprintf("[Op %v k:%v v:%v seq:%v clnt:%v]",
		op.Op, op.Key, op.Value, op.Seq, op.Clnt % clntIdDebugMod,
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
