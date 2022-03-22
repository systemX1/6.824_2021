package kvraft

import (
	"fmt"
	"time"
)

const (
	ClerkRetryTimeout      = 100 * time.Millisecond
	ClerkWLeaderTimeout    = 50 * time.Millisecond
	ServerApplyTimeout     = 2000 * time.Millisecond
	ServerSnapshotInterval = 200 * time.Millisecond
	clntIdDebugMod         = 100000
)

type Err string
const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
)

type OPType string
const (
	GET    OPType = "Get"
	PUT    OPType = "Put"
	APPEND OPType = "Append"
)

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
