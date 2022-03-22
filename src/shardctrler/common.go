package shardctrler

import (
	"fmt"
	"time"
)

// Shard controller: assigns shards to replication groups.

// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.

// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).

// You will need to add fields to the RPC argument structs.


// NShards The number of shards.
const NShards = 10

// Config A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num     int               // config number
	Shards  []int             // shard -> gid
	Groups  map[int] []string // gid -> servers[]
}

func (cf *Config) String() string {
	return fmt.Sprintf("[cfg%v s:%v g:%v]",
		cf.Num, cf.Shards, cf.Groups,
	)
}

const (
	ClerkRetryTimeout       = 50 * time.Millisecond
	ClerkWLeaderTimeout     = 20 * time.Millisecond
	ServerApplyTimeout      = 2000 * time.Millisecond
	clntIdDebugMod          = 1000000
)

type Err string
const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
	ErrNoConfig    Err = "ErrNoConfig"
	ErrNoGID       Err = "ErrNoGID"
)

type OPType string
const (
	OPJoin  OPType = "Join"
	OPLeave OPType = "Leave"
	OPMove  OPType = "Move"
	OPQuery OPType = "Query"
)

type OpArgs struct {
	OpType  OPType				// Op type
	Servers map[int] []string 	// Join(servers): add a set of groups (gid -> server-list mapping)
	GIDs    []int				// Leave: delete a set of groups.
	Shard   int					// Move: hand off one shard
	GID     int					//   from current owner to gid.
	Num     int					// Query: fetch Config "num", or latest config if num==-1.
	Seq     int64
	ClntId  int64
}

func (op *OpArgs) String() string {
	switch op.OpType {
	case OPQuery:
		return fmt.Sprintf("[Op %v num:%v seq:%v clnt:%v]",
			op.OpType, op.Num, op.Seq, op.ClntId % clntIdDebugMod,
		)
	case OPMove:
		return fmt.Sprintf("[Op %v shard:%v GID:%v seq:%v clnt:%v]",
			op.OpType, op.Shard, op.GID, op.Seq, op.ClntId % clntIdDebugMod,
		)
	case OPJoin:
		return fmt.Sprintf("[Op %v servers:%v seq:%v clnt:%v]",
			op.OpType, op.Servers, op.Seq, op.ClntId % clntIdDebugMod,
		)
	case OPLeave:
		return fmt.Sprintf("[Op %v GIDs:%v seq:%v clnt:%v]",
			op.OpType, op.GIDs, op.Seq, op.ClntId % clntIdDebugMod,
		)
	}
	return "Error: Type Not Exist"
}

type OpReply struct {
	Err    Err
	Config Config
}

func (reply *OpReply) String() string {
	return fmt.Sprintf("[rly %v v:%v]",
		reply.Err, reply.Config,
	)
}

//type JoinArgs struct {
//	Servers map[int][]string // new GID -> servers mappings
//}
//
//type JoinReply struct {
//	WrongLeader bool
//	Err
//}
//
//type LeaveArgs struct {
//	GIDs []int
//}
//
//type LeaveReply struct {
//	WrongLeader bool
//	Err
//}
//
//type MoveArgs struct {
//	Shard int
//	GID   int
//}
//
//type MoveReply struct {
//	WrongLeader bool
//	Err
//}
//
//type QueryArgs struct {
//	Num int // desired config number
//}
//
//type QueryReply struct {
//	WrongLeader bool
//	Err
//	Config
//}