package shardctrler

// Shardctrler clerk.

import (
	"../labrpc"
	"fmt"
	"sync/atomic"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader  int32
	clntId  int64
	seqId   int64
}

func (ck *Clerk) String() string {
	return fmt.Sprintf("[ck l:S%v clntId:%v]",
		ck.leader, ck.clntId % clntIdDebugMod,
	)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers: servers,
		clntId:  nrand(),
		seqId: -1,
	}
}

func (ck *Clerk) StartOp(args *OpArgs) Config {
	args.Seq = ck.nextSeq()
	leader := atomic.LoadInt32(&ck.leader)
	reply := &OpReply{}
	for {
		DPrintf(clerk, "%v to S%v %v", ck, leader, args)
		if ok := ck.servers[leader].Call("ShardCtrler.OpHandler", args, reply); !ok {
			DPrintf(clerk, "%v to S%v failed %v %v", ck, leader, args, reply)
			leader = ck.nextLeader()
			time.Sleep(ClerkRetryTimeout)
			continue
		}

		DPrintf(debugInfo|clerk, "%v to S%v return %v %v", ck, leader, args, reply)
		switch reply.Err {
		case OK, ErrNoConfig:
			return reply.Config
		case ErrTimeout:
			continue
		case ErrWrongLeader:
			leader = ck.nextLeader()
			time.Sleep(ClerkWLeaderTimeout)
		default:
			DPanicf(clerk, "return with ERROR %v to S%v %v %v", ck, leader, args, reply)
		}
	}
}

func (ck *Clerk) Query(num int) Config {
	return ck.StartOp(&OpArgs{
		OpType: OPQuery,
		Num:    num,
		ClntId: ck.clntId,
	})
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.StartOp(&OpArgs{
		OpType:  OPJoin,
		Servers: servers,
		ClntId:  ck.clntId,
	})
}

func (ck *Clerk) Leave(gids []int) {
	ck.StartOp(&OpArgs{
		OpType:  OPLeave,
		GIDs:    gids,
		ClntId:  ck.clntId,
	})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.StartOp(&OpArgs{
		OpType:  OPMove,
		Shard:   shard,
		GID:     gid,
		ClntId:  ck.clntId,
	})
}

func (ck *Clerk) nextLeader() int32 {
	nextLeader := (atomic.LoadInt32(&ck.leader) + 1) % int32(len(ck.servers))
	atomic.StoreInt32(&ck.leader, nextLeader)
	return nextLeader
}

func (ck *Clerk) nextSeq() int64 {
	nextSeq := atomic.LoadInt64(&ck.seqId) + 1
	atomic.StoreInt64(&ck.seqId, nextSeq)
	return nextSeq
}

