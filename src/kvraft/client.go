package kvraft

import (
	"../labrpc"
	"fmt"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int32
	clntId int64
	seqId  int64
}

func (ck *Clerk) String() string {
	return fmt.Sprintf("[Ck l:S%v clntId:%v]",
		ck.leader, ck.clntId % clntIdDebugMod,
	)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// You'll have to add code here.
	time.Sleep(1500 * time.Millisecond)
	return &Clerk{
		servers: servers,
		clntId:  nrand(),
		seqId: -1,
	}
}

func (ck *Clerk) StartOp(args *OpArgs) string {
	args.Seq = ck.nextSeq()
	leader := atomic.LoadInt32(&ck.leader)
	rpcErrorNum := 0 // try another server after rpc error(server crashed) 3 times
	for {
		reply := &OpReply{}
		DPrintf(clerk, "%v to S%v %v", ck, leader, args)
		if ok := ck.servers[leader].Call("KVServer.OpHandler", args, reply); !ok {
			DPrintf(clerk, "%v to S%v failed %v %v", ck, leader, args, reply)
			rpcErrorNum++
			if rpcErrorNum == 3 {
				leader = ck.nextLeader()
				rpcErrorNum = 0
			}
			time.Sleep(ClerkRetryTimeout)
			continue
		}

		DPrintf(debugInfo|clerk, "%v to S%v return %v %v", ck, leader, args, reply)
		switch reply.Err {
		case OK, ErrNoKey:
			return reply.Value
		case ErrTimeout:
			continue
		case ErrWrongLeader:
			leader = ck.nextLeader()
			time.Sleep(ClerkWLeaderTimeout)
		default:
			DPanicf(clerk, "return with ERROR Uninitialized %v to S%v %v %v", ck, leader, args, reply)
		}
	}
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	if len(key) == 0 {
		return ""
	}
	return ck.StartOp(&OpArgs{
		Key:   key,
		Value: "",
		Op:    "Get",
		Clnt:  ck.clntId,
	})
}

// PutAppend shared by Put and Append.
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	if len(key) == 0 {
		return
	}
	ck.StartOp(&OpArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Clnt:  ck.clntId,
	})
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
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
