package shardkv

// Client code to talk to a sharded key/value service.
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.

import (
	"../labrpc"
	"sync"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"
import "../shardctrler"
import "time"

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

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sync.Mutex
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	makeEnd  func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int32
	clntId   int64
	seqId    int64
}

// MakeClerk the tester calls MakeClerk.
// ctrler[] is needed to call shardctrler.MakeClerk().
// make_end(servername) turns a server name from a
// Config.Groups[GID][i] into a labrpc.ClientEnd on which you can send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		makeEnd:  makeEnd,
		clntId:   nrand(),
		seqId:    -1,
	}
	ck.sm = shardctrler.MakeClerk(ctrlers)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) StartOp(args *OpArgs) string {
	for ck.config.Shards == nil {
		time.Sleep(ClerkWrongGroupInterval)
		ck.config = ck.sm.Query(-1)
	}
	args.Seq = ck.nextSeq()
	for {
		shard := key2shard(args.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si = (si + 1) % len(servers) {
				serv := ck.makeEnd(servers[si])
				DPrintf(clerk, "si:%v %v to Serv %v %v", si, ck, servers[si], args)

				reply := &OpReply{}
				if ok := serv.Call("ShardKV.OpHandler", args, reply); !ok {
					DPrintf(clerk, "si:%v %v to Serv %v failed %v %v", si, ck, servers[si], args, reply)
					time.Sleep(ClerkRetryTimeout)
					continue
				}

				DPrintf(clerk, "si:%v %v to Serv %v %v", si, ck, servers[si], args)
				switch reply.Err {
				case OK, ErrNoKey:
					return reply.Value
				case ErrTimeout:
					continue
				case ErrWrongLeader:
					time.Sleep(ClerkWrongLeaderInterval)
				case ErrWrongGroup:
					time.Sleep(ClerkWrongGroupInterval)
					ck.config = ck.sm.Query(-1)
					break
				default:
					DPanicf(clerk, "si:%v %v to Serv %v return with ERROR Uninitialized %v %v", si, ck, servers[si], args, reply)
				}
			}
		}
	}
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	return ck.StartOp(&OpArgs{
		Key:    key,
		Value:  "",
		OpType: OPGet,
		Clnt:   ck.clntId,
	})
}

// PutAppend shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.StartOp(&OpArgs{
		Key:    key,
		Value:  value,
		OpType: OPType(op),
		Clnt:   ck.clntId,
	})
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) nextSeq() int64 {
	nextSeq := atomic.LoadInt64(&ck.seqId) + 1
	atomic.StoreInt64(&ck.seqId, nextSeq)
	return nextSeq
}
