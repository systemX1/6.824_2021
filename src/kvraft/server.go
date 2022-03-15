package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type OPType string
const (
	GET    OPType = "Get"
	PUT    OPType = "Put"
	APPEND OPType = "Append"
)

type OpContext struct {
	Key      string
	Value    string
	OpType   OPType
	Seq	     int64
	Clnt     int64
	ReplyVal string
	ReplyErr Err
}

func (opctx *OpContext) String() string {
	return fmt.Sprintf("[OpCtx %v k:%v v:%v seq:%v clnt:%v rly v:%v err:%v]",
		opctx.OpType, opctx.Key, opctx.Value, opctx.Seq, opctx.Clnt,
		opctx.ReplyVal, opctx.ReplyErr,
	)
}

type Op OpArgs
type KVMap                map[string]string
type clntIdOpContextMap   map[int64]*OpContext	// (key: seq,      val: lastAppliedOp)
type replyChan            chan *OpReply
type logIndexReplyChanMap map[int]replyChan		// (key: logIndex, val: replyChan)

type KVServer struct {
	sync.Mutex
	me            int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	dead          int32 // set by Kill()
	maxraftstate  int // snapshot if log grows this big

	// Your definitions here.
	lastApplied   int
	storage       KVMap
	lastClntOpSet clntIdOpContextMap
	replyChanSet  logIndexReplyChanMap
}

func (kv *KVServer) String() string {
	return fmt.Sprintf("[ck S%v a:%v stor:%v laClOp:%v rlyCh:%v]",
		kv.me, kv.lastApplied, kv.storage, kv.lastClntOpSet, kv.replyChanSet,
	)
}

func (kv *KVServer) OpHandler(args *OpArgs, reply *OpReply) {
	kv.Lock()
	if args.Op != string(GET) && kv.isDuplicated(args.Clnt, args.Seq) {
		lastOpContext := kv.lastClntOpSet[args.Clnt]
		reply.Value, reply.Err = lastOpContext.ReplyVal, lastOpContext.ReplyErr
		DPrintf(kvserver, "Op isDuplicated%v %v %v", kv, args, reply)
		kv.Unlock()
		return
	}
	kv.Unlock()

	var op = (Op)(*args)
	logIndex, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}

	kv.Lock()
	DPrintf(kvserver, "OpReceive %v %v", kv, args)
	replyChan := kv.appendApplyChan(logIndex)
	kv.Unlock()
	select {
	case tmp := <-replyChan:
		reply.Value, reply.Err = tmp.Value, tmp.Err
	case <-time.After(ServerApplyTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.Lock()
		defer kv.Unlock()
		DPrintf(kvserver, "Op DONE %v %v %v", kv, args, reply)
		delete(kv.replyChanSet, logIndex)
	}()
	return
}

func (kv *KVServer) appendApplyChan(key int) replyChan {
	ch := make(replyChan)
	kv.replyChanSet[key] = ch
	return ch
}

func (kv *KVServer) isDuplicated(clntId, seqId int64) bool {
	lastAppliedOp, ok := kv.lastClntOpSet[clntId]
	return ok && seqId <= lastAppliedOp.Seq
}

func (kv *KVServer) run() {
	for {
		if kv.killed() {
			DPrintf(kvserver, "%v is killed %v", kv, kv.rf)
			return
		}
		select {
		case applyMsg := <-kv.applyCh:
			kv.Lock()
			DPrintf(kvserver, "%v Receive applyMsg %v", kv, applyMsg)
			if applyMsg.CommandValid {
				if applyMsg.Command == nil || applyMsg.CommandIndex <= kv.lastApplied {
					DPrintf(kvserver|debugError, "%v applyMsg ERROR %v", kv, applyMsg)
					kv.Unlock()
					continue
				}
				op := applyMsg.Command.(Op)
				reply := kv.doApply(&op)
				kv.lastApplied = applyMsg.CommandIndex
				DPrintf(kvserver, "apply to SM %v %v %v", kv, applyMsg, reply)
				if currTerm, isLeader := kv.rf.GetState(); isLeader && currTerm == applyMsg.CommandTerm {
					// reply clerk only if it is the leader
					if ch, ok := kv.replyChanSet[applyMsg.CommandIndex]; ok {
						ch <-reply
					}
				}
			} else if applyMsg.SnapshotValid {
				if applyMsg.Snapshot == nil {
					DPrintf(kvserver|debugError, "applyMsg ERROR %v %v", kv, applyMsg)
					kv.Unlock()
					continue
				}

			} else {
				DPanicf(kvserver|debugError, "applyMsg ERROR %v %v", kv, applyMsg)
			}
			kv.Unlock()
		}
	}
}

func (kv *KVServer) doApply(op *Op) *OpReply {
	reply := &OpReply{Err: OK}
	if preOp, ok := kv.lastClntOpSet[op.Clnt]; ok {
		if preOp.Seq == op.Seq {
			reply.Value, reply.Err = preOp.ReplyVal, preOp.ReplyErr
			return reply
		}
	}
	switch op.Op {
	case string(GET):
		v, ok := kv.storage[op.Key]; if !ok {
			reply.Err = ErrNoKey
		}
		reply.Value = v
	case string(PUT):
		kv.storage[op.Key] = op.Value
	case string(APPEND):
		kv.storage[op.Key] += op.Value
	}
	kv.lastClntOpSet[op.Clnt] = &OpContext{
		Key: op.Key, Value: op.Value,
		OpType: OPType(op.Op),
		Seq: op.Seq, Clnt: op.Clnt,
		ReplyVal: reply.Value,
		ReplyErr: reply.Err,
	}
	return reply
}

func (kv *KVServer) doSnapshot() {

}

func (kv *KVServer) snapshotLoop() {

}

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// "me" is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.storage = make(KVMap)
	kv.lastClntOpSet = make(clntIdOpContextMap)
	kv.replyChanSet = make(logIndexReplyChanMap)
	kv.lastApplied = -1
	DPrintf(kvserver,"%v init", kv)
	go kv.run()
	return kv
}
