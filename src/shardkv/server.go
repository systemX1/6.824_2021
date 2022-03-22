package shardkv


import (
	"../labgob"
	"../labrpc"
	"../raft"
	"../shardctrler"
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type OpCache struct {
	Key      string
	Value    string
	OpType   OPType
	Seq	     int64
	Clnt     int64
	ReplyVal string
	ReplyErr Err
}

func (oc *OpCache) String() string {
	return fmt.Sprintf("[OpCtx %v k:%v v:%v seq:%v clnt:%v rly v:%v err:%v]",
		oc.OpType, oc.Key, oc.Value, oc.Seq, oc.Clnt,
		oc.ReplyVal, oc.ReplyErr,
	)
}

type Op                   OpArgs
type KVMap                map[string]string
type clntIdOpContextMap   map[int64]*OpCache	// (key: seq,      val: lastAppliedOp)
type replyChan            chan *OpReply
type logIndexReplyChanMap map[int]replyChan		// (key: logIndex, val: replyChan)

type ShardKV struct {
	sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	GID          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	ctrlerClerk   *shardctrler.Clerk
	dead          int32
	lastApplied   int
	storage       KVMap
	lastClntOpSet clntIdOpContextMap
	replyChanSet  logIndexReplyChanMap
}

func (kv *ShardKV) OpHandler(args *OpArgs, reply *OpReply) {
	kv.Lock()
	if kv.isDuplicated(args.Clnt, args.Seq) {
		lastOpContext := kv.lastClntOpSet[args.Clnt]
		reply.Value, reply.Err = lastOpContext.ReplyVal, lastOpContext.ReplyErr
		DPrintf(shardkv, "Op isDuplicated%v %v %v", kv, args, reply)
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
	DPrintf(shardkv, "OpReceive %v %v", kv, args)
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
		DPrintf(shardkv, "Op DONE %v %v %v", kv, args, reply)
		delete(kv.replyChanSet, logIndex)
	}()
}

func (kv *ShardKV) appendApplyChan(key int) replyChan {
	ch := make(replyChan)
	kv.replyChanSet[key] = ch
	return ch
}

func (kv *ShardKV) isDuplicated(clntId, seqId int64) bool {
	lastAppliedOp, ok := kv.lastClntOpSet[clntId]
	return ok && seqId <= lastAppliedOp.Seq
}

func (kv *ShardKV) run() {
	for {
		if kv.killed() {
			DPrintf(shardkv, "%v is killed %v", kv, kv.rf)
			return
		}
		select {
		case applyMsg := <-kv.applyCh:
			kv.Lock()
			DPrintf(shardkv, "%v Receive applyMsg %v", kv, &applyMsg)
			if applyMsg.CommandValid {
				if applyMsg.Command == nil || applyMsg.CommandIndex <= kv.lastApplied {
					DPrintf(shardkv|debugError, "%v applyMsg ERROR %v", kv, &applyMsg)
					kv.Unlock()
					continue
				}
				op := applyMsg.Command.(Op)
				currTerm, isLeader := kv.rf.GetState()

				var reply *OpReply
				if !(op.OpType == OPGet && isLeader == false) {
					reply = kv.applyCommand(&op)
				}
				kv.lastApplied = applyMsg.CommandIndex
				DPrintf(shardkv, "applyCommand to SM %v %v %v", kv, &applyMsg, reply)
				// reply clerk only if it is the leader
				if reply != nil && isLeader && currTerm == applyMsg.CommandTerm {
					if ch, ok := kv.replyChanSet[applyMsg.CommandIndex]; ok {
						ch <-reply
					}
				}
			} else if applyMsg.SnapshotValid {
				if applyMsg.Snapshot == nil {
					DPrintf(shardkv|debugError, "applyMsg ERROR %v %v", kv, &applyMsg)
					kv.Unlock()
					continue
				}
				if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
					kv.lastApplied = applyMsg.SnapshotIndex
					kv.ApplySnapshot(applyMsg.Snapshot)
					DPrintf(shardkv|debugError, "CondInstallSnapshot %v %v %v", kv, kv.rf, &applyMsg)
				}
			} else {
				DPanicf(shardkv|debugError, "applyMsg ERROR %v %v", kv, &applyMsg)
			}
			kv.Unlock()
		}
	}
}

func (kv *ShardKV) applyCommand(op *Op) *OpReply {
	reply := &OpReply{Err: OK}
	if preOp, ok := kv.lastClntOpSet[op.Clnt]; ok {
		if preOp.Seq == op.Seq {
			reply.Value, reply.Err = preOp.ReplyVal, preOp.ReplyErr
			return reply
		}
	}
	switch op.OpType {
	case OPGet:
		v, ok := kv.storage[op.Key]; if !ok {
		reply.Err = ErrNoKey
	}
		reply.Value = v
	case OPPut:
		kv.storage[op.Key] = op.Value
	case OPAppend:
		kv.storage[op.Key] += op.Value
	}
	kv.lastClntOpSet[op.Clnt] = &OpCache{
		Key: op.Key, Value: op.Value,
		OpType: op.OpType,
		Seq: op.Seq, Clnt: op.Clnt,
		ReplyVal: reply.Value,
		ReplyErr: reply.Err,
	}
	return reply
}

func (kv *ShardKV) snapshotLoop() {
	if kv.maxraftstate == -1 {
		return
	}
	maxraftstate := kv.maxraftstate
	for !kv.killed() {
		if kv.rf.GetPersisterSize() >= maxraftstate {
			kv.Lock()
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.storage)
			e.Encode(kv.lastClntOpSet)
			snapshot := w.Bytes()
			if snapshot != nil {
				go kv.rf.Snapshot(kv.lastApplied, snapshot)
			}
			DPrintf(snapshotLog, "do Snapshot %v %v", kv, kv.rf)
			kv.Unlock()
		}
		time.Sleep(ServerSnapshotInterval)
	}
}

func (kv *ShardKV) ApplySnapshot(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var storage KVMap
	var lastClntOpSet clntIdOpContextMap
	if d.Decode(&storage) != nil {
		storage = make(KVMap)
	}
	if d.Decode(&lastClntOpSet) != nil {
		lastClntOpSet = make(clntIdOpContextMap)
	}
	kv.storage, kv.lastClntOpSet = storage, lastClntOpSet
	DPrintf(snapshotLog, "ApplySnapshot to SM %v %v", kv, kv.rf)
}

// Kill the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartServer servers[] contains the ports of the servers in this group.
// "me" is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// GID is this group's GID, for interacting with the shardctrler.
//
// pass ctrler[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[GID][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrler[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.makeEnd = makeEnd
	kv.GID = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	// Use something like this to talk to the shardctrler:
	kv.ctrlerClerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.storage = make(KVMap)
	kv.lastClntOpSet = make(clntIdOpContextMap)
	kv.replyChanSet = make(logIndexReplyChanMap)
	kv.lastApplied = -1
	kv.ApplySnapshot(persister.ReadSnapshot())

	go kv.run()
	go kv.snapshotLoop()
	go kv.debugGoroutine()

	DPrintf(shardkv,"%v init", kv)
	return kv
}

func (kv *ShardKV) debugGoroutine() bool {
	t1 := time.Now()
	for {
		DPrintf(debugTest2, "Goroutine Num:%v %v", runtime.NumGoroutine(), time.Now().Sub(t1))
		//if runtime.NumGoroutine() > 120 {
		//	panic(1)
		//}
		time.Sleep(10 * time.Second)
	}
}

