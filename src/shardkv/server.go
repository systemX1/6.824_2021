package shardkv


import (
	"../labgob"
	"../labrpc"
	"../raft"
	"../shardctrler"
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Op                   OpArgs
type KVMap                map[string]string
type ShardMap             map[int]*Shard
type replyChan            chan *OpReply
type replyChanMap         map[int]replyChan // (key: logIndex, val: replyChan)

func (s *ShardMap) isDuplicated(clntId, seqId int64) (*OpContext, bool) {
	for _, shard := range *s {
		if lastAppliedOp, ok := shard.isDuplicated(clntId, seqId); ok {
			return lastAppliedOp, true
		}
	}
	return nil, false
}

func (s *ShardMap) String() string {
	var sb strings.Builder
	sb.WriteString("[")
	for _, shard := range *s {
		sb.WriteString(shard.String())
		sb.WriteByte(' ')
	}
	sb.WriteString("]")
	return sb.String()
}

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

	ShardMap
	replyChanMap

	lastConfig    shardctrler.Config
	currConfig    shardctrler.Config

	migrationCh   chan int
	GCCh          chan int
}

func (kv *ShardKV) String() string {
	return fmt.Sprintf("[ShardKV GID:%v %v a:%v Cfg:%v%v Shards: %v %v]",
		kv.GID, kv.rf, kv.lastApplied, &kv.currConfig, &kv.lastConfig,
		len(kv.ShardMap), &kv.ShardMap)
}

func (kv *ShardKV) OpHandler(args *OpArgs, reply *OpReply) {
	kv.Lock()
	if lastOpCtx, ok := kv.isDuplicated(args.Clnt, args.Seq); ok {
		reply.RlyVal, reply.RlyErr = lastOpCtx.RlyVal, lastOpCtx.RlyErr
		DPrintf(shardkv, "Op Duplicated %v %v %v", kv, args, reply)
		kv.Unlock()
		return
	}
	kv.Unlock()

	shardNum := key2shard(args.Key)
	shard, ok := kv.ShardMap[shardNum]
	if !ok  {
		reply.RlyErr = ErrWrongGroup
		DPrintf(debugTest|shardkv, "reject %v %v", ErrWrongGroup, kv)
		return
	} else if shard.Stat == Preparing {
		reply.RlyErr = ErrShardStatUnexpected
		DPrintf(debugTest|shardkv, "reject %v %v %v", ErrShardStatUnexpected, kv, args)
		return
	}
	var op = (Op)(*args)
	logIndex, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.RlyErr = ErrWrongLeader
		DPrintf(debugTest|shardkv, "reject %v %v", ErrWrongLeader, kv)
		return
	}

	kv.Lock()
	DPrintf(shardkv, "OpReceive %v %v", kv, args)
	replyChan := kv.appendApplyChan(logIndex)
	kv.Unlock()
	select {
	case tmp := <-replyChan:
		reply.RlyVal, reply.RlyErr = tmp.RlyVal, tmp.RlyErr
	case <-time.After(ServerApplyTimeout):
		reply.RlyErr = ErrTimeout
	}

	go func() {
		kv.Lock()
		defer kv.Unlock()
		DPrintf(shardkv, "Op DONE %v %v %v", kv, args, reply)
		delete(kv.replyChanMap, logIndex)
	}()
}

func (kv *ShardKV) appendApplyChan(key int) replyChan {
	ch := make(replyChan)
	kv.replyChanMap[key] = ch
	return ch
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
					DPrintf(shardkv|debugError, "%v applyMsg ERROR or outdated %v", kv, &applyMsg)
					kv.Unlock()
					continue
				}
				opCtx := &OpContext{
					OpArgs:  OpArgs(applyMsg.Command.(Op)),
					OpReply: OpReply{RlyErr: OK},
				}

				DPrintf(shardkv, "applyOp %v %v", kv, opCtx)
				kv.applyOp(opCtx)
				kv.lastApplied = applyMsg.CommandIndex
				DPrintf(shardkv, "applyOp DONE %v %v", kv, opCtx)

				// reply clerk only if it is the leader
				if currTerm, isLeader := kv.rf.GetState(); isLeader && currTerm == applyMsg.CommandTerm {
					if ch, ok := kv.replyChanMap[applyMsg.CommandIndex]; ok {
						ch <- &opCtx.OpReply
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

func (kv *ShardKV) applyOp(opCtx *OpContext) {
	reply := &OpReply{RlyErr: OK}
	if lastOpCtx, ok := kv.isDuplicated(opCtx.Clnt, opCtx.Seq); ok {
		reply.RlyVal, reply.RlyErr = lastOpCtx.RlyVal, lastOpCtx.RlyErr
		return
	}

	switch opCtx.OpType {
	case OPKV:
		kv.applyOpKV(opCtx)
	case OPPullConf:
		kv.applyOpPullConf(opCtx)
	case OPAddShard:
		kv.applyAddShard(opCtx)
	case OPDelShard:
		kv.applyDelShard(opCtx)
	case OPAvailShard:
		kv.applyAvailShard(opCtx)
	case NOOP:
	default:
		DPanicf(shardkv|debugError, "OpType ERROR %v %v", kv, &opCtx)
	}
}

func (kv *ShardKV) applyOpKV(opCtx *OpContext) {
	shardNum := key2shard(opCtx.Key)
	shard, ok := kv.ShardMap[shardNum]
	if !ok || shard.Stat == Preparing || shard.Stat == Removing {
		opCtx.RlyErr = ErrShardStatUnexpected
		return
	}
	switch opCtx.OpKVType {
	case OPGet:
		v, ok := shard.Storage[opCtx.Key]; if !ok {
		opCtx.RlyErr = ErrNoKey
		}
		opCtx.RlyVal = v
	case OPPut:
		shard.Storage[opCtx.Key] = opCtx.Value
	case OPAppend:
		shard.Storage[opCtx.Key] += opCtx.Value
	}
	shard.LastClntOpMap[opCtx.Clnt] = opCtx
}

func (kv *ShardKV) applyOpPullConf(opCtx *OpContext) {
	if opCtx.Config.Num != kv.currConfig.Num + 1 || opCtx.Config.Shards == nil {
		DPrintf(debugTest|shardkv, "reject outdated config %v %v", kv, opCtx)
		return
	}
	DPrintf(debugTest, "%v %v", kv, &opCtx.Config)
	kv.lastConfig = kv.currConfig
	kv.currConfig = opCtx.Config

	// pull needed
	for shardNum, GID := range kv.currConfig.Shards {
		if GID == kv.GID {
			if _, ok := kv.ShardMap[shardNum]; !ok {
				if kv.lastConfig.Shards != nil && kv.lastConfig.Shards[shardNum] != 0 {
					kv.ShardMap[shardNum] = NewShard(shardNum, Preparing)
				} else {
					kv.ShardMap[shardNum] = NewShard(shardNum, Available)
				}
			}
		}
	}
	// remove needed
	for shardNum := range kv.ShardMap {
		if kv.currConfig.Shards[shardNum] != kv.GID {
			if kv.ShardMap[shardNum].Stat != Available {
				DPanicf(debugTest, "unexpected %v", kv)
			}
			kv.ShardMap[shardNum].Stat = Removing
		}
	}
	DPrintf(debugTest, "DONE %v %v", kv, &opCtx.Config)
}

func (kv *ShardKV) applyAddShard(opCtx *OpContext) {
	defer DPrintf(debugError, "applyAddShard DONE %v %v", kv, opCtx)
	if opCtx.Config.Num != kv.currConfig.Num {
		opCtx.RlyErr = ErrWrongConfig
		return
	}
	shardNum := opCtx.Shard.Num
	shard, ok := kv.ShardMap[shardNum]
	if !ok || shard.Stat != Preparing {
		opCtx.RlyErr = ErrShardStatUnexpected
		return
	}
	shard.Stat, shard.Storage, shard.LastClntOpMap = GCWait, opCtx.Storage, opCtx.LastClntOpMap
	if shard.Storage == nil {
		shard.Storage = make(KVMap)
	}
	if shard.LastClntOpMap == nil {
		shard.LastClntOpMap = make(clntIdOpCtxMap)
	}
}

func (kv *ShardKV) applyDelShard(opCtx *OpContext) {
	defer DPrintf(debugError, "applyDelShard DONE %v %v", kv, opCtx)
	if opCtx.Config.Num != kv.currConfig.Num {
		opCtx.RlyErr = ErrWrongConfig
		return
	}
	shardNum := opCtx.Shard.Num
	shard, ok := kv.ShardMap[shardNum]
	if !ok || shard.Stat != Removing {
		opCtx.RlyErr = ErrShardStatUnexpected
		return
	}
    delete(kv.ShardMap, shardNum)
}

func (kv *ShardKV) applyAvailShard(opCtx *OpContext) {
	defer DPrintf(debugError, "applyDelShard DONE %v %v", kv, opCtx)
	if opCtx.Config.Num != kv.currConfig.Num {
		opCtx.RlyErr = ErrWrongConfig
		return
	}
	shardNum := opCtx.Shard.Num
	shard, ok := kv.ShardMap[shardNum]
	if !ok || shard.Stat != GCWait {
		opCtx.RlyErr = ErrShardStatUnexpected
		return
	}
	shard.Stat = Available
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
			e.Encode(kv.ShardMap)
			e.Encode(kv.lastConfig)
			e.Encode(kv.currConfig)
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
	var shardMap ShardMap
	var lastConfig, currConfig shardctrler.Config
	if d.Decode(&shardMap) != nil {
		DPanicf(debugTest, "%v", kv)
		shardMap = make(ShardMap)
	}
	if d.Decode(&lastConfig) != nil {
		DPanicf(debugTest, "%v", kv)
		lastConfig = *shardctrler.NewConfig()
	}
	if d.Decode(&currConfig) != nil {
		DPanicf(debugTest, "%v", kv)
		currConfig = *shardctrler.NewConfig()
	}
	kv.ShardMap, kv.lastConfig, kv.currConfig = shardMap, lastConfig, currConfig
	DPrintf(snapshotLog, "ApplySnapshot to SM %v %v", kv, kv.rf)
}

func (kv *ShardKV) pullConfigAction() {
	kv.Lock()
	for shardNum := range kv.ShardMap {
		if kv.ShardMap[shardNum].Stat != Available {
			kv.Unlock()
			return
		}
	}
	currCfgNum := kv.currConfig.Num
	kv.Unlock()
	if config := kv.ctrlerClerk.Query(currCfgNum + 1); config.Num == currCfgNum + 1 {
		DPrintf(debugTest, "pull Config %v %v", kv, &config)
		kv.rf.Start(Op{
			OpType:   OPPullConf,
			Config:   config,
		})
	}
}

func (kv *ShardKV) migrationAction() {
	kv.Lock()
	var wg sync.WaitGroup
	for shardNum, shard := range kv.ShardMap {
		if shard.Stat == Preparing {
			GID := kv.lastConfig.Shards[shardNum]
			servs := kv.lastConfig.Groups[GID]
			args := &MigrationArgs{
				ShardNum:  shardNum,
				ConfigNum: kv.currConfig.Num,
				GC:        false,
			}
			DPrintf(debugTest, "To GID:%v %v %v %v", GID, servs, kv, args)

			go func(servs []string, args *MigrationArgs) {
				wg.Add(1)
				defer wg.Done()

				for _, serv := range servs {
					servAddr := kv.makeEnd(serv)
					reply := &MigrationReply{}
					if ok := servAddr.Call("ShardKV.MigrationHandler", args, reply); !ok || reply.RlyErr != OK {
						DPrintf(shardkv, "pull shard failed %v %v %v %v", serv, kv, args, reply)
						continue
					}
					DPrintf(shardkv, "pull shard succ %v %v %v %v", serv, kv, args, reply)
					kv.rf.Start(Op{
						OpType: OPAddShard,
						Shard: Shard{Num: args.ShardNum, Storage: reply.Storage, LastClntOpMap: reply.LastClntOpMap},
						Config: shardctrler.Config{Num: args.ConfigNum},
					})
					break
				}
			}(servs, args)
		}
	}
	kv.Unlock()
	wg.Wait()
}

func (kv *ShardKV) MigrationHandler(args *MigrationArgs, reply *MigrationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.RlyErr = ErrWrongLeader
		return
	}
	kv.Lock()
	defer kv.Unlock()
	defer DPrintf(shardkv, "MigrationHandle DONE %v %v %v", kv, args, reply)
	reply.ConfigNum = kv.currConfig.Num
	if kv.currConfig.Num != args.ConfigNum  {
		reply.RlyErr = ErrWrongConfig
		return
	}
	shard, ok := kv.ShardMap[args.ShardNum]
	if !ok {
		DPanicf(debugError, "shard not exist %v %v", kv, args)
	}
	if shard.Stat != Removing {
		reply.RlyErr = ErrShardStatUnexpected
	}

	if !args.GC {
		reply.Storage, reply.LastClntOpMap = make(KVMap), make(clntIdOpCtxMap)
		for k, v := range shard.Storage {
			reply.Storage[k] = v
		}
		for k, v := range shard.LastClntOpMap {
			reply.LastClntOpMap[k] = v
		}
	} else {
		delete(kv.ShardMap, args.ShardNum)
	}
	reply.RlyErr = OK
}

func (kv *ShardKV) GCAction() {
	kv.Lock()
	var wg sync.WaitGroup
	for shardNum, shard := range kv.ShardMap {
		if shard.Stat == GCWait {
			wg.Add(1)
			GID := kv.lastConfig.Shards[shardNum]
			servs := kv.lastConfig.Groups[GID]
			go func() {
				defer wg.Done()
				args := &MigrationArgs{
					ShardNum:  shardNum,
					ConfigNum: kv.lastConfig.Num,
					GC:        true,
				}
				reply := &MigrationReply{}
				for _, serv := range servs {
					servAddr := kv.makeEnd(serv)
					if ok := servAddr.Call("ShardKV.MigrationHandler", args, reply); !ok || reply.RlyErr != OK {
						DPrintf(shardkv, "GC failed %v %v %v %v", servAddr, kv, args, reply)
					}
					DPrintf(shardkv, "GC succ %v %v %v %v", servAddr, kv, args, reply)
					kv.rf.Start(Op{
						OpType: OPAvailShard,
						Shard: Shard{Storage: reply.Storage, LastClntOpMap: reply.LastClntOpMap},
						Config: shardctrler.Config{Num: args.ConfigNum},
					})
				}
			}()
		}
	}
	kv.Unlock()
	wg.Wait()
}

func (kv *ShardKV) appendNOOPAction() {

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
	labgob.Register(OpArgs{})
	labgob.Register(OpReply{})
	labgob.Register(Shard{})

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

	kv.ShardMap = make(ShardMap)
	kv.replyChanMap = make(replyChanMap)
	kv.lastApplied = -1
	kv.lastConfig, kv.currConfig = shardctrler.Config{Num: 0}, shardctrler.Config{Num: 0}
	kv.ApplySnapshot(persister.ReadSnapshot())

	go kv.run()
	go kv.snapshotLoop()
	go kv.debugGoroutine()

	go kv.LeaderDaemon(kv.pullConfigAction, LeaderPullConfigInterval)
	go kv.LeaderDaemon(kv.migrationAction, LeaderMigrationInterval)
	go kv.LeaderDaemon(kv.GCAction, LeaderGCInterval)
	go kv.LeaderDaemon(kv.appendNOOPAction, LeaderAppendNOOPInterval)

	DPrintf(shardkv,"%v init", kv)
	return kv
}

func (kv *ShardKV) LeaderDaemon(action func(), interval time.Duration) {
	for !kv.killed() {
		time.Sleep(interval)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		action()
	}
}

func (kv *ShardKV) debugGoroutine() bool {
	t1 := time.Now()
	for {
		DPrintf(debugTest2, "Goroutine Num:%v %v", runtime.NumGoroutine(), time.Now().Sub(t1))
		time.Sleep(10 * time.Second)
	}
}

