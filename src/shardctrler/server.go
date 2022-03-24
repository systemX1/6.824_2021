package shardctrler


import (
	"../raft"
	"fmt"
	"math"
	"sync/atomic"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

type Op                   OpArgs
type replyChan            chan *OpReply
type clntIdOpContextMap   map[int64]*OpCache // (key: seq,      val: lastAppliedOp)
type logIndexReplyChanMap map[int]replyChan  // (key: logIndex, val: replyChan)

type ShardCtrler struct {
	sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead          int32
	lastApplied   int
	configs       []Config // indexed by config num
	lastClntOpSet clntIdOpContextMap
	replyChanSet  logIndexReplyChanMap
}

func (sc *ShardCtrler) String() string {
	return fmt.Sprintf("[ShCt S%v conf:%v]",
		sc.me, sc.configs[len(sc.configs)-1],
	)
}

type OpCache struct {
	OpType    OPType
	Seq	      int64
	Clnt      int64
	ReplyErr  Err
	ReplyConf Config
}

func (opctx *OpCache) String() string {
	return fmt.Sprintf("[OpCtx %v seq:%v clnt:%v rly cof:%v err:%v]",
		opctx.OpType, opctx.Seq, opctx.Clnt,
		opctx.ReplyConf, opctx.ReplyErr,
	)
}

func (sc *ShardCtrler) OpHandler(args *OpArgs, reply *OpReply) {
	sc.Lock()
	if sc.isDuplicated(args.ClntId, args.Seq) {
		lastOpContext := sc.lastClntOpSet[args.ClntId]
		reply.Config, reply.Err = lastOpContext.ReplyConf, lastOpContext.ReplyErr
		DPrintf(shardctrler, "Op isDuplicated%v %v %v", sc, args, reply)
		sc.Unlock()
		return
	} else if args.OpType == OPQuery && args.Num >= 0 && args.Num < len(sc.configs) {
		reply.Err, reply.Config = OK, sc.configs[args.Num]
		sc.Unlock()
		return
	}
	sc.Unlock()

	var op = (Op)(*args)
	logIndex, _, isLeader := sc.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}

	sc.Lock()
	DPrintf(shardctrler, "OpReceive %v %v", sc, args)
	replyChan := sc.appendApplyChan(logIndex)
	sc.Unlock()
	select {
	case tmp := <-replyChan:
		reply.Config, reply.Err = tmp.Config, tmp.Err
	case <-time.After(ServerApplyTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.Lock()
		defer sc.Unlock()
		DPrintf(shardctrler, "Op DONE %v %v %v", sc, args, reply)
		delete(sc.replyChanSet, logIndex)
	}()
}

func (sc *ShardCtrler) appendApplyChan(key int) replyChan {
	ch := make(replyChan)
	sc.replyChanSet[key] = ch
	return ch
}

func (sc *ShardCtrler) isDuplicated(clntId, seqId int64) bool {
	lastAppliedOp, ok := sc.lastClntOpSet[clntId]
	return ok && seqId <= lastAppliedOp.Seq
}

func (sc *ShardCtrler) run() {
	for {
		if sc.killed() {
			DPrintf(shardctrler, "%v is killed %v", sc, sc.rf)
			return
		}
		select {
		case applyMsg := <-sc.applyCh:
			sc.Lock()
			DPrintf(shardctrler, "%v Receive applyMsg %v", sc, &applyMsg)
			if applyMsg.CommandValid {
				if applyMsg.Command == nil || applyMsg.CommandIndex <= sc.lastApplied {
					DPrintf(shardctrler|debugError, "%v applyMsg ERROR %v", sc, &applyMsg)
					sc.Unlock()
					continue
				}
				op := applyMsg.Command.(Op)
				currTerm, isLeader := sc.rf.GetState()
				var reply *OpReply
				if !(op.OpType == OPQuery && isLeader == false) {
					reply = sc.applyCommand(&op)
				}

				sc.lastApplied = applyMsg.CommandIndex
				DPrintf(shardctrler, "applyCommand to SM %v %v %v", sc, &applyMsg, reply)
				// reply clerk only if it is the leader
				if reply != nil && isLeader && currTerm == applyMsg.CommandTerm {
					if ch, ok := sc.replyChanSet[applyMsg.CommandIndex]; ok {
						ch <-reply
					}
				}
			} else {
				DPanicf(shardctrler|debugError, "applyMsg ERROR %v %v", sc, &applyMsg)
			}
			sc.Unlock()
		}
	}
}

func (sc *ShardCtrler) applyCommand(op *Op) *OpReply {
	reply := &OpReply{Err: OK}
	if preOp, ok := sc.lastClntOpSet[op.ClntId]; ok {
		if preOp.Seq == op.Seq {
			reply.Config, reply.Err = preOp.ReplyConf, preOp.ReplyErr
			return reply
		}
	}
	if op.OpType == OPQuery {
		if op.Num == -1 {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else if op.Num >= 0 && op.Num < len(sc.configs) {
			reply.Config = sc.configs[op.Num]
		} else {
			reply.Err = ErrNoConfig
		}
	} else {
		sc.updateConfig(op, reply)
	}

	sc.lastClntOpSet[op.ClntId] = &OpCache{
		OpType: op.OpType,
		Seq: op.Seq, Clnt: op.ClntId,
		ReplyErr: reply.Err,
		ReplyConf: reply.Config,
	}
	return reply
}

func (sc *ShardCtrler) updateConfig(op *Op, reply *OpReply) {
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: make([]int, NShards),
		Groups: make(map[int][]string),
	}
	copy(newConfig.Shards, oldConfig.Shards)
	for GID, servers := range oldConfig.Groups {
		newConfig.Groups[GID] = servers
	}
	defer func() {sc.configs = append(sc.configs, newConfig)}()
	DPrintf(debugTest, "oldConfig:%v\t newConfig:%v", oldConfig, newConfig)

	switch op.OpType {
	case OPMove:
		if _, ok := newConfig.Groups[op.GID]; ok {
			newConfig.Shards[op.Shard] = op.GID
		} else {
			reply.Err = ErrNoGID
		}
		return
	case OPLeave:
		sc.rebalance(op, &newConfig)
		for _, GID := range op.GIDs {
			delete(newConfig.Groups, GID)
		}
	case OPJoin:
		if op.Servers == nil || len(op.Servers) == 0 {
			if len(newConfig.Groups) == 0 {
				newConfig.Shards = []int{}
			}
			return
		}
		for GID, servers := range op.Servers {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[GID] = newServers
		}
		sc.rebalance(op, &newConfig)
	default:
		DPanicf(shardctrler|debugError, "ERROR %v %v", sc, op)
	}
}

func (sc *ShardCtrler) rebalance(op *Op, conf *Config) {
	GIDShardMap := make(map[int] []int, len(conf.Groups)) // GID -> Shard
	for GID := range conf.Groups {
		GIDShardMap[GID] = make([]int, 0, 4)
	}
	for shard, GID := range conf.Shards {
		GIDShardMap[GID] = append(GIDShardMap[GID], shard)
	}
	DPrintf(debugTest, "GIDShardMap:%v", GIDShardMap)

	switch op.OpType {
	case OPLeave:
		DPrintf(debugTest, "len(conf.Groups):%v len(op.GIDs):%v", len(conf.Groups), len(op.GIDs))
		if len(conf.Groups) - len(op.GIDs) <= 0 {
			conf.Shards = []int{}
		} else {
			delGIDShards := make([]int, 0, len(op.GIDs)) // delGID -> Shard
			for _, delGID := range op.GIDs {
				delGIDShards = append(delGIDShards, GIDShardMap[delGID]...)
				delete(GIDShardMap, delGID)
			}
			DPrintf(debugTest, "GIDShardMap:%v delGIDShards:%v", GIDShardMap, delGIDShards)

			avg := NShards / float64(len(conf.Groups) - len(op.GIDs))
			avgFloor, avgCeil := int(math.Floor(avg)), int(math.Ceil(avg))
			DPrintf(debugTest, "avg:%v avgFloor:%v avgCeil:%v", avg, avgFloor, avgCeil)

			for {
				if len(delGIDShards) == 0 {
					break
				}
				minGID, minNum, _, _ := getGIDWithMinMaxShards(GIDShardMap)
				need := avgFloor - minNum
				if need <= 0 {
					need = avgCeil - minNum
				}
				DPrintf(debugTest, "delGIDShards:%p %v, conf.Shards:%p %v, minGID:%v, minNum:%v, need:%v GIDShardMap:%v", &delGIDShards, delGIDShards, &conf.Shards, conf.Shards, minGID, minNum, need, GIDShardMap)
				moveShardsBetweenGID(minGID, need, &delGIDShards, &conf.Shards, GIDShardMap)
			}
		}
	case OPJoin:
		avg := NShards / float64(len(conf.Groups))
		avgFloor, avgCeil := int(math.Floor(avg)), int(math.Ceil(avg))
		DPrintf(debugTest, "avg:%v avgFloor:%v avgCeil:%v", avg, avgFloor, avgCeil)
		if _, ok := GIDShardMap[0]; ok {
			minGID, _, _, _ := getGIDWithMinMaxShards(GIDShardMap)
			tmp := GIDShardMap[0]
			moveShardsBetweenGID(minGID, len(tmp), &tmp, &conf.Shards, GIDShardMap)
			delete(GIDShardMap, 0)
		}

		for {
			minGID, minNum, maxGID, maxNum := getGIDWithMinMaxShards(GIDShardMap)
			if maxNum <= avgCeil {
				break
			}
			need := avgFloor - minNum
			if need <= 0 {
				need = avgCeil - minNum
			}
			tmp := GIDShardMap[maxGID]
			DPrintf(debugTest, "minGID:%v, minNum:%v, maxGID:%v, need:%v delGIDShards:%v conf.Shards:%v GIDShardMap:%v", minGID, minNum, maxGID, need, tmp, conf.Shards, GIDShardMap)
			moveShardsBetweenGID(minGID, need, &tmp, &conf.Shards, GIDShardMap)
			GIDShardMap[maxGID] = tmp
		}
	}
}



// Kill the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// Raft needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// "me" is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1, NShards)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Shards = []int{}

	labgob.Register(Op{})
	labgob.Register(OpReply{})
	labgob.Register(Config{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastClntOpSet = make(clntIdOpContextMap)
	sc.replyChanSet = make(logIndexReplyChanMap)
	sc.lastApplied = -1

	go sc.run()

	DPrintf(shardctrler,"%v init", sc)
	return sc
}
