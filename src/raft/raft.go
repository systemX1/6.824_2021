package raft

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.

import (
	"../labgob"
	"../labrpc"
	"bytes"
	"fmt"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type State uint8
const (
	Follower State = iota
	Candidate
	Leader
	Dead
)
var statStr = []string{ "Follower", "Candidate", "Leader", "Dead" }
func (s State) String() string {
	return statStr[s]
}

// ApplyMsg as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (msg ApplyMsg) String() string {
	if msg.CommandValid == true {
		return fmt.Sprintf("[msg idx:%v cmd:%v]",
			msg.CommandIndex, msg.Command,
		)
	}
	return fmt.Sprintf("[msg sn idx:%v t:%v %v]",
		msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot,
	)
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	sync.Mutex          // Lock to protect shared access to this peer's state
	peers     	[]*labrpc.ClientEnd // RPC end points of all peers
	persister 	*Persister          // Object to hold this peer's persisted state
	me        	int                 // this peer's index into peers[]
	dead      	int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currTerm	int
	votedFor	int
	stat 		State
	electTimer	*time.Timer
	lastReset	time.Time
	timeMu		sync.Mutex

	rfLog     *RfLog
	nextIndex []int
	matchIndex 	[]int
	applyCh		chan ApplyMsg

	// 2D
	lastIncludedIndex int
	lastIncludedTerm  int
}

func (rf *Raft) String() string {
	if rf.stat == Leader {
		return fmt.Sprintf("[S%v %v t:%v vF:%v c:%v a:%v sn:%v %v m:%v n:%v]",
			rf.me, rf.stat, rf.currTerm, rf.votedFor,
			rf.rfLog.GetCommitIndex(), rf.rfLog.GetLastApplied(),
			rf.lastIncludedIndex, rf.lastIncludedTerm,
			rf.matchIndex, rf.nextIndex,
		)
	}
	return fmt.Sprintf("[S%v %v t:%v vF:%v c:%v a:%v sn:%v %v]",
		rf.me, rf.stat, rf.currTerm, rf.votedFor,
		rf.rfLog.GetCommitIndex(), rf.rfLog.GetLastApplied(),
		rf.lastIncludedIndex, rf.lastIncludedTerm,
	)
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()
	// Your code here (2A).
	return rf.currTerm, rf.stat == Leader
}

func (rf *Raft) setCurrTerm(term int) {
	rf.currTerm = term
	rf.setVotedFor(-1)
}

func (rf *Raft) setVotedFor(serv int) {
	rf.votedFor = serv
}

func (rf *Raft) setState(stat State) {
	switch stat {
	case Follower:
		if rf.stat == Leader {
			rf.resetElectionTimeout()
		}
	case Candidate:
		rf.resetElectionTimeout()
	case Leader:
	}
	rf.stat = stat
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	if rf.stat == Dead {
		return
	}
	rf.persister.SaveRaftState(rf.serializeStableState())
}

func (rf *Raft) serializeStableState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	var err error
	e.Encode(rf.currTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	rf.rfLog.Lock()
	err = e.Encode(rf.rfLog.Entries)
	rf.rfLog.Unlock()
	if err != nil {
		DPrintf(persist, "ERROR:%v", err)
	}
	data := w.Bytes()
	return data
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var Entries []LogEntry
	// Encode Decode顺序要相同
	var err error
	err = d.Decode(&currTerm)
	err = d.Decode(&votedFor)
	err = d.Decode(&lastIncludedIndex)
	err = d.Decode(&lastIncludedTerm)
	err = d.Decode(&Entries)
	if err != nil {
		DPrintf(persist, "%v read persist ERROR:%v %v", rf, err, rf.rfLog)
	}
	rf.currTerm = currTerm
	rf.votedFor = votedFor
	rf.rfLog.Entries = Entries
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	// 避免重复apply日志
	rf.rfLog.SetLastApplied(lastIncludedIndex)
	rf.rfLog.SetCommitIndex(lastIncludedIndex)
	DPrintf(persist, "%v read persist %v", rf, rf.rfLog)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("[arg t:%v lasInIdx:%v lasInT:%v data:%v]",
		args.Term, args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
}

type InstallSnapshotReply struct {
	Term              int
	Succ              bool
}
func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("[rly %v %v]",
		reply.Term, reply.Succ)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.HandleInstallSnapshot", args, reply)
	return ok
}

// HandleInstallSnapshot handle RPC RequestVote call from another server
func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Lock()
	defer rf.Unlock()
	defer DPrintf(snapshot, "%v %v %v %v", rf, rf.rfLog, args, reply)
	reply.Term, reply.Succ = rf.currTerm, false
	DPrintf(snapshot, "%v %v %v", rf, args, rf.rfLog)

	if args.Term > rf.currTerm {
		rf.setState(Follower)
		rf.setCurrTerm(args.Term)
		rf.persist()
	}
	if args.Term < rf.currTerm || args.LastIncludedIndex < rf.lastIncludedIndex {
		return
	}

	if args.LastIncludedIndex < rf.rfLog.GetCommitIndex() {
		DPrintf(snapshot, "%v outdated Snapshot %v", rf, args)
		return
	}

	if args.LastIncludedIndex >= rf.rfLog.GetLastEntryIndex() {
		rf.rfLog.Clear()
	} else {
		rf.rfLog.DoSnapshot(args.LastIncludedIndex)
	}

	lastApplied, commitIndex := rf.rfLog.GetLastApplied(), rf.rfLog.GetCommitIndex()
	if args.LastIncludedIndex > lastApplied {
		rf.rfLog.SetLastApplied(args.LastIncludedIndex)
	}
	if args.LastIncludedIndex > commitIndex {
		rf.rfLog.SetCommitIndex(args.LastIncludedIndex)
	}

	rf.lastIncludedIndex, rf.lastIncludedTerm = args.LastIncludedIndex, args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.serializeStableState(), args.Data)
	reply.Succ = true
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex + 1,
		}
		DPrintf(applyClient, "%v %v %v", rf, rf.rfLog, args)
	}()
}

// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// had more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int,
	lastIncludedIndex int, data []byte) bool {
	// Your code here (2D).
	lastIncludedIndex--
	rf.Lock()
	defer rf.Unlock()
	defer DPrintf(snapshot, "%v lasInIdx:%v lasInT:%v data:%v", rf, lastIncludedIndex, lastIncludedTerm, data)

	if lastIncludedIndex < rf.rfLog.GetCommitIndex() {
		DPrintf(snapshot, "%v outdated Snapshot lasInIdx:%v lasInT:%v data:%v", rf, lastIncludedIndex, lastIncludedTerm, data)
		return false
	}

	DPrintf(snapshot, "%v lasInIdx:%v lasInT:%v data:%v", rf, lastIncludedIndex, lastIncludedTerm, data)
	if lastIncludedIndex >= rf.rfLog.GetLastEntryIndex() {
		rf.rfLog.Clear()
	} else {
		rf.rfLog.DoSnapshot(lastIncludedIndex)
	}

	lastApplied, commitIndex := rf.rfLog.GetLastApplied(), rf.rfLog.GetCommitIndex()
	if lastIncludedIndex > lastApplied {
		rf.rfLog.SetLastApplied(lastIncludedIndex)
	}
	if lastIncludedIndex > commitIndex {
		rf.rfLog.SetCommitIndex(lastIncludedIndex)
	}

	rf.lastIncludedIndex, rf.lastIncludedTerm = lastIncludedIndex, lastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.serializeStableState(), data)
	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, data []byte) {
	// Your code here (2D).
	index--
	rf.Lock()
	defer rf.Unlock()
	DPrintf(snapshot, "%v idx:%v data:%v %v", rf, index, data, rf.rfLog)
	ok, entryIdx, entryTerm := rf.rfLog.DoSnapshot(index)
	if ok {
		rf.persister.SaveStateAndSnapshot(rf.serializeStableState(), data)
		rf.lastIncludedIndex, rf.lastIncludedTerm = entryIdx, entryTerm
		DPrintf(snapshot, "%v snapshot succ %v", rf, rf.rfLog)
	} else {
		DPrintf(snapshot, "%v snapshot failed %v", rf, rf.rfLog)
	}
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int
	CandidateID  	int
	LastLogIndex 	int
	LastLogTerm  	int
}
func (arg *RequestVoteArgs) String() string {
	return fmt.Sprintf("[Varg t:%v lasLogIdx:%v lasLogT:%v]",
		arg.Term, arg.LastLogIndex, arg.LastLogTerm)
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term 			int
	VoteGranted 	bool
}
func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("[Vrly t:%v %v]",
		reply.Term, reply.VoteGranted)
}

// start an election
func (rf *Raft) startElection() {
	// Your code here (2A, 2B).
	rf.Lock()
	defer rf.Unlock()
	defer DPrintf(persist, "%v save persist %v", rf, rf.rfLog)
	defer rf.persist()

	rf.setState(Candidate)
	rf.setCurrTerm(rf.currTerm + 1)
	rf.setVotedFor(rf.me)
	lastLogIndex := rf.getLastEntryIndex()
	lastLogTerm := rf.getLastEntryTerm()
	DPrintf(requsetVote, "%v is starting an election", rf)
	votes := 1
	done := false
	for i := range rf.peers {
		if i == rf.me {
			DPrintf(requsetVote, "%v votes to itself, votes:%v", rf, votes)
			continue
		}
		go func(idx, term, me, lastLogIndex, lastLogTerm int, stat State) {
			votedGranted := rf.startRequestVote(idx, term, me, lastLogIndex, lastLogTerm, stat)
			if !votedGranted { return }
			// tally the votes
			rf.Lock()
			defer rf.Unlock()
			votes++
			DPrintf(requsetVote, "%v got vote from %v, votes:%v", rf, idx, votes)
			if done || votes <= len(rf.peers) / 2 { return }
			done = true
			if rf.stat != Candidate || rf.currTerm != term {
				DPrintf(requsetVote, "%v back to Follower", rf)
				return
			}
			rf.setState(Leader)
			rf.initPeerLogIndex()
			DPrintf(requsetVote, "%v WON the election, votes:%v, peers:%v, RLogs:%v", rf, votes, len(rf.peers), rf.rfLog)
		}(i, rf.currTerm, rf.me, lastLogIndex, lastLogTerm, rf.stat)
	}
}

// don't hold any locks thought any RPC calls for deadlock avoidance
func (rf *Raft) startRequestVote(serv, term, me, lastLogIndex, lastLogTerm int, stat State) bool {
	rf.DMutexPrintf(requsetVote, "%v is sending an RequestVote to %v", rf, serv)
	args := RequestVoteArgs{
		Term: term, 	CandidateID: me,
		LastLogIndex: 	lastLogIndex,
		LastLogTerm: 	lastLogTerm,
	}
	var reply RequestVoteReply
	if ok := rf.sendRequestVote(serv, &args, &reply); !ok || reply.Term > term {
		rf.DMutexPrintf(requsetVote, "%v sendRequestVote to S%v failed", rf, serv)
		return false
	}
	// Term confusion: drop the reply and return if the term and state has changed
	rf.Lock()
	defer rf.Unlock()
	if rf.currTerm != term || rf.stat != stat {
		return false
	}
	DPrintf(requsetVote, "%v sendRequestVote to S%v succ", rf, serv)
	return reply.VoteGranted
}

// Example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// Fills in *reply with RPC reply, so caller should pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus, Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus, there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(serv int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[serv].Call("Raft.HandleRequestVote", args, reply)
	return ok
}

// HandleRequestVote handle RPC RequestVote call from another server
func (rf *Raft) HandleRequestVote(arg *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	defer rf.Unlock()
	defer DPrintf(persist, "%v save persist %v", rf, rf.rfLog)
	defer rf.persist()
	DPrintf(requsetVote, "%v received RequestVote RPC from %v, %v", rf, arg.CandidateID, arg)
	reply.Term = rf.currTerm

	lastLogIndex := rf.getLastEntryIndex()
	lastLogTerm := rf.getLastEntryTerm()
	DPrintf(requsetVote, "%v lasLogIdx:%v lasLogT:%v", rf, lastLogIndex, lastLogTerm)

	if (arg.Term < rf.currTerm) || (arg.LastLogTerm < lastLogTerm ||
		(arg.LastLogTerm == lastLogTerm && arg.LastLogIndex < lastLogIndex) ) {
		reply.VoteGranted =	false
		if arg.Term > rf.currTerm {
			rf.setState(Follower)
			rf.setCurrTerm(arg.Term)
		}
	} else if (rf.votedFor == -1 && rf.stat == Follower) || (arg.Term > rf.currTerm) {
		rf.setState(Follower)
		rf.setCurrTerm(arg.Term)
		reply.VoteGranted = true
		rf.setVotedFor(arg.CandidateID)
		rf.resetElectionTimeout()
		DPrintf(requsetVote, "%v VOTE to %v", rf, arg.CandidateID)
	}
}

// AppendEntriesArgs AppendEntries RPC
type AppendEntriesArgs struct {
	Term 			int
	LeaderID	 	int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries     	[]LogEntry
	LeaderCommit 	int
}
func (arg *AppendEntriesArgs) String() string {
	return fmt.Sprintf("[arg S%v term:%v preLogIdx:%v preLogT:%v %v LComit:%v]",
		arg.LeaderID, arg.Term, arg.PrevLogIndex, arg.PrevLogTerm,
		arg.Entries, arg.LeaderCommit)
}

// AppendEntriesReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    		int
	Success     	bool
	Incoist     	bool
	TermIncoist 	bool
	NextIndex    	int
}
func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("[rply term:%v succ:%v Incoist:%v tIncoist:%v n:%v]",
		reply.Term, reply.Success, reply.Incoist, reply.TermIncoist, reply.NextIndex)
}

func (rf *Raft) startLogReplication()  {
	rf.Lock()
	defer rf.Unlock()
	defer DPrintf(persist, "%v save persist %v", rf, rf.rfLog)
	defer rf.persist()
	DPrintf(logReplicate, "%v startLogReplication", rf)
	leaderCommit := rf.rfLog.GetCommitIndex()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(serv, currTerm, me, LeaderCommit int, stat State) {
			rf.Lock()
			prevLogIndex := -1
			prevLogTerm := -1
			if rf.nextIndex[serv] > 0 {
				prevLogIndex = rf.nextIndex[serv] - 1
			}

			var entries []LogEntry
			if rf.nextIndex[serv] - rf.matchIndex[serv] == 1 {
				tmp := rf.rfLog.GetUncommited(rf.nextIndex[serv])
				entries = make([]LogEntry, len(tmp))
				copy(entries, tmp)
			}
			lastIncludedIndex, lastIncludedTerm := rf.lastIncludedIndex, rf.lastIncludedTerm
			rf.Unlock()
			snapshotArgs := &InstallSnapshotArgs{
				Term:              currTerm,
				LeaderId:          me,
				LastIncludedIndex: lastIncludedIndex,
				LastIncludedTerm:  lastIncludedTerm,
				Data:              rf.persister.ReadSnapshot(),
			}
			if prevLogIndex < lastIncludedIndex {
				if ok := rf.startSendSnapshot(serv, snapshotArgs, stat); !ok {
					return
				}
			}

			rf.Lock()
			if rf.nextIndex[serv] > 0 {
				prevLogIndex = rf.nextIndex[serv] - 1
				prevLogTerm = rf.rfLog.GetEntryTerm(prevLogIndex)
			}
			if prevLogIndex == rf.lastIncludedIndex {
				prevLogTerm = rf.lastIncludedTerm
			}
			rf.Unlock()
			if ok := rf.startAppendEntries(serv, currTerm, me, prevLogIndex, prevLogTerm, LeaderCommit, entries, stat); !ok {
				return
			}
		}(i, rf.currTerm, rf.me, leaderCommit, rf.stat)
	}
}

func(rf *Raft) startSendSnapshot(serv int, args *InstallSnapshotArgs, stat State) bool {
	reply := &InstallSnapshotReply{}
	DPrintf(snapshot, "%v sendSnapshot to S%v, %v", rf, serv, args)
	if ok := rf.sendInstallSnapshot(serv, args, reply); !ok {
		DPrintf(snapshot, "%v sendSnapshot to S%v failed, %v %v", rf, serv, args, reply)
		return false
	}

	rf.Lock()
	defer rf.Unlock()
	if rf.currTerm != args.Term || rf.stat != stat {
		return false
	}
	if reply.Term > rf.currTerm {
		rf.setCurrTerm(reply.Term)
		rf.setState(Follower)
		return false
	}
	rf.nextIndex[serv] = args.LastIncludedIndex + 1
	rf.matchIndex[serv] = rf.nextIndex[serv] - 1
	DPrintf(snapshot, "%v sendSnapshot to S%v succ, %v %v", rf, serv, args, reply)
	return true
}

func(rf *Raft) startAppendEntries(serv, currTerm, me, prevLogIndex, prevLogTerm,
	leaderCommit int, entries []LogEntry, stat State) bool {
	rf.Lock()

	args := &AppendEntriesArgs{
		Term: currTerm, 			LeaderID: me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries: entries,
		LeaderCommit: leaderCommit,
	}
	reply := &AppendEntriesReply{}
	DPrintf(logReplicate, "%v sendAppendEntries to S%v, %v", rf, serv, args)
	rf.persist()
	DPrintf(persist, "%v save persist %v", rf, rf.rfLog)
	rf.Unlock()

	ok := rf.sendAppendEntries(serv, args, reply)
	rf.Lock()
	defer rf.Unlock()
	// Term confusion: drop the reply and return if the term and state has changed
	if rf.currTerm != currTerm || rf.stat != stat {
		return false
	}
	defer DPrintf(persist, "%v save persist %v", rf, rf.rfLog)
	defer rf.persist()
	if reply.Term > rf.currTerm {
		rf.setCurrTerm(reply.Term)
		rf.setState(Follower)
		return false
	}
	if !ok {
		return false
	}
	// NOTE: 因为RPC期间无锁, 可能相关状态被其他RPC修改了
	if !reply.Success &&
		args.PrevLogIndex == rf.nextIndex[serv] - 1 &&
		args.PrevLogTerm == rf.rfLog.GetEntryTerm(prevLogIndex) {
		if reply.Incoist == true {
			rf.nextIndex[serv]--
			if reply.TermIncoist == true {
				rf.nextIndex[serv] = reply.NextIndex
				// RPC乱序 先发的可能后返回
				rf.matchIndex[serv] = rf.nextIndex[serv] - 1
			}
		}
		DPrintf(debugError|logReplicate, "%v sendAppendEntries to S%v failed, %v %v",
			rf, serv, args, reply)
		return false
	}
	// NOTE: 因为RPC期间无锁, 可能相关状态被其他RPC修改了
	// 因此这里得根据发出RPC请求时的状态做更新，而不要直接对nextIndex和matchIndex做相对加减
	rf.nextIndex[serv] = args.PrevLogIndex + len(args.Entries) + 1
	rf.matchIndex[serv] = rf.nextIndex[serv] - 1

	rf.checkCommit()
	DPrintf(logReplicate, "%v sendAppendEntries to %v succ, %v %v", rf, serv, args, reply)
	return true
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()
	defer DPrintf(logReplicate, "%v HandleAppendEntries DONE %v %v %v", rf, rf.rfLog, args, reply)
	defer DPrintf(persist, "%v save persist %v", rf, rf.rfLog)
	defer rf.persist()
	reply.Success, reply.Incoist, reply.TermIncoist, reply.Term = false, false, false, rf.currTerm
	DPrintf(logReplicate, "%v Receive AppendEntries %v %v", rf, rf.rfLog, args)
	if args.Term < rf.currTerm {
		return
	} else if args.Term > rf.currTerm {
		rf.setCurrTerm(args.Term)
		rf.setState(Follower)
	}

	rf.resetElectionTimeout()
	if args.PrevLogIndex < rf.lastIncludedIndex ||
		(args.PrevLogIndex == rf.lastIncludedIndex &&
			args.PrevLogTerm != rf.lastIncludedTerm) {
		reply.Incoist, reply.TermIncoist, reply.NextIndex = true, true, rf.lastIncludedIndex - 1
		return
	}
	if rf.lastIncludedIndex != -1 &&
		args.PrevLogIndex == rf.lastIncludedIndex &&
		args.PrevLogTerm == rf.lastIncludedTerm {
		reply.Success = true
		rf.rfLog.Clear()
		rf.rfLog.AppendEntries2(args.Entries)
		return
	}

	if ok1, ok2 := rf.rfLog.CheckAppendEntries(args.PrevLogIndex, args.PrevLogTerm); !ok1 {
		reply.Incoist = true
		if !ok2 {
			reply.TermIncoist = true
			reply.NextIndex = rf.rfLog.ConflictingEntryTermIndex(args.PrevLogTerm)
			if rf.lastIncludedIndex > reply.NextIndex {
				reply.NextIndex = rf.lastIncludedIndex + 1
			}
		}
		DPrintf(logReplicate, "ConflictingEntry %v %v %v", rf, rf.rfLog, reply)
		return
	}
	reply.Success = true

	DPrintf(heartbeat|logReplicate, "%v reset electionTimeout", rf)
	rf.rfLog.TruncateAppend(args.PrevLogIndex, args.Entries)

	if args.LeaderCommit > rf.rfLog.GetCommitIndex() {
		lastEntryIndex := rf.getLastEntryIndex()
		rf.rfLog.SetCommitIndex(min(args.LeaderCommit, lastEntryIndex) )
	}
}

func (rf *Raft) sendAppendEntries(serv int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[serv].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}

func (rf *Raft) initPeerLogIndex() {
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.getLastEntryIndex() + 1
		rf.matchIndex[i] = -1
	}
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
}

func (rf *Raft) getLastEntryIndex() int {
	lastEntryIndex := rf.rfLog.GetLastEntryIndex()
	if lastEntryIndex == -1 && rf.lastIncludedIndex > -1 {
		lastEntryIndex = rf.lastIncludedIndex
	}
	return lastEntryIndex
}

func (rf *Raft) getLastEntryTerm() int {
	lastEntryTerm := rf.rfLog.GetLastEntryTerm()
	if lastEntryTerm == -1 && rf.lastIncludedTerm > -1 {
		lastEntryTerm = rf.lastIncludedTerm
	}
	return lastEntryTerm
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise, start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.Lock()
	//defer rf.startLogReplication()
	defer rf.Unlock()
	// Your code here (2B).
	if rf.stat != Leader {
		return rf.getLastEntryIndex() + 1, rf.currTerm, false
	}
	DPrintf(client, "%v: Client start to append command %v, %v", rf, command, rf.rfLog)
	defer DPrintf(persist, "%v save persist %v", rf, rf.rfLog)
	defer rf.persist()
	rf.rfLog.AppendEntries(rf.lastIncludedIndex, LogEntry{Term: rf.currTerm, Command: command})
	rf.nextIndex[rf.me] += 1
	rf.matchIndex[rf.me] += 1
	return rf.getLastEntryIndex() + 1, rf.currTerm, true
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.Lock()
	DPrintf(client, "%v is killed %v %v", rf, rf.rfLog, time.Now().Sub(rf.lastReset))
	rf.stat = Dead
	rf.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers: peers, 	persister: persister,
		me: me, 		votedFor: -1,
		electTimer: newTimer(),
		lastReset: time.Now(),
		nextIndex: make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		lastIncludedIndex: -1,
		lastIncludedTerm: -1,
		rfLog: NewRaftLog(),
		applyCh: applyCh,
	}

	// Your initialization code here (2A, 2B, 2C).
	DPrintf(client,"%v init", rf)
	go rf.run()
	go rf.applyClient(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf *Raft) run() {
	heartbeatTicker := time.Tick(heartbeatTimeout)
	rf.resetElectionTimeout()
	for {
		if rf.killed() {
			rf.DMutexPrintf(client, "%v stop running", rf)
			PrintLine()
			return
		}
		select {
		case <-heartbeatTicker:
			rf.Lock()
			if rf.stat != Leader {
				rf.Unlock()
				break
			}
			DPrintf(heartbeat, "%v sending heartbeat msg", rf)
			rf.Unlock()
			rf.startLogReplication()

		case t1 := <-rf.electTimer.C:
			rf.Lock()
			if rf.stat == Leader {
				rf.Unlock()
				break
			}
			DPrintf(requsetVote, "%v ElectionTimeout %v %v",
				rf, getTimeOffset(t1), time.Now().Sub(rf.lastReset))
			rf.Unlock()
			rf.startElection()
		}
	}
}

func (rf *Raft) applyClient(applyCh chan<- ApplyMsg) {
	heartbeatTicker := time.Tick(heartbeatTimeout)
	for {
		if rf.killed() {
			rf.DMutexPrintf(applyClient, "S%v stop applyClient", rf.me)
			PrintLine2()
			return
		}
		select {
		case <-heartbeatTicker:
			lastApplied := rf.rfLog.GetLastApplied()
			commitIndex := rf.rfLog.GetCommitIndex()
			for lastApplied < commitIndex {
				DPrintf(applyClient, "%v lastApplied:%v, commitIndex:%v", rf, lastApplied, commitIndex)
				lastApplied = rf.rfLog.SetLastApplied(lastApplied + 1)
				command, cmdTerm := rf.rfLog.GetEntryCommand(lastApplied), rf.rfLog.GetEntryTerm(lastApplied)
				if command == nil {
					continue
				}
				applyMsg := ApplyMsg{
					CommandValid: true,
					CommandIndex: lastApplied + 1,
					Command:      command,
					CommandTerm:  cmdTerm,
				}
				DPrintf(applyClient, "%v rfLogLen:%v %v", rf, rf.rfLog.Len(), applyMsg)
				applyCh <- applyMsg
			}
		}
	}
}

func (rf *Raft) checkCommit() {
	m := majority(rf.matchIndex)
	for n := rf.rfLog.GetCommitIndex() + 1; n <= m; n++ {
		if rf.rfLog.GetEntryTerm(n) == rf.currTerm {
			DPrintf(debugInfo, "SetCommitIndex %v %v", rf, rf.rfLog)
			rf.rfLog.SetCommitIndex(n)
		}
	}
}

// wrap
func (rf *Raft) resetElectionTimeout()  {
	rf.timeMu.Lock()
	defer rf.timeMu.Unlock()
	stopResetTimer(rf.electTimer, GetElectionTimeout())
	rf.lastReset = time.Now()
	funcName, _, line, _ := runtime.Caller(1)
	funcNameStr := path.Base(runtime.FuncForPC(funcName).Name())
	funcName2, _, line2, _ := runtime.Caller(2)
	funcNameStr2 := path.Base(runtime.FuncForPC(funcName2).Name())
	DPrintf(requsetVote, "%d %s %d %s @resetElectionTimeout S%v",
		line, funcNameStr, line2, funcNameStr2, rf.me)
}
