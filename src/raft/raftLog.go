package raft

import (
	"fmt"
	"sort"
	"sync"
)

type RfLog struct {
	commitIndex 	int
	lastApplied 	int
	Entries     	[]LogEntry
	sync.Mutex
}

func NewRaftLog() *RfLog {
	rL := &RfLog{commitIndex: -1, lastApplied: -1}
	rL.Entries = make([]LogEntry, 0, 20)
	return rL
}

func (rL *RfLog) Clear() {
	rL.Lock()
	defer rL.Unlock()
	rL.Entries = make([]LogEntry, 0, 20)
}

func (rL *RfLog) Len() int {
	rL.Lock()
	defer rL.Unlock()
	return len(rL.Entries)
}

func (rL *RfLog) AppendEntries(lastIncludedIndex int, entries ...LogEntry) {
	rL.Lock()
	defer rL.Unlock()
	nextIdx := 0
	if next := rL.getLastEntry(Index); next != nil {
		nextIdx = next.(int) + 1
	}
	if rL.Entries == nil || len(rL.Entries) == 0 {
		nextIdx = lastIncludedIndex + 1
	}
	for _, entry := range entries {
		if entry.Index != -1 {
			entry.Index = nextIdx
		}
		nextIdx++
		rL.Entries = append(rL.Entries, entry)
		DPrintf(raftLog, "%v %v", entry, rL.Entries)
	}
}

func (rL *RfLog) AppendEntries2(entries []LogEntry) {
	rL.Lock()
	defer rL.Unlock()
	rL.Entries = append(rL.Entries, entries...)
}

// CheckAppendEntries return if prevLogIndex and lastLogTerm correspond to paras
func (rL *RfLog) CheckAppendEntries(prevLogIndex, prevLogTerm int) (bool, bool) {
	rL.Lock()
	defer rL.Unlock()
	if prevLogIndex == -1 {
		return true, true
	}

	lastEntry := rL.getLastEntry(Pointer)
	lastEntryTerm := -1
	if lastEntry != nil {
		lastEntryTerm = lastEntry.(*LogEntry).Index
	}

	myPrevLogTerm := -1
	if prevLog := rL.getEntry(prevLogIndex, Term); prevLog != nil {
		myPrevLogTerm = prevLog.(int)
	}

	if myPrevLogTerm != prevLogTerm {
		DPrintf(logReplicate|debugError, "myPrevLogTerm:%v prevLogTerm:%v", myPrevLogTerm, prevLogTerm)
		return false, false
	}
	if prevLogIndex > lastEntryTerm {
		DPrintf(logReplicate|debugError, "prevLogIndex:%v lastEntryTerm:%v", prevLogIndex, lastEntryTerm)
		return false, true
	}
	return true, true
}

func (rL *RfLog) ConflictingEntryTermIndex(lastLogTerm int) int {
	entry := sort.Search(len(rL.Entries), func(i int) bool { return rL.Entries[i].Term >= lastLogTerm })
	if entry < len(rL.Entries) && rL.Entries[entry].Term == lastLogTerm {
		i := 0
		for i = entry; i < len(rL.Entries) && rL.Entries[i].Term == lastLogTerm; i++ {}
		return i - 1
	}
	return 0
}

func (rL *RfLog) TruncateAppend(prevLogIndex int, entries []LogEntry) {
	rL.Lock()
	defer rL.Unlock()
	if prevLogIndex < 0 {
		rL.Entries = entries
		return
	}

	// binarySearch, tran to real index
	lastEntryIndex := -1
	if rL.Entries != nil && len(rL.Entries) > 0 {
		lastEntryIndex = len(rL.Entries) - 1
	}
	entryIdx := sort.Search(len(rL.Entries), func(i int) bool { return rL.Entries[i].Index >= prevLogIndex })
	if entryIdx < len(rL.Entries) && rL.Entries[entryIdx].Index == prevLogIndex {
		prevLogIndex = entryIdx
	}
	DPrintf(raftLog, "lastEntryIndex:%v entryIdx:%v", lastEntryIndex, entryIdx)

	// remove conflict Entries
	if entries == nil && rL.Entries != nil && prevLogIndex + 1 <= lastEntryIndex {
		rL.Entries = rL.Entries[:prevLogIndex + 1]
	}
	i, j := prevLogIndex + 1, 0
	for ;
		i < prevLogIndex + len(entries) && i <= lastEntryIndex;
		i, j = i + 1, j + 1 {
		DPrintf(raftLog, "i:%v j:%v", i, j)
		if rL.Entries[i].Term != entries[j].Term {
			break
		}
	}
	DPrintf(raftLog, "i:%v j:%v fin", i, j)
	// prevent overlap
	rL.Entries = rL.Entries[:i]
	entries = entries[j:]
	// append
	rL.Entries = append(rL.Entries, entries...)
	return
}

func (rL *RfLog) DoSnapshot(index int) (bool, int, int) {
	rL.Lock()
	defer rL.Unlock()
	if len(rL.Entries) == 0 {
		return false, -1, -1
	}
	entryIdx := binarySearch(rL.Entries, index)
	if entryIdx == -1 {
		return false, -1, -1
	}
	entryTerm := rL.Entries[entryIdx].Term
	rL.Entries = rL.Entries[entryIdx+1:]
	return true, index, entryTerm
}

func (rL *RfLog) getLastEntry(typ LogEntryItem) interface{} {
	entry := -1
	for i := len(rL.Entries) - 1; i >= 0; i-- {
		if rL.Entries[i].Index != -1 {
			entry = i
			break
		}
	}
	if entry == -1 {
		return nil
	}
	if entry < len(rL.Entries) {
		switch typ {
		case Pointer:
			return &rL.Entries[entry]
		case Index:
			return rL.Entries[entry].Index
		case Term:
			return rL.Entries[entry].Term
		case Command:
			return rL.Entries[entry].Command
		}
	}
	return nil
}

func (rL *RfLog) GetLastEntryPointer() *LogEntry {
	rL.Lock()
	defer rL.Unlock()
	entry := rL.getLastEntry(Pointer)
	if entry != nil {
		return entry.(*LogEntry)
	}
	return nil
}

func (rL *RfLog) GetLastEntryIndex() int {
	rL.Lock()
	defer rL.Unlock()
	entry := rL.getLastEntry(Index)
	if entry != nil {
		return entry.(int)
	}
	return -1
}

func (rL *RfLog) GetLastEntryTerm() int {
	rL.Lock()
	defer rL.Unlock()
	entry := rL.getLastEntry(Term)
	if entry != nil {
		return entry.(int)
	}
	return -1
}

func (rL *RfLog) GetLastEntryCommand() interface{} {
	rL.Lock()
	defer rL.Unlock()
	return rL.getLastEntry(Command)
}

func (rL *RfLog) GetCommitIndex() int {
	rL.Lock()
	defer rL.Unlock()
	return rL.commitIndex
}

func (rL *RfLog) SetCommitIndex(i int) int {
	rL.Lock()
	defer rL.Unlock()
	rL.commitIndex = i
	return rL.commitIndex
}

func (rL *RfLog) GetLastApplied() int {
	rL.Lock()
	defer rL.Unlock()
	return rL.lastApplied
}

func (rL *RfLog) SetLastApplied(i int) int {
	rL.Lock()
	defer rL.Unlock()
	rL.lastApplied = i
	return rL.lastApplied
}

func (rL *RfLog) GetUncommited(nextIndex int) (entries []LogEntry) {
	rL.Lock()
	defer rL.Unlock()
	if rL.Entries == nil ||
		len(rL.Entries) == 0 ||
		nextIndex > rL.Entries[len(rL.Entries)-1].Index {
		return nil
	}
	nextIndex = binarySearch(rL.Entries, nextIndex)
	if nextIndex == -1 {
		return nil
	}
	entries = rL.Entries[nextIndex:]
	return entries
}

func (rL *RfLog) GetUnappliedCopy() (entries []LogEntry) {
	entryIdx := sort.Search(len(rL.Entries), func(i int) bool { return rL.Entries[i].Index >= rL.lastApplied })
	if entryIdx < len(rL.Entries) && rL.Entries[entryIdx].Index == rL.lastApplied  {
		length := rL.commitIndex - rL.lastApplied
		entries := make([]LogEntry, length)
		copy(entries, rL.Entries[entryIdx + 1:entryIdx + 1 + length])
	}
	return entries
}

func (rL *RfLog) getEntry(index int, typ LogEntryItem) interface{} {
	entryIdx := sort.Search(len(rL.Entries), func(i int) bool { return rL.Entries[i].Index >= index })
	if entryIdx < len(rL.Entries) && rL.Entries[entryIdx].Index == index {
		switch typ {
		case Pointer:
			return &rL.Entries[entryIdx]
		case Term:
			return rL.Entries[entryIdx].Term
		case Command:
			return rL.Entries[entryIdx].Command
		case Type:
			return rL.Entries[entryIdx].Type
		}
	}
	return nil
}

func (rL *RfLog) GetEntryPointer(index int) *LogEntry {
	rL.Lock()
	defer rL.Unlock()
	entry := rL.getEntry(index, Pointer)
	if entry != nil {
		return entry.(*LogEntry)
	}
	return nil
}

func (rL *RfLog) GetEntryTerm(index int) int {
	rL.Lock()
	defer rL.Unlock()
	term := rL.getEntry(index, Term)
	if term != nil {
		return term.(int)
	}
	return -1
}

func (rL *RfLog) GetEntryCommand(index int) interface{} {
	rL.Lock()
	defer rL.Unlock()
	if entry :=rL.getEntry(index, Type); entry != nil && entry.(LogEntryType) == Noop {
		return nil
	}
	return rL.getEntry(index, Command)
}

func (rL *RfLog) String() string {
	rL.Lock()
	defer rL.Unlock()
	return fmt.Sprintf("[RLog c:%v a:%v len:%v %v]",
		rL.commitIndex, rL.lastApplied, len(rL.Entries), rL.Entries)
}

type LogEntryType uint8
const (
	Normal LogEntryType = iota
	Noop
)
type LogEntry struct {
	Index 	int
	Term    int
	Command interface{}
	Type	LogEntryType
}
func (e LogEntry) String() string {
	return fmt.Sprintf("{%v %v %v}", e.Index, e.Term, e.Command)
}

type LogEntryItem uint8
const (
	Pointer LogEntryItem = iota
	Index
	Term
	Command
	Type
)

func binarySearch(entries []LogEntry, index int) int {
	entry := sort.Search(len(entries), func(i int) bool { return entries[i].Index >= index })
	DPrintf(debugError, "found:%v", entry)
	if entry < len(entries) && entries[entry].Index == index {
		return entry
	}
	return -1
}


