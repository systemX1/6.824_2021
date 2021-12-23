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

func (rL *RfLog) AppendEntries(entries ...LogEntry) {
	rL.Lock()
	defer rL.Unlock()
	nextIdx := 0
	if next := rL.getLastEntry(Index); next != nil {
		nextIdx = next.(int) + 1
	}
	for _, entry := range entries {
		entry.Index = nextIdx
		nextIdx++
		rL.Entries = append(rL.Entries, entry)
		DPrintf(logReplicate, "%v %v", entry, rL.Entries)
	}
}

// CheckAppendEntries return if prevLogIndex and lastLogTerm correspond to paras
func (rL *RfLog) CheckAppendEntries(prevLogIndex, prevLogTerm int) (bool, bool) {
	rL.Lock()
	defer rL.Unlock()
	lastEntry := rL.getLastEntry(Pointer)
	lastEntryTerm := -1
	if lastEntry != nil {
		lastEntryTerm = lastEntry.(*LogEntry).Index
	}
	//if prevLogIndex == -1 || lastEntry == nil {
	//	return true, true
	//}
	if prevLogIndex == -1 {
		return true, true
	}

	myPrevLogTerm := -1
	if prevLog := rL.getEntry(prevLogIndex, Term); prevLog != nil {
		myPrevLogTerm = prevLog.(int)
	}

	if myPrevLogTerm != prevLogTerm {
		return false, false
	}
	if prevLogIndex > lastEntryTerm {
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
	DPrintf(debugInfo|logReplicate, "%v preLgIdx:%v %v", rL.Entries, prevLogIndex, entries)
	if prevLogIndex < 0 {
		rL.Entries = entries
		DPrintf(debugInfo|logReplicate, "%v", rL.Entries)
		return
	}

	// remove conflict Entries
	if entries == nil && rL.Entries != nil && prevLogIndex + 1 < len(rL.Entries) {
		rL.Entries = rL.Entries[:prevLogIndex + 1]
	}
	for i, j := prevLogIndex + 1, 0;
		i < prevLogIndex + len(entries) && i < len(rL.Entries);
		i, j = i + 1, j + 1 {
		if rL.Entries[i].Term != entries[j].Term {
			rL.Entries = rL.Entries[:i]
			break
		}
	}
	rL.Entries = append(rL.Entries, entries...)
	DPrintf(debugInfo|logReplicate, "%v ", rL.Entries)
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
	if nextIndex >= len(rL.Entries) || rL.Entries == nil || len(rL.Entries) == 0 {
		return nil
	}
	nextIndex = binarySearch(rL.Entries, nextIndex)
	if nextIndex == -1 {
		return nil
	}
	entries = rL.Entries[nextIndex:]
	return entries
}

func (rL *RfLog) getEntry(index int, typ LogEntryItem) interface{} {
	entry := sort.Search(len(rL.Entries), func(i int) bool { return rL.Entries[i].Index >= index })
	if entry < len(rL.Entries) && rL.Entries[entry].Index == index {
		switch typ {
		case Pointer:
			return &rL.Entries[entry]
		case Term:
			return rL.Entries[entry].Term
		case Command:
			return rL.Entries[entry].Command
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
	return rL.getEntry(index, Command)
}

func (rL *RfLog) String() string {
	rL.Lock()
	defer rL.Unlock()
	return fmt.Sprintf("[RLog c:%v a:%v %v]",
		rL.commitIndex, rL.lastApplied, rL.Entries)
}

type LogEntry struct {
	Index 			int
	Term    		int
	Command 		interface{}
}

type LogEntryItem uint8
const (
	Pointer	LogEntryItem = iota
	Index
	Term
	Command
)

func binarySearch(entries []LogEntry, index int) int {
	entry := sort.Search(len(entries), func(i int) bool { return entries[i].Index >= index })
	if entry < len(entries) && entries[entry].Index == index {
		return entry
	}
	return -1
}

//func backwardSearch(Entries []LogEntry, index int) int {
//	for i := len(Entries) - 1; i >= 0; i-- {
//		if Entries[i].Index == index {
//			return i
//		}
//	}
//	return -1
//}


