package raft

import (
	"fmt"
	"sort"
	"sync"
)

type RaftLog struct {
	commitIndex 	int
	lastApplied 	int
	entries     	[]LogEntry
	sync.Mutex
}

func NewRaftLog() *RaftLog {
	rL := &RaftLog{commitIndex: -1, lastApplied: -1}
	rL.entries = make([]LogEntry, 0, 20)
	return rL
}

func (rL *RaftLog) AppendEntries(es ...LogEntry) {
	rL.Lock()
	defer rL.Unlock()
	next := len(rL.entries)
	for _, e := range es {
		e.Index = next
		next++
		rL.entries = append(rL.entries, e)
		DPrintf(logReplicate, "%v %v", e, rL.entries)
	}
}

// return if prevLogIndex and lastLogTerm correspond to paras
func (rL *RaftLog) CheckAppendEntries(prevLogIndex, lastLogTerm int) (bool, bool) {
	rL.Lock()
	defer rL.Unlock()
	lastEntry := rL.getLastEntry(Pointer)
	if prevLogIndex == -1 || lastEntry == nil {
		return true, true
	}

	prevLogTerm := -1
	if prevLog := rL.getEntry(prevLogIndex, Term); prevLog != nil {
		prevLogTerm = prevLog.(int)
	}

	if prevLogTerm != lastLogTerm {
		return false, false
	}
	if prevLogIndex > lastEntry.(*LogEntry).Index {
		return false, true
	}
	return true, true
}

func (rL *RaftLog) ConflictingEntryTermIndex(lastLogTerm int) int {
	entry := sort.Search(len(rL.entries), func(i int) bool { return rL.entries[i].Term >= lastLogTerm })
	if entry < len(rL.entries) && rL.entries[entry].Term == lastLogTerm {
		i := 0
		for i = entry; i < len(rL.entries) && rL.entries[i].Term == lastLogTerm; i++ {}
		return i - 1
	}
	return 0
}

func (rL *RaftLog) TruncateAppend(prevLogIndex int, entries []LogEntry) {
	rL.Lock()
	defer rL.Unlock()
	DPrintf(debugInfo|logReplicate, "%v preLgIdx:%v %v", rL.entries, prevLogIndex, entries)
	if prevLogIndex < 0 {
		rL.entries = entries
		DPrintf(debugInfo|logReplicate, "%v", rL.entries)
		return
	}

	// remove conflict entries
	if entries == nil && rL.entries != nil && prevLogIndex + 1 < len(rL.entries) {
		rL.entries = rL.entries[:prevLogIndex + 1]
	}
	for i, j := prevLogIndex + 1, 0;
		i < prevLogIndex + len(entries) && i < len(rL.entries);
		i, j = i + 1, j + 1 {
		if rL.entries[i].Term != entries[j].Term {
			rL.entries = rL.entries[:i]
			break
		}
	}
	rL.entries = append(rL.entries, entries...)
	DPrintf(debugInfo|logReplicate, "%v ", rL.entries)
}

func (rL *RaftLog) getLastEntry(typ LogEntryItem) interface{} {
	entry := -1
	for i := len(rL.entries) - 1; i >= 0; i-- {
		if rL.entries[i].Index != -1 {
			entry = i
			break
		}
	}
	if entry == -1 {
		return nil
	}
	if entry < len(rL.entries) {
		switch typ {
		case Pointer:
			return &rL.entries[entry]
		case Index:
			return rL.entries[entry].Index
		case Term:
			return rL.entries[entry].Term
		case Command:
			return rL.entries[entry].Command
		}
	}
	return nil
}

func (rL *RaftLog) GetLastEntryPointer() *LogEntry {
	rL.Lock()
	defer rL.Unlock()
	entry := rL.getLastEntry(Pointer)
	if entry != nil {
		return entry.(*LogEntry)
	}
	return nil
}

func (rL *RaftLog) GetLastEntryIndex() int {
	rL.Lock()
	defer rL.Unlock()
	entry := rL.getLastEntry(Index)
	if entry != nil {
		return entry.(int)
	}
	return -1
}

func (rL *RaftLog) GetLastEntryTerm() int {
	rL.Lock()
	defer rL.Unlock()
	entry := rL.getLastEntry(Term)
	if entry != nil {
		return entry.(int)
	}
	return -1
}

func (rL *RaftLog) GetLastEntryCommand() interface{} {
	rL.Lock()
	defer rL.Unlock()
	return rL.getLastEntry(Command)
}

func (rL *RaftLog) GetCommitIndex() int {
	rL.Lock()
	defer rL.Unlock()
	return rL.commitIndex
}

func (rL *RaftLog) SetCommitIndex(i int) int {
	rL.Lock()
	defer rL.Unlock()
	rL.commitIndex = i
	return rL.commitIndex
}

func (rL *RaftLog) GetLastApplied() int {
	rL.Lock()
	defer rL.Unlock()
	return rL.lastApplied
}

func (rL *RaftLog) SetLastApplied(i int) int {
	rL.Lock()
	defer rL.Unlock()
	rL.lastApplied = i
	return rL.lastApplied
}

func (rL *RaftLog) Len() int {
	rL.Lock()
	defer rL.Unlock()
	return len(rL.entries)
}

func (rL *RaftLog) GetUncommited(nextIndex int) (entries []LogEntry) {
	rL.Lock()
	defer rL.Unlock()
	if nextIndex >= len(rL.entries) || rL.entries == nil || len(rL.entries) == 0 {
		return nil
	}
	nextIndex = binarySearch(rL.entries, nextIndex)
	if nextIndex == -1 {
		return nil
	}
	entries = rL.entries[nextIndex:]
	return entries
}

func (rL *RaftLog) getEntry(index int, typ LogEntryItem) interface{} {
	entry := sort.Search(len(rL.entries), func(i int) bool { return rL.entries[i].Index >= index })
	if entry < len(rL.entries) && rL.entries[entry].Index == index {
		switch typ {
		case Pointer:
			return &rL.entries[entry]
		case Term:
			return rL.entries[entry].Term
		case Command:
			return rL.entries[entry].Command
		}
	}
	return nil
}

func (rL *RaftLog) GetEntryPointer(index int) *LogEntry {
	rL.Lock()
	defer rL.Unlock()
	entry := rL.getEntry(index, Pointer)
	if entry != nil {
		return entry.(*LogEntry)
	}
	return nil
}

func (rL *RaftLog) GetEntryTerm(index int) int {
	rL.Lock()
	defer rL.Unlock()
	term := rL.getEntry(index, Term)
	if term != nil {
		return term.(int)
	}
	return -1
}

func (rL *RaftLog) GetEntryCommand(index int) interface{} {
	rL.Lock()
	defer rL.Unlock()
	return rL.getEntry(index, Command)
}

func (rL *RaftLog) String() string {
	rL.Lock()
	defer rL.Unlock()
	return fmt.Sprintf("[RLog c:%v a:%v %v]",
		rL.commitIndex, rL.lastApplied, rL.entries)
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

func backwardSearch(entries []LogEntry, index int) int {
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Index == index {
			return i
		}
	}
	return -1
}


