package raft

import (
	"fmt"
	"log"
	"runtime"
	"sort"
	"testing"
)

func init() {
	//log.SetFlags(log.Lshortfile | log.Ltime | log.Lmicroseconds)
}

func TestLogDebug(t *testing.T) {
	log.Printf("%v", subset(0x1001, 0x1011))
	log.Printf("%v", subset(0x1011, 0x0111))
	log.Printf("%v", subset(0x0000, 0x0111))
	log.Printf("%v", intersection(0x1011, 0x0110))
	log.Printf("%v", intersection(0x1011, 0x0100))
	log.Printf("%v", intersection(0x0000, 0x0100))
	log.Printf("%v", debugFilter(logReplicate, debugConf))
}

func TestSliceDebug(t *testing.T) {
	var nextIndex []int
	nextIndex = make([]int, 5)
	for i := 0; i < len(nextIndex); i++ {
		nextIndex[i] = 100
	}
	log.Println(nextIndex[0:5])
}

func TestLoggerDebug(t *testing.T) {
	funcName, file, line, ok := runtime.Caller(0)
	if ok {
		fmt.Println("func name: " + runtime.FuncForPC(funcName).Name())
		fmt.Printf("file: %s, line: %d\n",file,line)
	}
}

func TestRaftLogCheckAppendEntries(t *testing.T) {
	rL := &RfLog{commitIndex: -1, lastApplied: -1}
	rL.Entries = []LogEntry{
		{0, 1, 0},
		{1, 2, 100}, {2, 2, 200},
		{3, 3, 300}, {4, 3, 400},
		{5, 3, 500}, {6, 4, 600},
	}
	// Index: 0 1 2 3 4
	// Term:  2 2 3 3 3
	log.Println()
	log.Println(rL.Entries)
	log.Println(rL.CheckAppendEntries(1, 2))
	log.Println(rL.CheckAppendEntries(0, 2))
	log.Println(rL.CheckAppendEntries(1, 3))
	log.Println(rL.CheckAppendEntries(-1, -1))
	log.Println(rL.CheckAppendEntries(2, 3))
	log.Println(rL.CheckAppendEntries(6, 4))
	log.Println(rL.CheckAppendEntries(7, 4))
	log.Println(rL.ConflictingEntryTermIndex(0))
	log.Println(rL.ConflictingEntryTermIndex(1))
	log.Println(rL.ConflictingEntryTermIndex(2))
	log.Println(rL.ConflictingEntryTermIndex(3))
	log.Println(rL.ConflictingEntryTermIndex(4))
	log.Println(rL.ConflictingEntryTermIndex(5))

	log.Println()
	rL.Entries = nil
	log.Println(rL.Entries)
	log.Println(rL.CheckAppendEntries(1, 2))
	log.Println(rL.CheckAppendEntries(0, 2))
	log.Println(rL.CheckAppendEntries(1, 3))
	log.Println(rL.CheckAppendEntries(-1, -1))
	log.Println(rL.CheckAppendEntries(2, 3))
	log.Println(rL.CheckAppendEntries(7, 4))
	log.Println(rL.ConflictingEntryTermIndex(0))
	log.Println(rL.ConflictingEntryTermIndex(1))
	log.Println(rL.ConflictingEntryTermIndex(2))
	log.Println(rL.ConflictingEntryTermIndex(3))
	log.Println(rL.ConflictingEntryTermIndex(4))
	log.Println(rL.ConflictingEntryTermIndex(5))

	rL.Entries = []LogEntry{
		{0, 1, 0},
		{1, 2, 100}, {2, 2, 200},
		{3, 2, 300}, {4, 2, 400},
		{5, 2, 500}, {6, 2, 600},
	}
	log.Println()
	log.Println(rL.Entries)
	log.Println(rL.CheckAppendEntries(1, 2))
	log.Println(rL.CheckAppendEntries(0, 2))
	log.Println(rL.CheckAppendEntries(1, 3))
	log.Println(rL.CheckAppendEntries(-1, -1))
	log.Println(rL.CheckAppendEntries(2, 3))
	log.Println(rL.CheckAppendEntries(7, 4))
	log.Println(rL.ConflictingEntryTermIndex(0))
	log.Println(rL.ConflictingEntryTermIndex(1))
	log.Println(rL.ConflictingEntryTermIndex(2))
	log.Println(rL.ConflictingEntryTermIndex(3))
	log.Println(rL.ConflictingEntryTermIndex(4))
	log.Println(rL.ConflictingEntryTermIndex(5))
}

func TestRaftLogTruncate(t *testing.T) {
	rL := &RfLog{commitIndex: -1, lastApplied: -1}
	rL.Entries = []LogEntry{
		{0, 2, 0}, {1, 2, 100},
		{2, 3, 200}, {3, 3, 300},
		{4, 3, 400},
	}
	bak := rL.Entries
	// Index: 0 1 2 3 4
	// Term:  2 2 3 3 3

	// Index:     2 3
	// Term:      3 3
	e := []LogEntry{
		{2, 2, 1000}, {3, 2, 2000},
	}
	rL.TruncateAppend(1, e)
	log.Println()

	// Index: 0 1 2 3 4
	// Term:  2 2 3 3 3
	rL.Entries = bak
	// Index:         4 5
	// Term:          3 3
	e = []LogEntry{
		{4, 4, 4000}, {5, 4, 5000},
	}
	rL.TruncateAppend(3, e)
	log.Println()

	// Index: 0 1 2 3 4
	// Term:  2 2 3 3 3
	rL.Entries = bak
	// Index:           5 6
	// Term:            3 3
	e = []LogEntry{
		{5, 3, 5000}, {6, 4, 6000},
	}
	rL.TruncateAppend(4, e)
	log.Println()

	// Index: 0 1 2 3 4
	// Term:  2 2 3 3 3
	rL.Entries = bak
	// Index: 0 1
	// Term:  1 1
	e = []LogEntry{
		{0, 1, 0}, {1, 1, 1000},
	}
	rL.TruncateAppend(-1, e)
	log.Println()

	// Index: 0 1 2 3 4
	// Term:  2 2 3 3 3
	rL.Entries = bak
	// Index:
	// Term:
	e = nil
	rL.TruncateAppend(-1, e)
	log.Println()

	// Index: 0 1 2 3 4
	// Term:  2 2 3 3 3
	rL.Entries = bak
	// Index:
	// Term:
	e = nil
	rL.TruncateAppend(4, e)
	log.Println()
}

type TestLogEntry struct {
	Index 			int
	Term    		int
	Command 		interface{}
}

func TestRaftLogEntry(t *testing.T) {
	//Entries := []TestLogEntry{
	//	{1, 2, 0}, {2, 2, 100},
	//	{3, 3, 200}, {-1, -1, 300},
	//	{4, 3, 300},
	//	{5, 3, 400},
	//}
	entries := []TestLogEntry{}
	entry := sort.Search(len(entries), func(i int) bool { return entries[i].Index >= 4 })
	if entry < len(entries) && entries[entry].Index == 4 {
		log.Printf("found %d at index %d in %v\n", entries[entry], entry, entries)
	} else {
		log.Printf("%d not found in %v\n", 4, entries)
	}
}

func TestRaftLogGetLastEntry(t *testing.T) {
	rL := &RfLog{commitIndex: -1, lastApplied: -1}
	rL.Entries = []LogEntry{
		{0, 2, 0}, {1, 2, 100},
		{2, 3, 200}, {3, 3, 300},
		{4, 3, 400},
	}
	//bak := rL.Entries
	// Index: 0 1 2 3 4
	// Term:  2 2 3 3 3
	lastLog := rL.GetLastEntryPointer()
	lastLogIndex := rL.GetLastEntryIndex()
	lastLogTerm := rL.GetLastEntryTerm()
	lastLogCommand := rL.GetLastEntryCommand()
	log.Printf("%v %v %v %v", lastLog, lastLogIndex, lastLogTerm, lastLogCommand)
}

func TestRaftCheckAppendEntries(t *testing.T) {
	rL := &RfLog{commitIndex: -1, lastApplied: -1}
	rL.Entries = []LogEntry{
		{0, 2, 0}, {1, 2, 100},
		{2, 3, 200}, {3, 3, 300},
		{4, 3, 400},
	}
	_, ok := rL.CheckAppendEntries(3, 3)
	log.Printf("%v", ok)

	log.Printf("majority:%v", majority([]int{20, -1, -1, 10, 20}) )
	log.Printf("majority:%v", majority([]int{1, 1, -1, -1, 1}) )

	log.Printf("%v", 2 < (5 / 2) )

	rL.Entries = []LogEntry{
		{1, 2, 100},
		{2, 3, 200}, {3, 3, 300},
		{8, 3, 400},
	}
	nextIdx := 1
	if next := rL.getLastEntry(Index); next != nil {
		nextIdx = next.(int) + 1
	}
	log.Printf("nextIdx:%v", nextIdx)
}













