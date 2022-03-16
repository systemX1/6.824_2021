package raft

import (
	"fmt"
	"log"
	"runtime"
	"sort"
	"testing"
	"time"
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

func TestElectionTimeout(t *testing.T) {
	t0 := time.Now()
	electTimer := newTimer()
	stopResetTimer(electTimer, GetElectionTimeout())
	for {
		select {
		case t1 := <-electTimer.C:
			DPrintf(requsetVote, "ElectionTimeout %v %v %v",
				getTimeOffset(t1), t1.Sub(t0), electTimer)
			t0 = t1
			stopResetTimer(electTimer, GetElectionTimeout())
		}
	}
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
		{Index:0, Term:1, Command:0},
		{Index:1, Term:2, Command:100}, {Index:2, Term:2, Command:200},
		{Index:3, Term:3, Command:300}, {Index:4, Term:3, Command:400},
		{Index:5, Term:3, Command:500}, {Index:6, Term:4, Command:600},
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
		{Index:0, Term:1, Command:0},
		{Index:1, Term:2, Command:100}, {Index:2, Term:2, Command:200},
		{Index:3, Term:3, Command:300}, {Index:4, Term:3, Command:400},
		{Index:5, Term:3, Command:500}, {Index:6, Term:4, Command:600},
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
	bak := []LogEntry{
		{Index:0, Term:1, Command:0},
		{Index:1, Term:2, Command:100}, {Index:2, Term:2, Command:200},
		{Index:3, Term:3, Command:300}, {Index:4, Term:3, Command:400},
		{Index:5, Term:3, Command:500}, {Index:6, Term:4, Command:600},
	}
	rL.Entries = make([]LogEntry, 6)

	// Index: 0 1 2 3 4 5
	// Term:  2 2 2 2 3 3

	// Index:     2 3
	// Term:      3 3
	e := []LogEntry{
		{Index:2, Term:2, Command:1000}, {Index:3, Term:2, Command:2000},
	}
	rL.TruncateAppendTest(1, bak, e)

	// Index: 0 1 2 3 4 5
	// Term:  2 2 2 2 3 3
	// Index:         4 5
	// Term:          3 3
	e = []LogEntry{
		{Index:4, Term:4, Command:4000}, {Index:5, Term:4, Command:5000},
	}
	rL.TruncateAppendTest(3, bak, e)

	// Index: 0 1 2 3 4 5
	// Term:  2 2 2 2 3 3
	// Index:           5 6
	// Term:            3 3
	e = []LogEntry{
		{Index:5, Term:3, Command:5000}, {Index:6, Term:4, Command:6000},
	}
	rL.TruncateAppendTest(4, bak, e)

	// Index: 0 1 2 3 4 5
	// Term:  2 2 2 2 3 3
	// Index: 0 1
	// Term:  1 1
	e = []LogEntry{
		{Index:0, Term:1, Command:0}, {Index:1, Term:1, Command:1000},
	}
	rL.TruncateAppendTest(-1, bak, e)

	// Index: 0 1 2 3 4 5
	// Term:  2 2 2 2 3 3
	// Index:
	// Term:
	e = nil
	rL.TruncateAppendTest(-1, bak, e)

	// Index: 0 1 2 3 4 5
	// Term:  2 2 2 2 3 3
	// Index:
	// Term:
	e = []LogEntry{
		{Index:3, Term:2, Command:0}, {Index:4, Term:2, Command:1000},
	}
	rL.TruncateAppendTest(2, bak, e)

	// Index: 0 1 2 3 4 5
	// Term:  2 2 2 2 3 3
	// Index:
	// Term:
	e = []LogEntry{
		{Index:3, Term:2, Command:0}, {Index:4, Term:3, Command:1000},
		{Index:5, Term:3, Command:2000},
	}
	rL.TruncateAppendTest(2, bak, e)

	e = []LogEntry{
		{0, 1, 7835, 0 }, {1, 1, 1526, 0 }, {2, 1, 7896, 0 }, {3, 1, 9377, 0 }, {4, 1, 1172, 0 }, {5, 1, 9152, 0 }, {6, 1, 1246, 0 }, {7, 1, 2920, 0 }, {8, 1, 9545, 0 }, {9, 1, 6123, 0 }, {10, 1, 1930, 0 }, {11, 1, 3918, 0 }, {12, 1, 3136, 0 }, {13, 1, 6714, 0 }, {14, 1, 5853, 0 }, {15, 1, 1010, 0 }, {16, 1, 6034, 0 }, {17, 1, 9439, 0 }, {18, 1, 3049, 0 }, {19, 1, 4074, 0 }, {20, 1, 7182, 0 }, {21, 1, 3688, 0 }, {22, 6, 3688, 0 }, {23, 6, 3688, 0 }, {24, 10, 3688, 0 },
	}
	n := make([]LogEntry, len(e))
	copy(n, e)
	rL.TruncateAppendTest(-1, e, n)
	rL.TruncateAppendTest(0, e, nil)

	e = []LogEntry{
		{9, 1, 7346114951392631761, 0}, {10, 1, 1738741665562420248, 0}, {11, 1, 6684187936943440122, 0}, {12, 1, 8036592355440237559, 0}, {13, 1, 8361368526235920682, 0},
	}
	n = []LogEntry{
		{14, 1, 4360611029131970908, 0}, {15, 1, 7400823843287529525, 0}, {16, 1, 2489418614210817561, 0}, {17, 1, 8519260016716978626, 0}, {18, 1, 6993154974038825947, 0}, {19, 1, 1008007605460300518, 0}, {20, 1, 1774280318773226980, 0}, {21, 1, 4617934639504924324, 0}, {22, 1, 3015210891149567234, 0}, {23, 1, 2534946382264358140, 0}, {24, 1, 7008595092771287268, 0}, {25, 1, 3753485396871821837, 0},
	}
	rL.TruncateAppendTest(13, e, n)

	e = []LogEntry{
		{198, 1, 4002,0}, {199, 1, 4003,0}, {200, 1, 41,0}, {201, 1, 4103,0}, {202, 1, 4101,0}, {203, 1, 4100,0}, {204, 1, 4102,0},
	}
	n = []LogEntry{
		{201, 1, 4103,0}, {202, 1, 4101,0}, {203, 1, 4100,0}, {204, 1, 4102,0},
	}
	rL.TruncateAppendTest(204, e, n)


}

func (rL *RfLog) TruncateAppendTest(prevLogIndex int, old, new []LogEntry) {
	rL.Entries = make([]LogEntry, len(old))
	copy(rL.Entries, old)
	fmt.Println()
	log.Printf("rL.Entries:%v", rL.Entries)
	rL.TruncateAppend(prevLogIndex, new)
	log.Printf("prevLogIndex:%v new:%v", prevLogIndex, new)
	log.Printf("rL.Entries:%v", rL.Entries)
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
	var entries []TestLogEntry
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
		{Index:0, Term:1, Command:0},
		{Index:1, Term:2, Command:100}, {Index:2, Term:2, Command:200},
		{Index:3, Term:3, Command:300}, {Index:4, Term:3, Command:400},
		{Index:5, Term:3, Command:500}, {Index:6, Term:4, Command:600},
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
	rL := &RfLog{commitIndex: 22, lastApplied: 0}
	rL.Entries = []LogEntry{
		{0, 1, 2708, 0}, {1, 1, 6094, 0}, {2, 1, 4932, 0}, {3, 1, 4447, 0}, {4, 1, 6978, 0}, {5, 1, 3823, 0}, {6, 1, 5190, 0}, {7, 1, 3398, 0}, {8, 1, 612, 0}, {9, 1, 3893, 0}, {10, 1, 9642, 0}, {11, 1, 8594, 0}, {12, 1, 9648, 0}, {13, 1, 742, 0}, {14, 1, 4394, 0}, {15, 1, 3312, 0}, {16, 1, 8528, 0}, {17, 1, 1564, 0}, {18, 1, 7022, 0}, {19, 1, 1365, 0}, {20, 1, 8981, 0}, {21, 1, 5553, 0}, {22, 12, 5553, 0},
	}
	ok1, ok2 := rL.CheckAppendEntries(22, 12)
	log.Printf("ok1:%v ok2:%v", ok1, ok2)
	log.Printf("Entries:%v", rL.Entries)

	//rL.Entries = []LogEntry{
	//	{0, 2, 0}, {1, 2, 100},
	//	{2, 3, 200}, {3, 3, 300},
	//	{4, 3, 400},
	//}
	//_, ok := rL.CheckAppendEntries(3, 3)
	//log.Printf("%v", ok)
	//rL.Entries = []LogEntry{
	//	{0, 2, 0}, {1, 2, 100},
	//	{2, 3, 200}, {3, 3, 300},
	//	{4, 3, 400},
	//}
	//nextIdx := 1
	//if next := rL.getLastEntry(Index); next != nil {
	//	nextIdx = next.(int) + 1
	//}
	//log.Printf("nextIdx:%v", nextIdx)
	//
	//log.Printf("Entries:%v", rL.Entries)
	//rL.TruncateAppend(4, nil)
	//log.Printf("Entries:%v", rL.Entries)
	//
	//log.Printf("")
	//rL.Entries = []LogEntry{
	//	{0, 1, 1914349784653825581},
	//	{1,1 ,5115215478442132969},
	//	{2 ,1 ,644023564989052030},
	//	{3 ,1 ,491952001898919428},{4 ,1 ,5320607044397088061},{5 ,1 ,7158046334734006528},{6 ,1 ,6882122341943900785},{7 ,1 ,8165268298106835542},{8 ,1 ,3073903228404029259},{9 ,1 ,5546012665909090137},{10 ,10 ,9091055988539345026},{11 ,10 ,2025263028397047231},{12 ,10 ,7239149249273189333},{13 ,12 ,5347915136078874454},{14 ,12 ,3459900741821227259},{15 ,12 ,3113703595031991792},{16 ,12 ,2215468956251713594},{17 ,12 ,5914235101744000764},{18 ,12 ,3387433702880092941},{19 ,12 ,5624514078489037657},{20 ,12 ,9135726393308299505},{21 ,12 ,1819735639310722253},{22 ,12 ,5735826076630930058},{23 ,12 ,6378264717613715731},{24 ,12 ,2631800315277782072},{25 ,16 ,3995365905765341801},{26 ,16 ,3739032140439442525},{27 ,16 ,3387851307745081001},{27 ,16 ,3387851307745081001},{28 ,16 ,3601870430737242983},{29 ,16 ,503975231338243367},{30 ,16 ,1352651752508428952},{31 ,16 ,4708315476667193553},{32 ,16 ,6660183472637444746},{33 ,16 ,8191903244253840065},{34 ,18 ,5630543216963647757},{35 ,18 ,8265434149690220681},{36 ,18 ,4761776791614191307},{37 ,20 ,1524602433378219246},{38 ,20 ,8513349999059836890},{39 ,20 ,9151644230429420542},
	//}
	//
	//log.Printf("Entries:%v", rL.Entries)
	//rL.TruncateAppend(39, nil)
	//log.Printf("Entries:%v", rL.Entries)
	//
	//s := []int{0, 1}
	//s = s[:2]
	//log.Printf(":%v", s)


	fmt.Println()
	rL.Entries = []LogEntry{
		{Index:0,Term:1,Command:293944620429666665},{Index:1,Term:1,Command:6598875434044817142},{Index:2,Term:1,Command:1359549880165236450},{Index:3,Term:1,Command:7089634551187792470},{Index:4,Term:1,Command:6523659949574271997},{Index:5,Term:1,Command:8833751060099643941},
	}
	b1, b2 := rL.CheckAppendEntries(20, 1)
	log.Printf("%v %v", b1, b2)
	log.Printf("Entries:%v", rL.Entries)
	rL.TruncateAppend(2, []LogEntry{{Index:3, Term:1, Command:7089634551187792470}, {Index:4 , Term:1, Command:6523659949574271997}, {Index:5, Term:1, Command:8833751060099643941}})
	log.Printf("Entries:%v", rL.Entries)

}

func TestGetUncommited(t *testing.T) {
	rL := &RfLog{commitIndex: -1, lastApplied: -1}
	rL.Entries = []LogEntry{
		{Index:0, Term:1, Command:101}, {Index:-1, Term:-1, Command:nil}, {Index:1, Term:3, Command:103},
	}
	log.Printf("majority:%v", majority([]int{11, -1, -1, 11, 11}))
	log.Printf("next:%v", binarySearch(rL.Entries, 0))

	//log.Printf("entries:%v", rL.Entries[binarySearch(rL.Entries, 0):])
}

func TestRfLog_DoSnapshot(t *testing.T) {
	rL := &RfLog{commitIndex: -1, lastApplied: -1}
	rL.Entries = []LogEntry{
		{Index:0, Term:1, Command:1000}, {Index:1, Term:1, Command:2000}, {Index:2, Term:3, Command:3000},
	}
	log.Printf("rL:%v", rL)
	rL.DoSnapshot(0)
	log.Printf("rL:%v", rL)

	rL.Entries = []LogEntry{
		{Index:0, Term:1, Command:1000}, {Index:1, Term:1, Command:2000}, {Index:2, Term:3, Command:3000},
	}
	rL.DoSnapshot(1)
	log.Printf("rL:%v", rL)

	rL.Entries = []LogEntry{
		{Index:0, Term:1, Command:1000}, {Index:1, Term:1, Command:2000}, {Index:2, Term:3, Command:3000},
	}
	rL.DoSnapshot(2)
	log.Printf("rL:%v", rL)
}

func TestRaftXXX(t *testing.T) {
	rL := &RfLog{commitIndex: -1, lastApplied: -1}
	rL.Entries = []LogEntry{
		{Index:0, Term:1, Command:101}, {Index:-1, Term:-1, Command:nil}, {Index:1, Term:3, Command:103},
	}
	log.Printf("majority:%v", majority([]int{11, -1, -1, 11, 11}))
	log.Printf("next:%v", binarySearch(rL.Entries, 0))

	//log.Printf("entries:%v", rL.Entries[binarySearch(rL.Entries, 0):])
}









