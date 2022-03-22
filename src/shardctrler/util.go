package shardctrler

import (
	"fmt"
	"io"
	"log"
	"math"
	"path"
	"runtime"
	"sort"
)

var debugFilter func (a uint, b uint) bool
func init() {
	debugFilter = subset
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetOutput(io.Discard)
	//log.Lshortfile |
}

const (
	all 			uint = math.MaxUint32
	clerk           uint = 1 << 4
	shardctrler     uint = 1 << 5
	debugTest       uint = 1 << 6
	debugTest2      uint = 1 << 7
	debugError 		uint = 1 << 8
	debugInfo 		uint = 1 << 9
	debugConf = all
	//debugConf = debugTest2 + debugTest
	//debugConf = 0
)

// return if "a" is a subset of "b"
func subset(a, b uint) bool {
	return a & b == a
}

// return if "a" intersection "b" not empty
func intersection(a, b uint) bool {
	return a & b != 0 || a == 0
}

func DPrintf(debugLevel uint, format string, a ...interface{}) {
	if debugFilter(debugLevel, debugConf) {
		funcName, file, line, _ := runtime.Caller(1)
		file = path.Base(file)
		funcNameStr := path.Base(runtime.FuncForPC(funcName).Name())
		logInfo := fmt.Sprintf("%v %d %s ", file, line, funcNameStr)
		printInfo := fmt.Sprintf(format, a...)
		log.Println(logInfo, printInfo)
	}
}

func DPanicf(debugLevel uint, format string, a ...interface{}) {
	DPrintf(debugLevel, format, a...)
	panic(1)
}

func getGIDWithMinMaxShards(GIDShardMap map[int] []int) (minGID, minNum, maxGID, maxNum int) {
	minGID, minNum, maxGID, maxNum = 0, math.MaxInt32, 0, -1
	GIDs := make([]int, 0, len(GIDShardMap))
	for GID := range GIDShardMap {
		GIDs = append(GIDs, GID)
	}
	sort.Ints(GIDs)	// 确保各状态机deterministic after rebalanced
	DPrintf(debugTest, "GIDs:%v", GIDs)
	for _, GID := range GIDs {
		l := len(GIDShardMap[GID])
		if l < minNum {
			minNum = l
			minGID = GID
		}
		if l > maxNum {
			maxNum = l
			maxGID = GID
		}
	}
	return
}

// rearrange num Shards from srcShards to GIDShardMap[minGID]
func moveShardsBetweenGID(minGID, num int, srcShards, Shards *[]int, GIDShardMap map[int] []int) {
	for i := 0; i < num; i++ {
		(*Shards)[(*srcShards)[i]] = minGID
		GIDShardMap[minGID] = append(GIDShardMap[minGID], (*srcShards)[i])
	}
	*srcShards = (*srcShards)[num:]
	DPrintf(debugTest, "delGIDShards:%p %v, conf.Shards:%p %v", srcShards, srcShards, Shards, Shards)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
