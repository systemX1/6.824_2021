package raft

import (
	"fmt"
	"log"
	"math"
	"path"
	"runtime"
)

var debugFilter func (a uint, b uint) bool
func init() {
	debugFilter = intersection
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	//log.Lshortfile |
	//l, _ := os.Open("/dev/null")
	//log.SetOutput(l)
}

const (
	all 			uint = math.MaxUint32
	always 			uint = 0
	requsetVote 	uint = 1
	heartbeat 		uint = 1 << 2
	logReplicate 	uint = 1 << 3
	persist 		uint = 1 << 4
	client 			uint = 1 << 5
	debugError 		uint = 1 << 6
	debugInfo 		uint = 1 << 7
	debugConf = all
)

// return if "a" is a subset of "b"
func subset(a, b uint) bool {
	return a & b == a
}

// return if "a" intersection "b" not empty
func intersection(a, b uint) bool {
	return a & b != 0 || a == 0
}

// DMutexPrintf log func
func (rf *Raft) DMutexPrintf(debugLevel uint, format string, a ...interface{}) {
	if debugFilter(debugLevel, debugConf) {
		funcName, file, line, _ := runtime.Caller(1)
		file = path.Base(file)
		funcNameStr := path.Base(runtime.FuncForPC(funcName).Name())
		logInfo := fmt.Sprintf("%v %d %s", file, line, funcNameStr)
		rf.Lock()
		defer rf.Unlock()
		printInfo :=  fmt.Sprintf(format, a...)
		log.Println(logInfo, printInfo)
	}
	return
}

func (rf *Raft) MutexLogPrintf(format string, a ...interface{}) {
	funcName, file, line, _ := runtime.Caller(1)
	file = path.Base(file)
	funcNameStr := path.Base(runtime.FuncForPC(funcName).Name())
	logInfo := fmt.Sprintf("%v %d %s", file, line, funcNameStr)
	rf.Lock()
	defer rf.Unlock()
	printInfo :=  fmt.Sprintf(format, a...)
	log.Println(logInfo, printInfo)
}

func DPrintf(debugLevel uint, format string, a ...interface{}) (n int, err error) {
	if debugFilter(debugLevel, debugConf) {
		funcName, file, line, _ := runtime.Caller(1)
		file = path.Base(file)
		funcNameStr := path.Base(runtime.FuncForPC(funcName).Name())
		logInfo := fmt.Sprintf("%v %d %s", file, line, funcNameStr)
		printInfo :=  fmt.Sprintf(format, a...)
		log.Println(logInfo, printInfo)
	}
	return
}

func PrintLine()  {
	log.Printf("========================================================")
}

func PrintStars() {
	log.Printf("********************************************************")
}
