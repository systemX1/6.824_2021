package kvraft

import (
	"fmt"
	"log"
	"math"
	"path"
	"runtime"
)


var debugFilter func (a uint, b uint) bool
func init() {
	debugFilter = subset
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	//log.SetOutput(io.Discard)
	//log.Lshortfile |
}

const (
	all 			uint = math.MaxUint32
	clerk			uint = 1 << 4
	kvserver 		uint = 1 << 5
	snapshot		uint = 1 << 6
	debugError 		uint = 1 << 8
	debugInfo 		uint = 1 << 9
	debugConf = debugInfo
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
		logInfo := fmt.Sprintf("%v %d %s", file, line, funcNameStr)
		printInfo := fmt.Sprintf(format, a...)
		log.Println(logInfo, printInfo)
	}
}

func DPanicf(debugLevel uint, format string, a ...interface{}) {
	DPrintf(debugLevel, format, a...)
	panic(1)
}

