package raft

import (
	"math/rand"
	"strings"
	"time"
)

const (
	// Time
	heartbeatTimeout = 100 * time.Millisecond
)

// GetElectionTimeout time func
func GetElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(int(randFloats(2.5, 5)*1000)) *
		heartbeatTimeout / 1000
}

func randFloats(min, max float64) float64 {
	return min + rand.Float64() * (max - min)
}

// Stop and reset the time.Timer
func stopResetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// return a stopped timer
func newTimer() *time.Timer {
	t := time.NewTimer(0)
	if !t.Stop() {
		<-t.C
	}
	return t
}

// get time offset
func getTimeOffset(t time.Time) string {
	s := t.String()
	strs := strings.Split(s, " ")
	off := strs[len(strs) - 1]
	return off
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// return the majority number of the input, return -1 if not exists
func majority(nums []int) int {
	m := make(map[int]int)
	for _, num := range nums {
		m[num]++
	}
	max := 0
	ret := -1
	for k, v := range m {
		if v > max {
			max = v
			ret = k
		}
	}
	if max > len(nums) / 2 {
		return ret
	}
	return -1
}
