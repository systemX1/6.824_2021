package shardkv

import "testing"

func TestSth(t *testing.T) {
	m := map[int]string{100:"1000", 200:"2000"}
	v, ok := m[100]; if !ok {

	}
	DPrintf(debugTest2, "%v %v", m, v)
	DPrintf(debugTest2, "%v", key2shard("0"))

}
