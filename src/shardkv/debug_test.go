package shardkv

import "testing"

func TestSth(t *testing.T) {
	m := map[int]string{100:"1000", 200:"2000"}
	v, ok := m[100]; if !ok {

	}
	DPrintf(debugTest2, "%v %v", m, v)
	DPrintf(debugTest2, "%v", key2shard("0"))

	ss := []string{"server-100-0", "server-100-1", "server-100-2"}
	for i, s := range ss {
		DPrintf(debugTest2, "%v %v", i, s)
	}

	kv := &KVMap{"5":"555", "1":"", "2":"2", "7":"7777"}
	shard := NewShard(2, Available)
	shard.Storage = *kv
	DPrintf(debugTest2, "%v %v", kv, shard)
}

func TestShardKVapplyOpPullConf(t *testing.T) {

}
