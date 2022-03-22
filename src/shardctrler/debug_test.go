package shardctrler

import "testing"

func TestSth(t *testing.T) {
	var strS []string
	strS = append(strS, "s")
	DPrintf(debugTest2, "%v", strS)

	s := []int{100, 200}
	func(s *[]int){
		(*s)[0] = 1000
		*s = (*s)[1:]
	}(&s)
	DPrintf(debugTest2, "%v", s)

	m := map[int]int{100:1000, 200:2000}
	func(mm map[int]int){
		delete(mm, 100)
		mm[300] = 3000
	}(m)
	DPrintf(debugTest2, "%v", m)
}

func TestUpdateConfig(t *testing.T) {
	//updateConfig(t, &Config{
	//	Num:    1,
	//	Shards: []int{78,80,81,79,79,77,77,78,80,81},
	//	Groups: map[int] []string{77: {"77serv"}, 78:[]string{"78serv"}, 79:[]string{"79serv"}, 80:[]string{"80serv"}, 81:[]string{"81serv"}},
	//}, &Op{OpType: OPLeave, GIDs: []int{81}}, "Leave Test1")
	//
	//updateConfig(t, &Config{
	//	Num:    1,
	//	Shards: []int{78,80,81,79,79,77,77,78,80,81},
	//	Groups: map[int] []string{77: {"77serv"}, 78:[]string{"78serv"}, 79:[]string{"79serv"}, 80:[]string{"80serv"}, 81:[]string{"81serv"}},
	//}, &Op{OpType: OPLeave, GIDs: []int{77,81}}, "Leave Test2")
	//
	//updateConfig(t, &Config{
	//	Num:    1,
	//	Shards: []int{77,80,80,77,78,80,80,77,78,78},
	//	Groups: map[int] []string{77: {"77serv"}, 78:[]string{"78serv"}, 80:[]string{"80serv"}},
	//}, &Op{OpType: OPLeave, GIDs: []int{80}}, "Leave Test3")
	//
	//updateConfig(t, &Config{
	//	Num:    1,
	//	Shards: []int{77,80,80,77,78,80,80,77,78,78},
	//	Groups: map[int] []string{77: {"77serv"}, 78:[]string{"78serv"}, 80:[]string{"80serv"}},
	//}, &Op{OpType: OPLeave, GIDs: []int{77}}, "Leave Test4")
	//updateConfig(t, &Config{
	//	Num:    1,
	//	Shards: []int{0,0,0,0,0,0,0,0,0,0},
	//	Groups: map[int] []string{77: {"77serv"}, 78:[]string{"78serv"}, 80:[]string{"80serv"}},
	//}, &Op{OpType: OPLeave, GIDs: []int{77}}, "Leave Test5")
	//updateConfig(t, &Config{
	//	Num:    1,
	//	Shards: []int{0,0,0,0,0,0,0,0,0,0},
	//	Groups: nil,
	//}, &Op{OpType: OPLeave, GIDs: nil}, "Leave Test6")
	updateConfig(t, &Config{
		Num:    1,
		Shards: []int{77,78,77,78,77,78,77,78,77,78},
		Groups: map[int] []string{77: {"77serv"}, 78:[]string{"78serv"}},
	}, &Op{OpType: OPLeave, GIDs: []int{77,78}}, "Leave Test7")

	//updateConfig(t, &Config{
	//	Num:    1,
	//	Shards: []int{0,0,0,0,0,0,0,0,0,0},
	//	Groups: map[int] []string{},
	//}, &Op{OpType: OPJoin, Servers: map[int] []string{77: {"77serv"}} }, "Leave Test1.1")
	//updateConfig(t, &Config{
	//	Num:    1,
	//	Shards: []int{0,0,0,0,0,0,0,0,0,0},
	//	Groups: map[int] []string{},
	//}, &Op{OpType: OPJoin, Servers: map[int] []string{78: {"78serv"}, 79: {"79serv"}, 77: {"77serv"}} }, "Leave Test1.2")
	//updateConfig(t, &Config{
	//	Num:    1,
	//	Shards: []int{0,0,0,0,0,0,0,0,0,0},
	//	Groups: map[int] []string{},
	//}, &Op{OpType: OPJoin, Servers: map[int] []string{78: {"78serv"}, 77: {"77serv"}} }, "Leave Test1.3")
	//updateConfig(t, &Config{
	//	Num:    1,
	//	Shards: []int{0,0,0,0,0,0,0,0,0,0},
	//	Groups: nil,
	//}, &Op{OpType: OPJoin, Servers: nil}, "Leave Test1.4")
	//updateConfig(t, &Config{
	//	Num:    1,
	//	Shards: []int{0,0,0,0,0,0,0,0,0,0},
	//	Groups: map[int] []string{77: {"77serv"}},
	//}, &Op{OpType: OPJoin, Servers: map[int] []string{78: {"78serv"}, 79: {"79serv"}, 86: {"86serv"}, 75: {"75serv"}, 85: {"85serv"}, 84: {"84serv"}, 83: {"83serv"}, 82: {"82serv"}, 81: {"81serv"}, 80: {"80serv"}, 76: {"76serv"}} }, "Leave Test1.5")
	//updateConfig(t, &Config{
	//	Num:    1,
	//	Shards: []int{0,0,0,0,0,0,0,0,0,0},
	//	Groups: map[int] []string{77: {"77serv"}, 78: {"78serv"}},
	//}, &Op{OpType: OPJoin, Servers: map[int] []string{ 76: {"76serv"}} }, "Leave Test1.6")
	//updateConfig(t, &Config{
	//	Num:    1,
	//	Shards: []int{77,77,77,77,78,78,78,78,78,78},
	//	Groups: map[int] []string{77: {"77serv"}, 78: {"78serv"}},
	//}, &Op{OpType: OPJoin, Servers: map[int] []string{ 76: {"76serv"}, 85: {"85serv"}} }, "Leave Test1.7")
	//updateConfig(t, &Config{
	//	Num:    1,
	//	Shards: []int{77,77,79,77,78,78,78,79,79,78},
	//	Groups: map[int] []string{77: {"77serv"}, 78: {"78serv"},79: {"79serv"} },
	//}, &Op{OpType: OPJoin, Servers: map[int] []string{ 76: {"76serv"}, 80: {"80serv"}} }, "Leave Test1.8")

}

func updateConfig(t *testing.T, conf *Config, op *Op, description string)  {
	const nservers = 3
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	cfg.servers[0].configs = append(cfg.servers[0].configs, *conf)
	lastConfig := cfg.servers[0].configs[len(cfg.servers[0].configs)-1]
	DPrintf(debugTest2, "%v begin l:%v %v", description, len(cfg.servers[0].configs), lastConfig)
	cfg.servers[0].updateConfig(op, &OpReply{})
	lastConfig = cfg.servers[0].configs[len(cfg.servers[0].configs)-1]
	DPrintf(debugTest2, "%v DONE l:%v %v\n", description, len(cfg.servers[0].configs), lastConfig)
}

