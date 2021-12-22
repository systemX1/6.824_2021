package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"net/rpc"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

type AckReply bool

type WorkerState struct {
	ID   			string
	Stat      		State
	timer     		*time.Timer
	IsMapDone 		bool
	IsReduceDone 	bool
	Files			[]string
}

type Task struct {
	ID      	string
	NReduce 	int
	Filename	string
	Stat 		State
}

type ReduceTask struct {
	Bucket 		int
	Files		[]string
	Stat 		State
}

type ChangeState struct {
	IsChanged 	bool
	Stat      	State
}

// send a RPC request
func call(sockname string, rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return fmt.Errorf("dialing: %w\n", err)
	}
	defer c.Close()

	if err = c.Call(rpcname, args, reply); err != nil {
		return fmt.Errorf("rpc call %s %s failed: %w\n", sockname, rpcname, err)
	}
	return nil
}
