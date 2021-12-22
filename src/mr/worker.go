package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)



type WorkerBlock struct {
	mu 			sync.Mutex
	quit		chan bool

	id			string
	stat		State

	mapf 		func(string, string) []KeyValue
	reducef 	func(string, []string) string
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   	string
	Value 	string
}
// for sorting by key.
type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reducer
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getBucket(key string, nReduce int) int {
	return ihash(key) % nReduce
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}
	// fill in the argument(s).
	args.X = 99
	// declare a reply structure.
	reply := ExampleReply{}
	// send the RPC request, wait for the reply.
	call(masterSock(), "Coordinator.Example", &args, &reply)
	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	w := WorkerBlock{id:genWorkerID(), stat: Spare, mapf: mapf, reducef: reducef}
	w.quit = make(chan bool)
	w.server()
	w.register()
	for {
		time.Sleep(time.Hour)
	}
}

func (w *WorkerBlock) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	//l, err := net.Listen("tcp", ":1234")
	sockname := workerSock(w.id)
	os.Remove(sockname)
	l, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatal("listen error: ", err)
	}
	go http.Serve(l, nil)
}

func (w *WorkerBlock) register() {
	args := WorkerState{ID:	w.id, Stat: w.stat}
	var reply bool

	if call(masterSock(), "Coordinator.Register", &args, &reply); reply == false {
		log.Fatal("Worker ", w.id, " register failed")
	}
	log.Println("Worker registered: ", w.id)
}

func (w *WorkerBlock) updateState() {

}


