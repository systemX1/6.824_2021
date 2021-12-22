package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"sort"
	"time"
)

// pharse1: tell master the job is already start, set state to Mapping
func (w *WorkerBlock) DoMap(task *Task, stat *State) error {
	if w.stat != Spare {
		return fmt.Errorf("worker is aready working")
	}

	w.mu.Lock()
	w.stat = Mapping
	w.mu.Unlock()
	*stat = Mapping
	task.Stat = Mapping

	log.Printf("worker: %v %v DO MAP1 %v task: %v %v file:%v\n",
		w.id, w.stat, *stat, task.ID, task.Stat, task.Filename)

	go w.doMap2(task)
	return nil
}

// pharse2: stop current job if received RPC Call from master that stop the worker (timeout)
func (w *WorkerBlock) doMap2(task *Task) {
	done := make(chan bool)
	interrupt := make(chan bool)
	filechan := make(chan string, len(task.Filename))	// non-blocking
	go w.doMap3(task, done, filechan, interrupt)

	log.Printf("worker: %v %v DO MAP2 task: %v %v file:%v\n",
		w.id, w.stat, task.ID, task.Stat, task.Filename)
	select {
	// actively quit, may be caused by timeout
	case <-w.quit:
		w.mu.Lock()
		interrupt <- true
		w.stat = Spare
		w.mu.Unlock()
		workerStat := WorkerState{ID: w.id, Stat: Spare, IsMapDone: false}
		var reply bool
		call(masterSock(), "Coordinator.ReportMapState", &workerStat, &reply)
		log.Printf("worker quit: %v %v task: %v stat: %v file: %v\n",
			w.id, w.stat, task.ID, task.Stat, task.Filename)
	// work done
	case <-done:
		w.mu.Lock()
		w.stat = Spare
		w.mu.Unlock()

		mapfiles := make([]string, 0, task.NReduce)
		for i := 0; i < task.NReduce; i++ {
			f := <- filechan
			mapfiles = append(mapfiles, f)
		}

		workerStat := WorkerState{ID: w.id, Stat: Spare, IsMapDone: true, Files: mapfiles}
		var reply bool
		call(masterSock(), "Coordinator.ReportMapState", &workerStat, &reply)
		log.Printf("worker done: %v %v task: %v stat: %v file: %v\n",
			w.id, w.stat, task.ID, task.Stat, task.Filename)
	}
}

// pharse3: do the actual work
func (w *WorkerBlock) doMap3(task *Task, d chan<- bool, filechan chan<- string, iurpt <-chan bool) {
	log.Printf("worker: %v %v DO MAP3 task %v: %v file:%v\n",
		w.id, w.stat, task.ID, task.Stat, task.Filename)
	//time.Sleep(3 * workerTimeout)

	// load files
	intermediate := []KeyValue{}
	f, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v %w\n", task.Filename, err)
	}
	content, err := io.ReadAll(f)
	if err != nil {
		log.Fatalf("failed to read %v %w\n", task.Filename, err)
	}
	f.Close()
	kva := w.mapf(task.Filename, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	log.Printf("doMap2: %v intermediate's len: %v\n", f.Name(), len(intermediate))

	// set workdir
	pwd, _ := os.Getwd()
	tmpwd := fmt.Sprintf("%s/tmp/", pwd)
	mapwd := fmt.Sprintf("%s/map/", pwd)

	// create tmp files and their json.Encoder
	tmpFiles := make([]*os.File, 0, task.NReduce)
	mapFiles := make([]string, 0, task.NReduce)
	encoders := make([]*json.Encoder, 0, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		// tmpfile's name is "mr-$(ReduceTaskNumber)-$(random string)"
		name := fmt.Sprintf("tmp-%d-*", i)
		tmpf, err := ioutil.TempFile(tmpwd, name)

		if err != nil {
			log.Fatal("creating tmp files: ", err)
		}
		tmpFiles = append(tmpFiles, tmpf)
		encoders = append(encoders, json.NewEncoder(tmpf))
	}
	log.Printf("worker: %v tmpf created: %v", w.id, len(tmpFiles))

	// write kv to tmpfile in json format
	for _, kv := range intermediate {
		i := getBucket(kv.Key, task.NReduce)
		if err := encoders[i].Encode(&kv); err != nil {
			log.Printf("encode %v to %v failed %w", kv, tmpFiles[i].Name(), err)
		}
	}

	// mv tmpfiles
	for i, file := range tmpFiles {
		// filename is "mr-$MapTaskNumber-$ReduceTaskNumber"
		name := fmt.Sprintf("%smr-%s-%d", mapwd, task.ID ,i)
		if err := os.Rename(file.Name(), name); err != nil {
			log.Fatalf("rename %v to %v failed: %v\n", file.Name(), name, err)
		} else {
			mapFiles = append(mapFiles, name)
		}

		defer file.Close()
	}
	select {
	case <-iurpt:
		// already timeout, remove files
		for _, f := range tmpFiles {
			os.Remove(f.Name())
		}
		return
	default:
		// non-blocking, tell work done
		select {
		case d <- true:
		default:
		}
		// tell files' names
		for _, f := range mapFiles {
			filechan <- f
		}
	}
}

// pharse1: tell master the job is already start, set state to Reducing
func (w *WorkerBlock) DoReduce(task *ReduceTask, stat *State) error {
	if w.stat != Spare {
		return fmt.Errorf("worker is aready working")
	}

	w.mu.Lock()
	w.stat = Reducing
	w.mu.Unlock()
	*stat = Reducing
	task.Stat = Reducing

	log.Printf("worker: %v %v DO REDUCE1 %v task bucket:%v files:%v\n",
		w.id, w.stat, *stat, task.Bucket, task.Files)

	go w.doReduce2(task)
	return nil
}

// pharse2: stop current job if received RPC Call from master that stop the worker (timeout)
func (w *WorkerBlock) doReduce2(task *ReduceTask) {
	done := make(chan bool)
	interrupt := make(chan bool)
	go w.doReduce3(task, done, interrupt)
	log.Printf("worker doReduce2: %v %v task: bucket: %v stat: %v\n", w.id, w.stat, task.Bucket, task.Stat)

	select {
	// actively quit, may be caused by timeout
	case <-w.quit:
		w.mu.Lock()
		select {
		case interrupt <- true:
		default:
		}
		w.stat = Spare
		w.mu.Unlock()

		workerStat := WorkerState{ID: w.id, Stat: Spare, IsReduceDone: false}
		var reply bool
		call(masterSock(), "Coordinator.ReportReduceState", &workerStat, &reply)
		log.Printf("worker quit: %v %v task: bucket: %v stat: %v\n", w.id, w.stat, task.Bucket, task.Stat)
	// work done
	case <-done:
		w.mu.Lock()
		w.stat = Spare
		w.mu.Unlock()

		workerStat := WorkerState{ID: w.id, Stat: Spare, IsReduceDone: true}
		var reply bool
		call(masterSock(), "Coordinator.ReportReduceState", &workerStat, &reply)
		log.Printf("worker done: %v %v task: bucket: %v stat: %v\n", w.id, w.stat, task.Bucket, task.Stat)
	}
}

func (w *WorkerBlock) doReduce3(task *ReduceTask, d chan<- bool, iurpt <-chan bool) {
	log.Printf("worker doReduce3: %v %v task: bucket: %v stat: %v\n", w.id, w.stat, task.Bucket, task.Stat)

	// load and read decode json files
	intermediate := []KeyValue{}
	for _, file := range task.Files {
		log.Printf("%v reading %v\n", w.id, file)
		f, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v %w\n", f.Name(), err)
		}

		decoder := json.NewDecoder(f)
		kva := new(KeyValue)
		for {
			if err := decoder.Decode(&kva); err != nil {
				break
			}
			if kva.Key == "ABOUT" {
				log.Printf("%v worker:%v fname:%v bucket:%v\n", kva, w.id, f.Name(), task.Bucket)
			}
			intermediate = append(intermediate, *kva)
		}
		f.Close()
	}
	sort.Sort(ByKey(intermediate))

	// create tmp file
	pwd, _ := os.Getwd()
	fname := fmt.Sprintf("tmp-%d-*", task.Bucket)
	tmpf, err := ioutil.TempFile(pwd, fname)
	if err != nil {
		log.Fatal("creating tmp files: ", err)
	}
	log.Printf("worker: %v tmpf created: %v", w.id, tmpf.Name())

	// write kv to mr-out-$bucket
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		fmt.Fprintf(tmpf, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	// mv tmpfiles
	fname = fmt.Sprintf("%v/mr-out-%v", pwd, task.Bucket)
	os.Rename(tmpf.Name(), fname)
	tmpf.Close()

	select {
	case <-iurpt:
		// already timeout, remove files
		os.Remove(fname)
		return
	default:
		// non-blocking, tell work done
		select {
		case d <- true:
			log.Printf("worker: %v %v reduce result has been written into %v\n", w.id, w.stat, fname)
		default:
		}
	}
}

// master query worker's state
// if arg.IsChanged == true, then worker is timeout and requested to stop its job
func (w *WorkerBlock) Stop(arg *ChangeState, stat *State) error {
	log.Printf("WORKER RPC STATE, NumGoroutine: %d, ChangeState: %v\n", runtime.NumGoroutine(), arg)

	// stop worker's current work, timeout
	if arg.IsChanged == true && arg.Stat == Spare {
		// non-blocking
		select {
		case w.quit <- true:
		default:
		}
		w.mu.Lock()
		w.stat = Spare
		w.mu.Unlock()
	}

	log.Println("worker RPC state, NumGoroutine: ", runtime.NumGoroutine())
	*stat = w.stat
	return nil
}

// shutdown
func (w *WorkerBlock) Shutdown(arg *bool, reply *bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stat = Done
	log.Printf("Worker %v exit\n", w.id)
	go func() {
		time.Sleep(deltaTime)
		os.Exit(0)
	}()
	return nil
}
