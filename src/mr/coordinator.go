package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mu			sync.Mutex

	nReduce 	int
	workers 	map[string]*WorkerState
	files  		[]string
	rTasks     	[]*ReduceTask
	mTasks     	[]*Task
	assignment 	map[string]*Task
	asignReduce map[string]*ReduceTask

	isDone 		bool
	masterStat	State
}


// start a thread that listens for RPCs from worker.go
func (m *Coordinator) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, err := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatal("listen error: ", err)
	}
	go http.Serve(l, nil)

	ticker := time.Tick(deltaTime)
	go m.schedule(ticker)
	go timeout()
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Coordinator) Done() bool {
	return m.isDone
}

// create a Coordinator.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce mTasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := Coordinator{nReduce: nReduce, files: files, isDone: false, masterStat: Mapping}
	m.workers = make(map[string]*WorkerState)
	m.assignment = make(map[string]*Task)
	m.asignReduce = make(map[string]*ReduceTask)
	m.initMapTask()
	m.initReduceTask()
	m.server()
	initDir()

	return &m
}

// schedule workers
func (m *Coordinator) schedule(ticker <-chan time.Time)  {
	for {
		select {
		case <-ticker:
			m.mu.Lock()
			switch m.masterStat {
			case Mapping:
				m.mu.Unlock()
				log.Println("Mapping...")

				// assign mTasks to free workers
				for _, w := range m.workers {
					var workerStat State

					if w.Stat == Spare {
						t := m.getSpareMapTask()
						if t != nil {
							log.Printf("getSpareMapTask:%v Stat:%v Files:%v %v, worker: %v %v\n",
								t.ID, t.Stat, len(t.Filename), t.Filename, w.ID, w.Stat)
						} else {
							break
						}

						m.call(w, "WorkerBlock.DoMap", t, &workerStat)
						m.mu.Lock()
						m.assignment[w.ID] = t
						t.Stat = workerStat
						w.Stat = workerStat
						m.mu.Unlock()
						m.stopResetTimer(w.timer, workerTimeout)

						log.Println("2CALL worker to map: ", w.ID, w.Stat)
					} else if w.Stat == Mapping {
						select {
						case t2 := <-w.timer.C:
							log.Printf("w.timer.C... %v worker:%v\n", t2, w.ID)
							newStat := ChangeState{IsChanged: true, Stat: Spare}
							workerStat = Mapping
							m.call(w, "WorkerBlock.Stop", &newStat, &workerStat)

							m.mu.Lock()
							// return normally, worker timeout
							if workerStat == Spare {
								m.workers[w.ID].Stat = Spare
								log.Printf("worker %v timeout\n", w.ID)
							} else {	// worker crashed
								log.Printf("worker %v crashed\n", w.ID)
								if _, ok := m.workers[w.ID]; ok {
									delete(m.workers, w.ID)
									log.Printf("worker %v removed\n", w.ID)
								}
							}
							if _, ok := m.assignment[w.ID]; ok {
								log.Printf("remove asignReduce worker: %v task: %v %v %v\n",
									w.ID, m.assignment[w.ID].ID, m.assignment[w.ID].Stat, m.assignment[w.ID].Filename)

								m.assignment[w.ID].Stat = Spare
								delete(m.assignment, w.ID)
							} else {
								m.showAssignment()
							}
							m.stopResetTimer(w.timer, workerTimeout)

							m.showWorkers()
							m.showMTasks()

							log.Println("QUERY worker's state ", w.ID, w.Stat)
							m.mu.Unlock()
						default:
						}
					}
				}
			case Reducing:
				m.mu.Unlock()
				log.Println("Reducing...")

				// assign mTasks to free workers
				for _, w := range m.workers {
					var workerStat State

					if w.Stat == Spare {
						t := m.getSpareReduceTask()
						if t != nil {
							log.Printf("getSpareReduceTask: Bucket:%v Stat:%v Files:%v %v, worker: %v\n",
								t.Bucket, t.Stat, len(t.Files), t.Files, w)
						} else {
							break
						}

						m.call(w, "WorkerBlock.DoReduce", t, &workerStat)
						m.mu.Lock()
						m.asignReduce[w.ID] = t
						t.Stat = workerStat
						w.Stat = workerStat
						m.mu.Unlock()
						m.stopResetTimer(w.timer, workerTimeout)

						log.Printf("2CALL worker %v %v to reduce: Task: Bucket:%v Stat:%v Files:%v %v",
							w.ID, w.Stat, t.Bucket, t.Stat, t.Files, len(t.Files))
					} else if w.Stat == Reducing {
						select {
						case t2 := <-w.timer.C:
							log.Printf("w.timer.C... %v worker:%v\n", t2, w.ID)
							newStat := ChangeState{IsChanged: true, Stat: Spare}
							workerStat = Reducing
							m.call(w, "WorkerBlock.Stop", &newStat, &workerStat)

							m.mu.Lock()
							// if return normally
							if workerStat == Spare {
								m.workers[w.ID].Stat = Spare
								log.Printf("worker %v timeout\n", w.ID)
							} else {	// worker has crashed
								log.Printf("worker %v crashed\n", w.ID)
								if _, ok := m.workers[w.ID]; ok {
									delete(m.workers, w.ID)
									log.Printf("worker %v removed\n", w.ID)
								}
							}
							if _, ok := m.asignReduce[w.ID]; ok {
								log.Printf("remove asignReduce worker: %v task: %v %v %v %v\n",
									w.ID, m.asignReduce[w.ID].Bucket, m.asignReduce[w.ID].Stat,
									len(m.asignReduce[w.ID].Files), m.asignReduce[w.ID].Files)

								m.asignReduce[w.ID].Stat = Spare
								delete(m.asignReduce, w.ID)
							}
							m.showWorkers()
							m.showMTasks()
							log.Println("QUERY worker's state ", w.ID, w.Stat)

							m.stopResetTimer(w.timer, workerTimeout)
							m.mu.Unlock()
						default:
						}
					}
				}
			default:
				m.mu.Unlock()
			}
		}
	}
}

// RPC call worker
func (m *Coordinator) call(w *WorkerState, rpcname string, args interface{}, reply interface{}) {
	if err := call(workerSock(w.ID), rpcname, args, reply); err != nil {
		log.Println(err)
		//// consider the worker as crashed, remove info
		//if _, ok := m.workers[w.ID]; ok {
		//	delete(m.workers, w.ID)
		//}
		//if _, ok := m.assignment[w.ID]; ok {
		//	delete(m.assignment, w.ID)
		//}
	}
}

func (m *Coordinator) initMapTask() {
	m.mTasks = make([]*Task, 0, len(m.files))
	for _, f := range m.files {
		m.mTasks = append(m.mTasks, &Task{ID: getTaskID(f), NReduce: m.nReduce, Filename: f, Stat: Spare})
	}
}

func (m *Coordinator) initReduceTask() {
	m.rTasks = make([]*ReduceTask, m.nReduce, m.nReduce)
	for i, _ := range m.rTasks {
		m.rTasks[i] = &ReduceTask{Bucket: i, Stat: Spare}
		m.rTasks[i].Files = make([]string, 0, len(m.files))
	}
}

// returns a pointer to a Spare MapTask
func (m *Coordinator) getSpareMapTask() *Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.mTasks {
		if t.Stat == Spare {
			return t
		}
	}
	return nil
}

// returns a pointer to a Spare ReduceTask
func (m *Coordinator) getSpareReduceTask() *ReduceTask {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.rTasks {
		if t.Stat == Spare {
			return t
		}
	}
	return nil
}

// Stop and reset the time.Timer
func (m *Coordinator) stopResetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

func newTimer() *time.Timer {
	t := time.NewTimer(0)
	if !t.Stop() {
		<-t.C
	}
	return t
}

// dir oper
func initDir() {
	pwd, _ := os.Getwd()
	tmpwd := fmt.Sprintf("%s/tmp/", pwd)
	mapwd := fmt.Sprintf("%s/map/", pwd)
	os.Mkdir(tmpwd, 0766)
	os.Mkdir(mapwd, 0766)
	removeAll(tmpwd)
	removeAll(mapwd)
}

func (m *Coordinator) mapDone() {
	if m.masterStat == Mapping {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, t := range m.mTasks {
			if t.Stat != Done {
				return
			}
		}
		m.masterStat = Reducing
		for i, task := range m.mTasks {
			log.Printf("%v %v", i, *task)
		}
		log.Println("Map done")
	}
}

func (m *Coordinator) reduceDone() {
	if m.masterStat == Reducing {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, t := range m.rTasks {
			if t.Stat != Done {
				return
			}
		}
		m.masterStat = Done
		for i, task := range m.rTasks {
			log.Printf("%v %v", i, *task)
		}
		log.Println("Reduce done")
		for _, w := range m.workers {
			b := true
			m.call(w, "WorkerBlock.Shutdown", &b, &b)
		}
		time.Sleep(2 * deltaTime)
		os.Exit(0)
	}
}

func (m *Coordinator) showWorkers() {
	log.Printf("Alive Workers: %v\n", len(m.workers))
	for _, worker := range m.workers {
		log.Printf("%v %v\n", worker.ID, worker.Stat)
	}
}

func (m *Coordinator) showMTasks() {
	log.Printf("MTasks: %v\n", len(m.mTasks))
	for _, t := range m.mTasks {
		log.Printf("%v %v %v\n", t.ID, t.Stat, t.Filename)
	}
}

func (m *Coordinator) showAssignment() {
	log.Printf("assignment: %v\n", len(m.assignment))
	for w, t := range m.assignment {
		log.Printf("worker:%v task:%v %v %v\n", w, t.ID, t.Stat, t.Filename)
	}
}

func (m *Coordinator) showRTasks() {
	log.Printf("RTasks: %v\n", len(m.rTasks))
	for _, t := range m.rTasks {
		log.Printf("%v %v %v\n", t.Bucket, t.Stat, t.Files)
	}
}

func timeout() {
	time.Sleep(masterLifetime)
	os.Exit(0)
}
