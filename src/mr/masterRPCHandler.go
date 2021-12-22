package mr

import (
	"log"
	"strconv"
	"strings"
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Coordinator) Register(w *WorkerState, reply *AckReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workers[w.ID] = w
	m.workers[w.ID].timer = newTimer()

	*reply = true
	log.Printf("Worker REGISTERED: %v %v\n", w.ID, *w)

	log.Printf("Alive Workers: %v\n", len(m.workers))
	for _, worker := range m.workers {
		log.Printf("%v %v\n", worker.ID, worker.Stat)
	}

	return nil
}

// master in Mapping state, worker actively report its state
func (m *Coordinator) ReportMapState(w *WorkerState, reply *AckReply) error {
	log.Printf("ReportMapState w.ID: %v\n", w.ID)
	if _, ok := m.workers[w.ID]; !ok || w.Stat == m.workers[w.ID].Stat {
		return nil
	}

	// set corresponding task's state to Done, set master'workers state record to spare
	m.mu.Lock()
	if w.Stat == Spare {
		m.workers[w.ID].Stat = Spare
		m.workers[w.ID].timer = newTimer()
		if w.IsMapDone == true {
			if len(w.Files) != m.nReduce {
				log.Println("Files missed")
				return nil
			}
			// add mapfiles to reduceTasks
			for _, f := range w.Files {
				strs := strings.Split(f, "-")
				bucket, _ := strconv.Atoi(strs[len(strs) - 1])
				m.rTasks[bucket].Files = append(m.rTasks[bucket].Files, f)
			}
			m.assignment[w.ID].Stat = Done
			log.Printf("ReportMapState files: %v %v\n", len(w.Files), w.Files)
		} else {
			m.assignment[w.ID].Stat = Spare
		}

		log.Printf("remove asignReduce worker: %v task: %v %v %v\n",
			w.ID, m.assignment[w.ID].ID, m.assignment[w.ID].Stat, m.assignment[w.ID].Filename)
		delete(m.assignment, w.ID)
	}
	m.mu.Unlock()


	*reply = true
	m.mapDone()
	return nil
}

// master in Reduing state, worker actively report its state
func (m *Coordinator) ReportReduceState(w *WorkerState, reply *AckReply) error {
	log.Printf("ReportReduceState w.ID: %v\n", w.ID)
	if _, ok := m.workers[w.ID]; !ok || w.Stat == m.workers[w.ID].Stat {
		return nil
	}

	// set corresponding task's state to Done, set master'workers state record to spare
	m.mu.Lock()
	if w.Stat == Spare {
		m.workers[w.ID].Stat = Spare
		m.workers[w.ID].timer = newTimer()
		if w.IsReduceDone {
			m.asignReduce[w.ID].Stat = Done
		} else {
			m.asignReduce[w.ID].Stat = Spare
		}
		delete(m.asignReduce, w.ID)
	} else {
		m.asignReduce[w.ID].Stat = Spare
	}
	m.mu.Unlock()

	log.Printf("m.asignReduce: %v m.rTasks: %v worker: %v\n",
		m.asignReduce, m.rTasks, *m.workers[w.ID])
	*reply = true
	m.reduceDone()
	return nil
}

