package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	idle bool
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	mr.eventLoop()
	return mr.KillWorkers()
}

type JobResult struct {
	ok         bool
	jobId      int
	workerAddr string
}

type JobSet struct {
	t           JobType
	totalJobNum int
	doneJobNum  int

	undispatchedJobMap map[int]bool
}

func NewJobSet(t JobType, totalJobNum int) *JobSet {
	js := &JobSet{
		t:                  t,
		totalJobNum:        totalJobNum,
		undispatchedJobMap: make(map[int]bool),
	}
	for i := 0; i < totalJobNum; i++ {
		js.undispatchedJobMap[i] = true
	}
	return js
}

func (js *JobSet) TryDispatchJob() int {
	for jobId := range js.undispatchedJobMap {
		delete(js.undispatchedJobMap, jobId)
		return jobId
	}
	return -1
}

func (js *JobSet) JobDone(jobId int, ok bool) {
	if !ok {
		js.undispatchedJobMap[jobId] = true
	} else {
		js.doneJobNum++
	}
}

func (js *JobSet) IsAllJobDone() bool {
	return js.doneJobNum == js.totalJobNum
}

func (js *JobSet) Type() JobType {
	return js.t
}

func (mr *MapReduce) eventLoop() {
	js := NewJobSet(Map, mr.nMap)

	for quit := false; !quit; {
		select {
		case workerAddr := <-mr.registerChannel:
			mr.Workers[workerAddr] = &WorkerInfo{
				address: workerAddr,
			}
			jobId := js.TryDispatchJob()
			if jobId == -1 {
				mr.Workers[workerAddr].idle = true
				break
			}
			go mr.dispatchJob(js.t, workerAddr, jobId)
		case result := <-mr.JobDoneWKChannel:
			js.JobDone(result.jobId, result.ok)
			if !result.ok {
				delete(mr.Workers, result.workerAddr)
				break
			}
			mr.Workers[result.workerAddr].idle = true
			if js.IsAllJobDone() {
				if js.Type() == Map {
					js = NewJobSet(Reduce, mr.nReduce)
					for _, wk := range mr.Workers {
						if !wk.idle {
							continue
						}
						jobId := js.TryDispatchJob()
						if jobId == -1 {
							break
						}
						wk.idle = false
						go mr.dispatchJob(js.t, wk.address, jobId)
					}
				} else {
					quit = true
					break
				}
			} else {
				jobId := js.TryDispatchJob()
				if jobId == -1 {
					break
				}
				mr.Workers[result.workerAddr].idle = false
				go mr.dispatchJob(js.t, result.workerAddr, jobId)
			}
		}
	}
}

func (mr *MapReduce) dispatchJob(operation JobType, workerAddr string, jobNumber int) {
	args := &DoJobArgs{
		File:      mr.file,
		Operation: operation,
		JobNumber: jobNumber,
	}
	if operation == Map {
		args.NumOtherPhase = mr.nReduce
	} else {
		args.NumOtherPhase = mr.nMap
	}
	var reply DoJobReply
	ok := call(workerAddr, "Worker.DoJob", args, &reply)
	mr.JobDoneWKChannel <- JobResult{ok: ok && reply.OK, jobId: jobNumber, workerAddr: workerAddr}
}
