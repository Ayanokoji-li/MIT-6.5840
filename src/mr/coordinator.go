package mr

import (
	"container/list"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	MapTaskStatus     map[string]TaskStatus
	MapTaskList       list.List
	MapTaskRes        map[string]string
	MapTaskResCollect []string
	ReduceTaskStatus  []TaskStatus
	ReduceTaskList    list.List

	mapDone    bool
	reduceDone bool
	allDone    bool

	Mu sync.Mutex

	nReduce    int
	worker_num int
}

const (
	WAIT_TIME int = 5
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) InitWorker(args *InitArgs, reply *InitReply) error {
	c.worker_num += 1
	reply.WorkerID = c.worker_num

	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if !c.mapDone {
		task_elm := c.MapTaskList.Front()
		if task_elm == nil {
			reply.TaskType = None
			return nil
		}

		// init reply
		filename := task_elm.Value.(string)
		reply.TaskType = Map
		reply.MapTask.NReduce = c.nReduce
		reply.MapTask.FileName = filename

		// update task status
		c.Mu.Lock()
		c.MapTaskStatus[filename] = Assigned
		c.MapTaskList.Remove(task_elm)
		c.Mu.Unlock()

		log.Printf("send map task %s", filename)

		// recover time-out task
		go func() {
			time.Sleep(time.Duration(WAIT_TIME) * time.Second)
			c.Mu.Lock()
			defer c.Mu.Unlock()

			if c.MapTaskStatus[filename] == Assigned {
				c.MapTaskStatus[filename] = Todo
				c.MapTaskList.PushBack(filename)
				log.Printf("Map task %s time out, reassign", filename)
			}
		}()
	}

	if c.mapDone {
		task_elm := c.ReduceTaskList.Front()
		if task_elm == nil {
			reply.TaskType = None
			return nil
		}
		task_id := task_elm.Value.(int)

		reply.TaskType = Reduce
		reply.ReduceTask.TaskID = task_id
		reply.ReduceTask.FileList = c.MapTaskResCollect

		c.Mu.Lock()
		defer c.Mu.Unlock()
		c.ReduceTaskList.Remove(task_elm)
		c.ReduceTaskStatus[task_id] = Assigned

		log.Printf("send reduce task %d", task_id)

		// recover time-out task
		go func() {
			time.Sleep(time.Duration(WAIT_TIME) * time.Second)
			c.Mu.Lock()
			defer c.Mu.Unlock()

			if c.ReduceTaskStatus[task_id] == Assigned {
				c.ReduceTaskStatus[task_id] = Todo
				c.ReduceTaskList.PushBack(task_id)
				log.Printf("Reduce task %d time out, reassign", task_id)
			}
		}()
	}

	return nil
}

func (c *Coordinator) CheckDone(args *CheckDoneArgs, reply *CheckDoneReply) error {
	reply.IsDone = c.allDone

	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {

	if args.TaskType == Map {
		log.Printf("recv finished map task %s", args.TaskFilename)
		c.Mu.Lock()
		defer c.Mu.Unlock()
		c.MapTaskStatus[args.TaskFilename] = Done
		c.MapTaskRes[args.TaskFilename] = args.ResBaseFilename
	} else if args.TaskType == Reduce {
		log.Printf("recv finished reduce task %d", args.TaskID)
		c.Mu.Lock()
		defer c.Mu.Unlock()
		c.ReduceTaskStatus[args.TaskID] = Done
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if c.allDone {
		return true
	}

	if !c.mapDone {
		c.mapDone = c.checkMapDone()
	}

	if c.mapDone && !c.reduceDone {
		c.reduceDone = c.checkReduceDone()
	}

	c.allDone = c.mapDone && c.reduceDone

	return c.allDone
}

func (c *Coordinator) checkMapDone() bool {
	for _, value := range c.MapTaskStatus {
		if value != Done {
			return false
		}
	}

	log.Printf("Map task end")

	c.Mu.Lock()
	defer c.Mu.Unlock()

	for _, value := range c.MapTaskRes {
		c.MapTaskResCollect = append(c.MapTaskResCollect, value)
	}

	log.Printf("res collect: ")
	log.Print(c.MapTaskResCollect)

	return true
}

func (c *Coordinator) checkReduceDone() bool {
	for _, value := range c.ReduceTaskStatus {
		if value != Done {
			return false
		}
	}

	log.Printf("All task finished")
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTaskStatus:    make(map[string]TaskStatus),
		MapTaskRes:       make(map[string]string),
		ReduceTaskStatus: make([]TaskStatus, nReduce),
		mapDone:          false,
		reduceDone:       false,
		allDone:          false,
		nReduce:          nReduce,
		worker_num:       0,
	}

	// init task status and task list
	for _, file := range files {
		c.MapTaskStatus[file] = Todo
	}
	for file := range c.MapTaskStatus {
		c.MapTaskList.PushBack(file)
	}

	for i := range nReduce {
		c.ReduceTaskStatus[i] = Todo
		c.ReduceTaskList.PushBack(i)
	}

	c.server()
	log.SetOutput(io.Discard)
	log.Printf("Coordinator server starts")
	return &c
}
