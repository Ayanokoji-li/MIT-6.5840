package mr

import (
	"container/list"
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
	// Your definitions here.
	MapTaskStatus    map[string]TaskStatus
	MapTaskList      list.List
	MapTaskRes       map[string][]string
	ReduceTaskStatus []TaskStatus
	ReduceTaskList   list.List

	mapDone    bool
	reduceDone bool
	allDone    bool

	Mu sync.Mutex

	nReduce    int
	worker_num int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) print() {
	fmt.Println("MapTaskMap:", c.MapTaskStatus)
	fmt.Println("MapTaskList:")
	for e := c.MapTaskList.Front(); e != nil; e = e.Next() {
		fmt.Println("  ", e.Value)
	}
	fmt.Println("MapTaskRes:", c.MapTaskRes)
	fmt.Println("ReduceTaskMap:", c.ReduceTaskStatus)
	fmt.Println("ReduceTaskList:")
	for e := c.ReduceTaskList.Front(); e != nil; e = e.Next() {
		fmt.Println("  ", e.Value)
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

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

		filename := task_elm.Value.(string)
		reply.TaskType = Map
		reply.MapTask.NReduce = c.nReduce
		reply.MapTask.FileName = filename

		c.Mu.Lock()
		c.MapTaskStatus[filename] = Assigned
		c.MapTaskList.Remove(task_elm)
		c.Mu.Unlock()

		log.Printf("map task send")
		log.Printf("nReduce: %d", reply.MapTask.NReduce)

		go func() {
			time.Sleep(time.Duration(1) * time.Second)
			c.Mu.Lock()
			defer c.Mu.Unlock()

			if c.MapTaskStatus[filename] == Assigned {
				c.MapTaskStatus[filename] = Todo
				c.MapTaskList.PushBack(filename)
				log.Printf("%s time out, reassign", filename)
			}
		}()
	}

	if c.mapDone {

	}

	return nil
}

func (c *Coordinator) CheckDone(args *CheckDoneArgs, reply *CheckDoneReply) error {
	reply.IsDone = c.allDone

	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {

	log.Printf("recv finish task")
	if args.TaskType == Map {
		c.MapTaskStatus[args.TaskFilename] = Done
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

	return true
}

func (c *Coordinator) checkReduceDone() bool {
	for _, value := range c.ReduceTaskStatus {
		if value != Done {
			return false
		}
	}

	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTaskStatus:    make(map[string]TaskStatus),
		MapTaskRes:       make(map[string][]string),
		ReduceTaskStatus: make([]TaskStatus, 0),
		mapDone:          false,
		reduceDone:       false,
		allDone:          false,
		nReduce:          nReduce,
		worker_num:       0,
	}

	// Your code here.
	for _, file := range files {
		c.MapTaskStatus[file] = Todo
	}
	for file := range c.MapTaskStatus {
		c.MapTaskList.PushBack(file)
	}
	c.print()
	c.server()
	return &c
}
