package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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

type TaskType string
type TaskStatus int8

const (
	Map    TaskType = "Map"
	Reduce TaskType = "Reduce"
	None   TaskType = "None"

	Done     TaskStatus = 0
	Assigned TaskStatus = 1
	Todo     TaskStatus = 2
)

type InitArgs struct {
}

type InitReply struct {
	WorkerID int
}

type GetTaskArgs struct {
	WorkerID int
}

type GetTaskReply struct {
	TaskType   TaskType
	MapTask    MapTask
	ReduceTask ReduceTask
	WorkerID   int
}

type CheckDoneArgs struct {
}

type CheckDoneReply struct {
	IsDone bool
}

type FinishTaskArgs struct {
	TaskFilename    string
	ResBaseFilename string
	TaskType        TaskType
}

type FinishTaskReply struct {
}

// Add your RPC definitions here.

type MapTask struct {
	FileName string
	Contents string
	NReduce  int
}

type ReduceTask struct {
	ID int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
