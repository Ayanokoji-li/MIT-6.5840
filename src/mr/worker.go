package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type KeyValue2 struct {
	Key    string
	Values []string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerID := initWorker()

	for !CheckDone() {

		task := getTask(workerID)
		if task.TaskType == None {
			log.Printf("no task")
			time.Sleep(time.Duration(1) * time.Second)
		} else {
			log.Printf("handle task")
			handleTask(task, mapf, reducef)
		}
	}
}

func initWorker() int {
	args := InitArgs{}
	reply := InitReply{}

	ok := call("Coordinator.InitWorker", &args, &reply)

	if ok {
		return reply.WorkerID
	} else {
		return -1
	}
}

func getTask(workerID int) GetTaskReply {
	args := GetTaskArgs{WorkerID: workerID}
	reply := GetTaskReply{TaskType: None, WorkerID: workerID}

	call("Coordinator.GetTask", &args, &reply)

	log.Printf("get nReduce: %d", reply.MapTask.NReduce)
	log.Printf("get filename: %s", reply.MapTask.FileName)
	log.Print(reply.MapTask.Contents)
	return reply
}

func handleTask(task GetTaskReply, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	if task.TaskType == Map {
		nReduce := task.MapTask.NReduce
		workerID := task.WorkerID
		base_filename := fmt.Sprintf("mr-inter-%d", workerID)
		file_list := make([]*os.File, nReduce)
		to_write_list := make([][]KeyValue2, nReduce)
		for i := range nReduce {
			filename := fmt.Sprintf("%s-%d", base_filename, i)
			file, err := os.Create(filename)
			if err != nil {
				log.Printf("open file %s failed", filename)
			}

			file_list[i] = file
		}

		intermediate := mapf(task.MapTask.FileName, task.MapTask.Contents)
		sort.Sort(ByKey(intermediate))
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			// output := reducef(intermediate[i].Key, values)
			to_write_list[ihash(intermediate[i].Key)%nReduce] = append(to_write_list[ihash(intermediate[i].Key)%nReduce], KeyValue2{Key: intermediate[i].Key, Values: values})
			i = j
		}

		for i := range nReduce {
			jsonData, _ := json.MarshalIndent(to_write_list[i], "", "  ")
			file_list[i].Write(jsonData)
		}
		args := FinishTaskArgs{TaskType: Map, TaskFilename: task.MapTask.FileName}
		reply := FinishTaskReply{}
		call("Coordinator.FinishTask", &args, &reply)
	}
}

func CheckDone() bool {
	args := CheckDoneArgs{}
	reply := CheckDoneReply{}

	ok := call("Coordinator.CheckDone", &args, &reply)

	return ok && reply.IsDone
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
