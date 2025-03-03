package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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
	// log.SetOutput(io.Discard)
	workerID := initWorker()

	for !CheckDone() {

		task := getTask(workerID)
		if task.TaskType == None {
			log.Printf("no task")
			time.Sleep(time.Duration(1) * time.Second)
		} else {
			handleTask(task, mapf, reducef)
		}
	}

	log.Printf("recv end signal")
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
	return reply
}

func handleTask(task GetTaskReply, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	if task.TaskType == Map {

		// init meta data
		nReduce := task.MapTask.NReduce
		task_filename := task.MapTask.FileName
		workerID := task.WorkerID

		// init output file
		output_base_filename := fmt.Sprintf("mr-%s-%d", filepath.Base(task_filename), workerID)
		output_file_list := make([]*os.File, nReduce)
		to_write_list := make([][]KeyValue2, nReduce)

		for i := range nReduce {
			filename := fmt.Sprintf("%s-%d", output_base_filename, i)
			file := create_file(filename)
			output_file_list[i] = file
		}

		// read target file
		content := get_file_content(task_filename)

		// get intermediate
		intermediate := mapf(task.MapTask.FileName, content)
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

		// write intermediate
		for i := range nReduce {
			jsonData, _ := json.MarshalIndent(to_write_list[i], "", "  ")
			output_file_list[i].Write(jsonData)
			output_file_list[i].Close()
		}

		// inform finish
		args := FinishTaskArgs{TaskType: Map, TaskFilename: task_filename, ResBaseFilename: output_base_filename}
		reply := FinishTaskReply{}
		call("Coordinator.FinishTask", &args, &reply)
	} else if task.TaskType == Reduce {

		//nReduce := task.ReduceTask.NReduce
		task_ID := task.ReduceTask.TaskID
		//workerID := task.WorkerID

		combined_res := make(map[string][]string)
		reduce_res := make([]KeyValue, 0)

		for _, base_filename := range task.ReduceTask.FileList {
			filename := fmt.Sprintf("%s-%d", base_filename, task_ID)
			contents := get_file_content(filename)

			var arr_kv2 []KeyValue2
			err := json.Unmarshal([]byte(contents), &arr_kv2)

			if err != nil {
				log.Printf("can't unamrshal %s", filename)
			}

			for _, kv := range arr_kv2 {
				_, exist := combined_res[kv.Key]
				if !exist {
					combined_res[kv.Key] = kv.Values
				} else {
					combined_res[kv.Key] = append(combined_res[kv.Key], kv.Values...)
				}
			}
		}

		for key, value := range combined_res {
			res := reducef(key, value)
			reduce_res = append(reduce_res, KeyValue{Key: key, Value: res})
		}

		sort.Sort(ByKey(reduce_res))

		filename := fmt.Sprintf("mr-out-%d", task_ID)
		file := create_file(filename)
		defer file.Close()

		for _, kv := range reduce_res {
			fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
		}

		args := FinishTaskArgs{TaskID: task_ID, TaskType: Reduce}
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

func create_file(filename string) *os.File {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("can't create %s", filename)
	}

	return file
}

func get_file(filename string) *os.File {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("can't open %s", filename)
	}

	return file
}

func get_file_content(filename string) string {
	file := get_file(filename)
	// defer file.Close()
	contents, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("can't read %s", filename)
	}

	return string(contents)
}
