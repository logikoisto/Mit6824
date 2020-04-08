package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var workerID uint64

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	workerID = Register()
	go func() {
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		for {
			<-timer.C
			PingPong()
		}
	}()
	for {
		// 1.获取任务
		task := GetTask()
		// 2.根据任务类型执行任务
		// TODO: 是否应该用err 来返回 这样更优雅?
		res := ExecTask(mapf, reducef, task)
		if res == nil {
			break
		}
		// 3.报告结果
		Report(res)
	}
}

//
// example function to show how to make an RPC call to the master.
//

const (
	CallRegister = "Master.RegisterWorker"
	CallPingPong = "Master.Health"
	CallGetTask  = "Master.GetTaskWorker"
	CallReport   = "Master.ReportResult"
)

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// TODO: 是否可以包装成对象
func Register() uint64 {
	args, reply := RegisterReq{}, RegisterRes{}
	// TODO: 处理请求异常?
	call(CallRegister, &args, &reply)
	return reply.WorkerID
}
func PingPong() {
	args, reply := Ping{WorkerID: workerID}, Pong{}
	call(CallPingPong, &args, &reply)
	//TODO:解析PingPong异常?
}
func GetTask() *Task {
	args, reply := GetTaskReq{WorkerID: workerID}, GetTaskRes{}
	call(CallGetTask, &args, &reply)
	//TODO:解析异常?
	return reply.T
}

func ExecTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, task *Task) *ResultReq {
	if task.Type == 0 {
		// TODO: 执行 map 任务
		_ = doMap(mapf, task)
	} else if task.Type == 1 {
		// TODO: 执行 reduce 任务
		_ = doReduce(reducef, task)
	}
	return nil
}

func Report(res *ResultReq) {
	reply := ResultRes{}
	call(CallReport, res, &reply)
	//TODO:处理返回异常?
}

func doMap(mapf func(string, string) []KeyValue, task *Task) error {
	return nil
}
func doReduce(reducef func(string, []string) string, task *Task) error {
	return nil
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
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
