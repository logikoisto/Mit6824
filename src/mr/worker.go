package mr

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		tc := time.NewTicker(10 * time.Second)
		defer tc.Stop()
		for {
			<-tc.C
			PingPong()
		}
	}()
	var task *Task
	for {
		// 1.获取任务
		task = GetTask()
		// 2.根据任务类型执行任务
		res, err := ExecTask(mapf, reducef, task)
		if err != nil {
			continue
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
	CallPingPong = "Master.PingPong"
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
	reducef func(string, []string) string, task *Task) (*ResultReq, error) {
	var res []string
	req := &ResultReq{WorkerID: workerID}
	if task == nil {
		return nil, fmt.Errorf("retry") // TODO:应该定义一种错误类型
	}
	if task.Type == 0 {
		// TODO: 执行 map 任务
		res, _ = doMap(mapf, task)
		req.Code = 0
	} else if task.Type == 1 {
		// TODO: 执行 reduce 任务
		res, _ = doReduce(reducef, task)
		req.Code = 1
	} else if task.Type == 2 {
		os.Exit(0)
	}
	if len(res) == 0 {
		return nil, fmt.Errorf("retry") // TODO:应该定义一种错误类型
	}
	req.M = res
	return req, nil
}

func Report(res *ResultReq) {
	reply := ResultRes{}
	call(CallReport, res, &reply)
	//TODO:处理返回异常?
}

func doMap(mapf func(string, string) []KeyValue, task *Task) ([]string, error) {
	// TODO:对task进行检查
	res := make([]string, 0)
	fileName := task.Conf.Source[0]
	file, err := os.Open(fileName)
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return nil, fmt.Errorf("doMap.Open.err:%s", err.Error())
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("doMap.ReadAll.err:%s", err.Error()))
	}
	cacheMap := make(map[string][]KeyValue, 0)
	for i := 0; i < task.Conf.RC; i++ {
		key := fmt.Sprintf("mr-worker-%d-%d.out", task.Conf.MNum, i)
		cacheMap[key] = []KeyValue{}
		res = append(res, key)
	}
	kva := mapf(fileName, string(content))
	for i := 0; i < len(kva); i++ {
		idx := ihash(kva[i].Key) % task.Conf.RC // TODO: ihash(kva[i].Key) & (task.Conf.RC - 1)
		key := fmt.Sprintf("mr-worker-%d-%d.out", task.Conf.MNum, idx)
		cacheMap[key] = append(cacheMap[key], kva[i])
	}

	for key, value := range cacheMap {
		sort.Sort(ByKey(value))
		// TODO: 在这里可以调用一次 reduce函数进行合并 以减少网络调用
		combine(value)
		// TODO: 这里是否也存在的 map函数生成文件的幂等问题
		outFile, _ := os.Create(key)
		for i := 0; i < len(value); i++ {
			_, _ = fmt.Fprintf(outFile, "%v %v\n", value[i].Key, value[i].Value)
		}
		_ = outFile.Close()
	}
	return res, nil
}

func doReduce(reducef func(string, []string) string, task *Task) ([]string, error) {
	// TODO: 检查task
	// TODO: 外部排序? -> 先实现一个内存排序吧
	kvas := readFiles(task)
	tmpFileName := fmt.Sprintf("mr-out-%d.%d.swap", time.Now().Unix(), task.Conf.RNum)
	outFile, _ := os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	defer outFile.Close()
	// TODO:先在内存排个顺序
	sort.Sort(ByKey(kvas))
	// TODO: 先这样处理
	if len(kvas) == 0 {
		key := fmt.Sprintf("mr-out-%d", task.Conf.RNum)
		_ = os.Rename(tmpFileName, key)
		return []string{key}, nil
	}
	buf := []KeyValue{kvas[0]}
	// TODO:处理边界
	// [a,a,a,a,a,b,b,b,b,c,c,c]
	for i := 1; i < len(kvas); i++ {
		if buf[len(buf)-1].Key == kvas[i].Key { // buf 中最后的一个key 与当前key 相同
			buf = append(buf, kvas[i])
		} else {
			out := reducef(buf[len(buf)-1].Key, toValues(buf))
			_, _ = fmt.Fprintf(outFile, "%v %v\n", buf[len(buf)-1].Key, out)
			buf = []KeyValue{kvas[i]}
		}
	}
	// 写入最后的buf进去
	out := reducef(buf[len(buf)-1].Key, toValues(buf))
	_, _ = fmt.Fprintf(outFile, "%v %v\n", buf[len(buf)-1].Key, out)
	//TODO:处理路径问题?
	key := fmt.Sprintf("mr-out-%d", task.Conf.RNum)
	_ = os.Rename(tmpFileName, key)
	return []string{key}, nil
}

func toValues(kvas []KeyValue) []string {
	res := make([]string, 0)
	for _, kv := range kvas {
		res = append(res, kv.Value)
	}
	return res
}
func readFiles(task *Task) []KeyValue {
	// TODO: 生产级别应该实现 外部排序并返回一个迭代器
	// TODO: 进行错误处理
	res := make([]KeyValue, 0)
	for _, v := range task.Conf.Source {
		file, _ := os.Open(v)
		br := bufio.NewReader(file)
		for {
			line, _, c := br.ReadLine()
			if c == io.EOF {
				break
			}
			data := strings.Split(string(line), " ")
			res = append(res, KeyValue{
				Key:   data[0],
				Value: data[1],
			})
		}
		_ = file.Close()
	}
	return res
}
func combine(intermediate []KeyValue) {

}
func sendTaskFail(task *Task) {
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
