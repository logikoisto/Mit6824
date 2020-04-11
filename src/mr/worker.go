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

/*
	TODO: debug:
		1. 消息通信的类型定义复杂 没有处理好 导致逻辑混乱调用
		2. reduce 任务无法输出全部数据 -> 更改了 状态矩阵的更新逻辑 导致的 偶然复杂度
		3. job 执行后无法正常关闭
				1. 资源释放不协调导致泄露
				2. 消息通信协议没有定义好 导致逻辑混乱
		4. reduce 输出内容 多个文件都是一样的

		现象: job 无法执行全部的r任务 而之前能够全部执行并输出
		期望: job 正常输出全部 结果文件
		问题: job 执行逻辑哪里出了问题?

		w没有获取到r任务
		sm 的状态是正确的
		假设 生成r任务时出现错误
				统计done时出现错误

		假设 m的提交是在一行上保持原子性  所以 m 正确提交只会是 mc 次
*/
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
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		for {
			<-timer.C
			PingPong()
		}
	}()
	var isRetry bool
	var task *Task
	for {
		// 1.获取任务
		if isRetry {
			task = ReTryTask()
		} else {
			task = GetTask()
		}
		// 2.根据任务类型执行任务
		// TODO: 是否应该用err 来返回 这样更优雅?
		res, err := ExecTask(mapf, reducef, task)
		if err != nil {
			isRetry = true
			continue
		}
		// 3.报告结果
		Report(res)
		// TODO 解析结果 在来判断是否需要 重新获取 task
		isRetry = false
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
	CallReTry    = "Master.ReTry"
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
func ReTryTask() *Task {
	args, reply := ReTryTaskReq{WorkerID: workerID}, ReTryTaskRes{}
	call(CallReTry, &args, &reply)
	//TODO:解析异常?
	return reply.T
}
func ExecTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, task *Task) (*ResultReq, error) {
	var res []string
	req := &ResultReq{WorkerID: workerID}
	if task.Type == 0 {
		// TODO: 执行 map 任务
		res, _ = doMap(mapf, task)
		req.Code = 0
	} else if task.Type == 1 {
		// TODO: 执行 reduce 任务
		res, _ = doReduce(reducef, task)
		req.Code = 1
	} else if task.Type == 2 {
		return nil, fmt.Errorf("retry") // TODO:应该定义一种错误类型
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
		key := fmt.Sprintf("mid-%d-%d.out", task.Conf.MNum, i)
		cacheMap[key] = []KeyValue{}
		res = append(res, key)
	}
	kva := mapf(fileName, string(content))
	for i := 0; i < len(kva); i++ {
		idx := ihash(kva[i].Key) % task.Conf.RC // TODO: ihash(kva[i].Key) & (task.Conf.RC - 1)
		key := fmt.Sprintf("mid-%d-%d.out", task.Conf.MNum, idx)
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
	// TODO: 检查 kvas
	buf := []KeyValue{kvas[0]}
	// TODO:处理边界
	for i := 1; i < len(kvas)-1; i++ {
		if buf[len(buf)-1].Key == kvas[i].Key {
			buf = append(buf, kvas[i])
		} else {
			out := reducef(buf[0].Key, toValues(buf))
			_, _ = fmt.Fprintf(outFile, "%v %v\n", buf[0].Key, out)
			buf = []KeyValue{kvas[i]}
		}
	}
	//TODO:处理路径问题?
	key := fmt.Sprintf("mr-out-%d", task.Conf.RNum)
	_ = os.Rename(tmpFileName, key)
	return []string{key}, nil
}

func toValues(kvas []KeyValue) (res []string) {
	for i := 0; i < len(kvas); i++ {
		res = append(res, kvas[i].Value)
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
