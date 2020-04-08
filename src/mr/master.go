package mr

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

// 主节点
type Master struct {
	// Your definitions here.
	S  *JobState
	TP *TaskPool
	W  map[uint64]*WorkerSession
}

// Job 状态
type JobState struct {
	MatrixSource [][]string // MC * RC
	MC           int
	RC           int
	DoneRC       int32
	nextWorkerID uint64
}

// 任务池
type TaskPool struct {
	Pool chan *Task
}

// 任务
type Task struct {
	Status int // 0 未完成 1工作中 2已完成
	Type   int // 0 map 任务 1 reduce 任务 2 shut down 3 retry
	Conf   *TaskConf
}

// 任务配置
type TaskConf struct {
	Source    string
	MatrixNum int
}

// 定时清理器
type Dispatcher struct {
	PollTs time.Duration //毫秒
	M      *Master       //主节点全局结构
}

// 工作者会话管理器
type WorkerSession struct {
	Status     int // 0 空闲状态 1 工作状态 2 无法正常工作
	T          *Task
	Mux        *sync.RWMutex
	LastPingTs int64
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) RegisterWorker(args *RegisterReq, reply *RegisterRes) error {
	_ = args
	for {
		assignID := atomic.LoadUint64(&m.S.nextWorkerID)
		if atomic.CompareAndSwapUint64(&m.S.nextWorkerID, assignID, assignID+1) {
			reply.WorkerID = assignID
			m.W[assignID] = &WorkerSession{
				Status: 0,   // 0 代表 健康良好 1 代表失联
				T:      nil, // 正在执行的任务
			}
			return nil
		}
		// TODO:不应该无限重试 应该设置一个限制
		time.Sleep(10 * time.Millisecond)
	}
}

func (m *Master) GetTaskWorker(args *GetTaskReq, reply *GetTaskRes) error {
	select {
	case task, ok := <-m.TP.Pool:
		if !ok {
			shutdown(reply)
			return nil
		}
		workerID := args.WorkerID
		if worker, ok := m.W[workerID]; ok {
			reply.T = task
			worker.Mux.Lock()
			defer worker.Mux.Unlock()
			worker.Status = 1 // 任务负载
			worker.T = task
		} else {
			m.TP.Pool <- task
			reply.Msg = "unregistered"
			reply.Code = 1
		}
	default:
		reTry(reply)
	}
	return nil
}

func (m *Master) ReportResult(args *ResultReq, reply *ResultRes) error {
	// TODO: 这是map 是否会有并发问题?
	if ws, ok := m.W[args.WorkerID]; ok {
		switch args.Code {
		case 0: // map
			for i, data := range args.M {
				m.S.MatrixSource[ws.T.Conf.MatrixNum][i] = data
			}
		case 1: // reduce
			// TODO: 这里存在 幂等性问题
			atomic.AddInt32(&(m.S.DoneRC), 1)
		case 2: // failed
			task := ws.T
			// TODO: 直接删除 会话 并发?
			delete(m.W, args.WorkerID)
			task.Status = 0   // 重新置为 未分配状态
			m.TP.Pool <- task // 将任务重新加入队列
			reply.Code = 0
			return nil
		default:
			reply.Code = 1
			reply.Msg = fmt.Sprintf("Code %d do not recognize", args.Code)
			return nil
		}
		ws.Mux.Lock()
		defer ws.Mux.Unlock()
		ws.Status = 0                               // 节点空闲
		ws.T = nil                                  // 任务完成
		ws.LastPingTs = time.Now().UnixNano() / 1e6 // 更新会话时间戳
		reply.Code = 0
		return nil
	}
	reply.Code = 1
	reply.Msg = "unregistered"
	return nil
}

func (m *Master) Health(args *Ping, reply *Pong) error {
	_ = args
	if ws, ok := m.W[args.WorkerID]; ok {
		ws.Mux.Lock()
		defer ws.Mux.Unlock()
		ws.LastPingTs = time.Now().UnixNano() / 1e6 // 更新会话时间戳
	}
	reply.Code = 0
	return nil
}

func shutdown(reply *GetTaskRes) {
	reply.Msg = "shut down!!!"
	reply.T = &Task{
		Status: 0,
		Type:   2,
		Conf:   &TaskConf{Source: ""},
	}
}
func reTry(reply *GetTaskRes) {
	reply.Msg = "reTry!!!"
	reply.T = &Task{
		Status: 0,
		Type:   2,
		Conf:   &TaskConf{Source: ""},
	}
}

func (d *Dispatcher) cleanSession() {
	// TODO: 对map的遍历是否存在并发问题?
	curTs := time.Now().UnixNano() / 1e6
	for workerID, worker := range d.M.W {
		if curTs-worker.LastPingTs > int64(10*time.Millisecond) {
			delete(d.M.W, workerID)
		}
	}
}

func (d *Dispatcher) updateJobState() {
	for i := 0; i < d.M.S.RC; i++ {
		count := 0
		for j := 0; j < d.M.S.MC; j++ {
			if len(d.M.S.MatrixSource[j][i]) != 0 {
				count++
			}
		}
		if count == d.M.S.RC {
			for j := 0; j < d.M.S.MC; j++ {
				d.M.TP.Pool <- &Task{
					Status: 0,
					Type:   1, // Reduce 任务
					Conf: &TaskConf{
						Source:    d.M.S.MatrixSource[j][i],
						MatrixNum: i,
					},
				}
			}
		}
	}
}
func (d *Dispatcher) run() {
	go func() {
		timer := time.NewTimer(d.PollTs)
		defer timer.Stop()
		for {
			<-timer.C
			d.updateJobState()
			d.cleanSession()
		}
	}()
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	if err := rpc.Register(m); err != nil {
		panic(err)
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	if err := os.Remove("mr-socket"); err != nil {
		panic(err)
	}
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		if err := http.Serve(l, nil); err != nil {
			panic(err)
		}
	}()
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	doneReduceCount := atomic.LoadInt32(&(m.S.DoneRC))
	if int(doneReduceCount) == m.S.RC {
		ret = true
	}
	return ret
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.server()
	// Your code here.
	m.S = &JobState{
		MatrixSource: make([][]string, len(files), nReduce),
		MC:           len(files),
		RC:           nReduce,
		nextWorkerID: uint64(0),
	}
	m.TP = &TaskPool{Pool: make(chan *Task, len(files))}
	m.W = make(map[uint64]*WorkerSession)

	dispatcher := &Dispatcher{
		PollTs: 1 * time.Second,
		M:      &m,
	}
	dispatcher.run()
	// 初始化map任务
	for num, file := range files {
		m.TP.Pool <- &Task{
			Status: 0, // 0 未完成 1工作中 2已完成
			Type:   0, // 0 map 任务 1 reduce 任务 2 shut down 3 retry
			Conf:   &TaskConf{Source: file, MatrixNum: num},
		}
	}
	return &m
}
