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

/*
debug:
	1. desc: map 的并发写入报错 fix: 改成 sync.map 结构
	2. desc: map 任务执行完 并不上报 完成状态 给master
*/

// 主节点
type Master struct {
	// Your definitions here.
	S  *JobState
	TP *TaskPool
	W  *sync.Map
	D  *Dispatcher
}

// Job 状态
type JobState struct {
	MatrixSource [][]string // MC * RC
	MC           int
	RC           int
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
	Source []string // 兼容两种任务
	RNum   int      // 当前 map 任务的 任务编号 如果是reduce任务 则为-1
	MNum   int      // 当前 reduce 任务的 任务编号 如果是map任务 则为-1
	RC     int      // reduce 的任务数
}

// 定时清理器
type Dispatcher struct {
	PollTs           time.Duration      //毫秒
	M                *Master            //主节点全局结构
	ReduceSourceChan chan *ReduceSource // 发送 reduce 的任务 执行内容
}

// 工作者会话管理器
type WorkerSession struct {
	Status     int // 0 空闲状态 1 工作状态 2 无法正常工作
	T          *Task
	Mux        *sync.RWMutex
	LastPingTs int64
}

type ReduceSource struct {
	MIdx      int
	MapSource []string // map 任务返回的 source 列表
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
			ws := &WorkerSession{
				Status:     0,   // 0 代表 健康良好 1 代表失联
				T:          nil, // 正在执行的任务
				LastPingTs: time.Now().UnixNano() / 1e6,
				Mux:        &sync.RWMutex{},
			}
			m.W.Store(assignID, ws)
			return nil
		}
		// TODO:不应该无限重试 应该设置一个限制
		time.Sleep(10 * time.Millisecond)
	}
}

func (m *Master) GetTaskWorker(args *GetTaskReq, reply *GetTaskRes) error {
	// TODO: 应该先判断是否合法再去拿任务
	// 延迟 5秒后 若五任务就返回
	c := time.After(5 * time.Second)
	select {
	case task, ok := <-m.TP.Pool:
		if !ok {
			shutdown(reply)
			return nil
		}
		workerID := args.WorkerID
		if worker, ok := m.W.Load(workerID); ok {
			w := worker.(*WorkerSession)
			reply.T = task
			w.Mux.Lock()
			defer w.Mux.Unlock()
			w.Status = 1 // 任务负载
			w.T = task
		} else {
			m.TP.Pool <- task
			reply.Msg = "unregistered"
			reply.Code = 1
		}
	case <-c:
		reTry(reply)
	}
	return nil
}

func (m *Master) ReportResult(args *ResultReq, reply *ResultRes) error {
	fmt.Println("ReportResult", args.WorkerID, args.M, args.Code)
	if len(args.M) == 0 {
		reply.Code = 1
		reply.Msg = "The report cannot be empty"
		return nil
	}
	if ws, ok := m.W.Load(args.WorkerID); ok {
		w := ws.(*WorkerSession)
		switch args.Code {
		case 0: // map
			m.D.ReduceSourceChan <- &ReduceSource{
				MIdx:      w.T.Conf.MNum,
				MapSource: args.M,
			}
		case 1: // reduce
			m.S.MatrixSource[m.S.MC][w.T.Conf.RNum] = "done"
		case 2: // failed
			task := w.T
			m.W.Delete(args.WorkerID)
			task.Status = 0   // 重新置为 未分配状态
			m.TP.Pool <- task // 将任务重新加入队列
			reply.Code = 0
			return nil
		default:
			reply.Code = 1
			reply.Msg = fmt.Sprintf("Code %d do not recognize", args.Code)
			return nil
		}
		w.Mux.Lock()
		defer w.Mux.Unlock()
		w.Status = 0                               // 节点空闲
		w.T = nil                                  // 任务完成
		w.LastPingTs = time.Now().UnixNano() / 1e6 // 更新会话时间戳
		reply.Code = 0
		return nil
	}
	reply.Code = 1
	reply.Msg = "unregistered"
	return nil
}

func (m *Master) ReTry(args *ReTryTaskReq, reply *ReTryTaskRes) {
	if worker, ok := m.W.Load(args.WorkerID); ok {
		w := worker.(*WorkerSession)
		reply.T = w.T
		reply.Code = 0
		w.Mux.Lock()
		defer w.Mux.Unlock()
	}
}

func (m *Master) Health(args *Ping, reply *Pong) error {
	_ = args
	if ws, ok := m.W.Load(args.WorkerID); ok {
		w := ws.(*WorkerSession)
		w.Mux.Lock()
		defer w.Mux.Unlock()
		w.LastPingTs = time.Now().UnixNano() / 1e6 // 更新会话时间戳
	}
	reply.Code = 0
	return nil
}

func shutdown(reply *GetTaskRes) {
	reply.Msg = "shut down!!!"
	reply.T = &Task{
		Status: 0,
		Type:   2,
		Conf:   &TaskConf{Source: []string{}},
	}
}
func reTry(reply *GetTaskRes) {
	reply.Msg = "reTry!!!"
	reply.T = &Task{
		Status: 0,
		Type:   2,
		Conf:   &TaskConf{Source: []string{}},
	}
}

func (d *Dispatcher) cleanSession() {
	curTs := time.Now().UnixNano() / 1e6
	d.M.W.Range(func(k, v interface{}) bool {
		worker, workerID := v.(*WorkerSession), k.(uint64)
		if curTs-worker.LastPingTs > int64(10*time.Millisecond) {
			d.M.W.Delete(workerID)
		}
		return true
	})
}

func (d *Dispatcher) updateJobState() {
	for rs := range d.ReduceSourceChan {
		for i, source := range rs.MapSource {
			d.M.S.MatrixSource[rs.MIdx][i] = source
		}
		sources := make([]string, 0)
		// TODO 这里会有重复计算问题
		for j := 0; j < d.M.S.RC; j++ {
			for i := 0; i < d.M.S.MC; i++ {
				if len(d.M.S.MatrixSource[i][j]) != 0 {
					sources = append(sources, d.M.S.MatrixSource[i][j])
				}
			}
			if len(d.M.S.MatrixSource[d.M.S.MC][j]) == 0 && len(sources) == d.M.S.MC {
				d.M.TP.Pool <- &Task{
					Status: 0,
					Type:   1, // Reduce 任务
					Conf: &TaskConf{
						Source: sources,
						RNum:   j,
						MNum:   -1,
						RC:     d.M.S.RC,
					},
				}
				d.M.S.MatrixSource[d.M.S.MC][j] = "created"
			}
		}
	}
}

func (d *Dispatcher) run() {
	go func() {
		timer := time.NewTicker(d.PollTs)
		defer timer.Stop()
		for {
			// TODO:这里存在资源泄露的问题
			<-timer.C
			d.cleanSession()
		}
	}()

	go d.updateJobState()
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
	_ = os.Remove("mr-socket")
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
	count := 0
	for _, v := range m.S.MatrixSource[m.S.MC] {
		if v == "done" {
			count++
		}
	}
	if count == m.S.RC {
		ret = true
		close(m.D.ReduceSourceChan)
		close(m.TP.Pool) // 将会通知所有 worker 进行下线
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
	sources := make([][]string, len(files)+1) // 多出一行保存完成状态
	for i := 0; i < len(sources); i++ {
		sources[i] = make([]string, nReduce)
	}
	m.S = &JobState{
		MatrixSource: sources,
		MC:           len(files),
		RC:           nReduce,
		nextWorkerID: uint64(0),
	}
	m.TP = &TaskPool{Pool: make(chan *Task, len(files))}
	m.W = &sync.Map{}

	dispatcher := &Dispatcher{
		PollTs:           1 * time.Second,
		M:                &m,
		ReduceSourceChan: make(chan *ReduceSource, nReduce),
	}
	dispatcher.run()
	m.D = dispatcher // 将 master 与 dispatcher 进行相互关联以便于传递新秀
	// 初始化map任务
	for num, file := range files {
		m.TP.Pool <- &Task{
			Status: 0, // 0 未完成 1工作中 2已完成
			Type:   0, // 0 map 任务 1 reduce 任务 2 shut down 3 retry
			Conf:   &TaskConf{Source: []string{file}, MNum: num, RNum: -1, RC: nReduce},
		}
	}
	return &m
}
