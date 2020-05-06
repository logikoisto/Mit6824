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

var dispatcher *Dispatcher

// 主节点
type Master struct {
	// Your definitions here.
	S  *JobState
	TP *TaskPool
	W  *sync.Map
}

// Job 状态
type JobState struct {
	MatrixSource [][]string // MC * RC
	MC           int
	RC           int
	MCDone       int32
	nextWorkerID uint64
	allDone      int // 用于标示全部完成的状态 0代表没有全部完成 1 代表已经全部完成 进入优雅关闭状态
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
	TimeOut          time.Duration      //默认10秒
	M                *Master            //主节点全局结构
	ReduceSourceChan chan *ReduceSource // 发送 reduce 的任务 执行内容
	CleanWorkerChan  chan uint64        // 清理失效的worker
}

// 工作者会话管理器
type WorkerSession struct {
	WorkerID     uint64
	Status       int // 0 空闲状态 1 工作状态 2 无法正常工作
	T            *Task
	Mux          *sync.RWMutex
	LastPingTs   int64
	PingPongChan chan struct{}
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
				WorkerID:     assignID,
				Status:       0,   // 0 代表 健康良好 1 代表失联
				T:            nil, // 正在执行的任务
				LastPingTs:   time.Now().UnixNano() / 1e6,
				Mux:          &sync.RWMutex{},
				PingPongChan: make(chan struct{}),
			}
			m.W.Store(assignID, ws)
			go ws.PingPong(dispatcher.TimeOut)
			return nil
		}
		// TODO:不应该无限重试 应该设置一个限制
		time.Sleep(10 * time.Millisecond)
	}
}

func (m *Master) GetTaskWorker(args *GetTaskReq, reply *GetTaskRes) error {
	// 延迟 5秒后 若五任务就返回
	c := time.After(5 * time.Second)
	if worker, ok := m.W.Load(args.WorkerID); ok {
		w := worker.(*WorkerSession)
		select {
		case task, ok := <-m.TP.Pool:
			if !ok {
				shutdown(reply)
				m.W.Delete(w.WorkerID)
				return nil
			}
			task.Status = 1
			reply.T = task
			w.Mux.Lock()
			defer w.Mux.Unlock()
			w.Status = 1 // 任务负载
			w.T = task
		case <-c:
			// 返回nil
		}
	} else {
		shutdown(reply)
		m.W.Delete(args.WorkerID)
	}
	return nil
}

func (m *Master) ReportResult(args *ResultReq, reply *ResultRes) error {
	//fmt.Println("ReportResult", args.WorkerID, args.M, args.Code)
	if len(args.M) == 0 {
		reply.Code = 1
		reply.Msg = "The report cannot be empty"
		return nil
	}
	if ws, ok := m.W.Load(args.WorkerID); ok {
		w := ws.(*WorkerSession)
		switch args.Code {
		case 0: // map
			if w.T == nil {
				reply.Msg = "shut down!!!"
				reply.Code = 1
				return nil
			}
			dispatcher.ReduceSourceChan <- &ReduceSource{
				MIdx:      w.T.Conf.MNum,
				MapSource: args.M,
			}
		case 1: // reduce
			if w.T == nil {
				reply.Msg = "shut down!!!"
				reply.Code = 1
				return nil
			}
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

func (m *Master) PingPong(args *Ping, reply *Pong) error {
	if ws, ok := m.W.Load(args.WorkerID); ok {
		w := ws.(*WorkerSession)
		w.Mux.Lock()
		defer w.Mux.Unlock()
		w.LastPingTs = time.Now().UnixNano() / 1e6 // 更新会话时间戳
		w.PingPongChan <- struct{}{}
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

func (d *Dispatcher) cleanSession() {
	for workerID := range d.CleanWorkerChan {
		if w, ok := d.M.W.Load(workerID); ok {
			worker := w.(*WorkerSession)
			worker.Mux.Lock()
			task := worker.T
			worker.T = nil
			worker.Mux.Unlock()
			if task != nil {
				task.Status = 0
				//fmt.Println("cleanSession.task",workerID,task.Status,task.Conf.Source)
				d.M.TP.Pool <- task
			}
			d.M.W.Delete(worker)
			//fmt.Println("cleanSession.worker",workerID)
		}
	}
}

func (d *Dispatcher) updateJobState() {
	for rs := range d.ReduceSourceChan {
		d.M.S.MatrixSource[rs.MIdx] = rs.MapSource
		atomic.AddInt32(&d.M.S.MCDone, 1)
		if atomic.LoadInt32(&d.M.S.MCDone) == int32(d.M.S.MC) {
			//fmt.Println(d.M.S.MCDone)
			for j := 0; j < d.M.S.RC; j++ {
				sources := make([]string, 0)
				for i := 0; i < d.M.S.MC; i++ {
					sources = append(sources, d.M.S.MatrixSource[i][j])
				}
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
				//fmt.Println(sources, d.M.S.MatrixSource[d.M.S.MC][j])
			}
		}
	}
}

func (d *Dispatcher) run() {
	go d.cleanSession()
	go d.updateJobState()
}

func (w *WorkerSession) PingPong(ts time.Duration) {
	for {
		tc := time.NewTicker(ts)
		select {
		case _ = <-tc.C:
			dispatcher.CleanWorkerChan <- w.WorkerID
		case _ = <-w.PingPongChan:
			tc.Stop()
			// TODO: 这里应该 有一个 close 信号将协程退出 否则程序中会存在大量无用的协程 存在泄露的风险
		}
	}
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
		//fmt.Println(m.S.allDone)
		//close(dispatcher.CleanWorkerChan)
		//close(dispatcher.ReduceSourceChan)
		if len(m.TP.Pool) != 0 {
			return false
		}
		if m.S.allDone == 0 {
			close(m.TP.Pool) // 将会通知所有 worker 进行下线
			m.S.allDone = 1
		}
		c := 0
		m.W.Range(func(key, value interface{}) bool {
			w := value.(*WorkerSession)
			if w.T != nil {
				c++
			}
			return true
		})
		if c == 0 {
			ret = true
			// TODO: 一个完美主义者 不想让命令行打印出来一些无关紧要的东西(又不想改框架本身)
			_ = os.Remove("mr-socket")
			_, _ = os.Create("mr-socket")
		}
	}
	return ret
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
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

	dispatcher = &Dispatcher{
		TimeOut:          10 * time.Second,
		M:                &m,
		ReduceSourceChan: make(chan *ReduceSource, nReduce),
		CleanWorkerChan:  make(chan uint64, len(files)),
	}
	dispatcher.run()
	// 初始化map任务
	for num, file := range files {
		m.TP.Pool <- &Task{
			Status: 0, // 0 未完成 1工作中 2已完成
			Type:   0, // 0 map 任务 1 reduce 任务 2 shut down 3 retry
			Conf:   &TaskConf{Source: []string{file}, MNum: num, RNum: -1, RC: nReduce},
		}
	}
	m.server()
	return &m
}
