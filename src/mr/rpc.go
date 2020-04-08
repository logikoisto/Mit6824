package mr

//
// RPC definitions.
//

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

// Add your RPC definitions here.
// 注册
type RegisterReq struct {
}

type RegisterRes struct {
	WorkerID uint64
}

// 获取任务
type GetTaskReq struct {
	WorkerID uint64
}
type GetTaskRes struct {
	Code int
	Msg  string
	T    *Task
}

// 返回结果
type ResultReq struct {
	WorkerID uint64
	Code     int // 0 代表 map 1 代表 reduce 2代表 失败
	Msg      string
	M        []string
}

type ResultRes struct {
	Code int
	Msg  string
}

// 健康检查
type Ping struct {
	WorkerID uint64
}

type Pong struct {
	Code int
}
