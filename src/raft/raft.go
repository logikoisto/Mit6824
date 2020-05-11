package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"github.com/google/uuid"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "Mit6824/src/labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	closeChan chan struct{}       // 关闭信号
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 唯一 持久化的配置
	isLeader      int32       // 标示当前对等点 是否为leader节点
	id            string      // 对等点唯一标示
	curVoteTarget string      // 当一次投票给node的ID
	myTerm        uint64      // 最后的已知的任期
	logs          []wal       // 日志条目
	lastCommitIdx int         // 最后的提交日志索引
	electionTimer *time.Timer // 用于选举的定时器
}

func (rf *Raft) getLastCommitIdx() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastCommitIdx
}

func (rf *Raft) setLogs(los []wal) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logs = los
}
func (rf *Raft) setMyTerm(term uint64) {
	atomic.StoreUint64(&rf.myTerm, term)
}

func (rf *Raft) setIsLeader(isLeader int32) {
	atomic.StoreInt32(&rf.isLeader, isLeader)
}
func (rf *Raft) setCurVoteTarget(vote string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.curVoteTarget = vote
}

func (rf *Raft) setLastCommitIdx(lastCommitIdx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastCommitIdx = lastCommitIdx
}

// TODO: 是否存在 一个并发安全的 list?
func (rf *Raft) getLogs() []wal {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.logs
}
func (rf *Raft) getMyTerm() uint64 {
	return atomic.LoadUint64(&rf.myTerm)
}
func (rf *Raft) getMe() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.me
}
func (rf *Raft) getIsLeader() int32 {
	return atomic.LoadInt32(&rf.isLeader)
}
func (rf *Raft) getId() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.id
}
func (rf *Raft) getCurVoteTarget() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.curVoteTarget
}

type wal struct {
	term uint64
	cmd  string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateID   string
	CandidateTerm uint64
	LastLogIdx    int
	LastLogTerm   uint64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurTerm uint64
	IsVote  bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	myTerm := rf.myTerm
	curVoteTarget := rf.curVoteTarget
	lastLog := rf.logs[len(rf.logs)-1]
	lastLogIdx := len(rf.logs) - 1
	reply.CurTerm = myTerm
	rf.mu.Unlock()
	if args.CandidateTerm < myTerm || len(curVoteTarget) != 0 ||
		lastLog.term > args.CandidateTerm || lastLogIdx > args.LastLogIdx {
		reply.IsVote = false
		return
	}
	rf.setCurVoteTarget(args.CandidateID)
	reply.IsVote = true
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// 附加日志/心跳rpc 请求
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	LeaderID   string
	LeaderTerm uint64
	PreLogIdx  int
	PreLogTerm int64
	LastCommit int
	Logs       []wal
}

//
// 附加日志/心跳 rpc 的回复
//
type AppendEntriesReply struct {
	// Your data here (2A).
	CurrTerm uint64
	IsOK     bool
}

//
// 附加日志/心跳 rpc 的执行体
//
func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	myTerm := rf.getMyTerm()
	if args.LeaderTerm < myTerm {
		reply.IsOK = false
		reply.CurrTerm = myTerm
		return
	}
	if len(args.Logs) == 0 {
		// 处理心跳
		reply.IsOK = false
		reply.CurrTerm = myTerm
		// 重置 选举超时计时器
		rf.electionTimer.Reset(getElectionTimeOut())
		return
	}
}

// 附加日志/心跳 rpc 的发送函数
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.closeChan = make(chan struct{})
	// Your initialization code here (2A, 2B, 2C).
	rf.id = uuid.New().String() // TODO: 应该去除特殊字符
	rf.logs = make([]wal, 0)
	rf.myTerm = 1 // 初始化的时候 大家都认为自己是1,除非 被快照覆盖
	go rf.election()
	go rf.heartbeat()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
func getElectionTimeOut() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(500)+500) * time.Millisecond
}
func (rf *Raft) election() {
	rf.electionTimer = time.NewTimer(getElectionTimeOut())
	defer rf.electionTimer.Stop()
	for {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.myTerm++
			rf.curVoteTarget = ""
			voteArgs, voteRes := RequestVoteArgs{
				CandidateID:   rf.id,
				CandidateTerm: rf.myTerm,
				LastLogIdx:    len(rf.logs) - 1,
				LastLogTerm:   rf.logs[len(rf.logs)-1].term,
			}, RequestVoteReply{}
			me := rf.me
			myTerm := rf.myTerm
			peersLen := len(rf.peers)
			rf.mu.Unlock()
			for i := 0; i < peersLen; i++ {
				if i == me {
					continue
				}
				if ok := rf.sendRequestVote(i, &voteArgs, &voteRes); ok {
					if voteRes.CurTerm > myTerm {
						rf.setMyTerm(voteRes.CurTerm)
						continue
					}
					if voteRes.IsVote {
						// TODO: 角色变更 这里需要重构一下
						rf.setIsLeader(1)
					}
				}
			}
		case <-rf.closeChan:
			return
		}
		rf.electionTimer.Reset(getElectionTimeOut())
	}
}

func (rf *Raft) heartbeat() {
	c := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-c.C:
			rf.mu.Lock()
			id := rf.id
			myTerm := rf.myTerm
			peersLen := len(rf.peers)
			rf.mu.Unlock()
			for i := 0; i < peersLen; i++ {
				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(i, &AppendEntriesArgs{
					LeaderID:   id,
					LeaderTerm: myTerm,
				}, reply)
			}
		case <-rf.closeChan:
			return
		}
	}
}
