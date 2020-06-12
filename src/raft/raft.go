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
	"fmt"
	"math/rand"
	"strconv"
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
// TODO: 对raft 各种关键行为的操作 没有保证原子性
type Raft struct {
	// TODO: 这里可以该成读写锁 进一步优化
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
	roles         int32        // 标示当前对等点当前的角色 1 跟随者 2 候选人 3 领导者
	id            atomic.Value // 对等点唯一标示
	curVoteTarget atomic.Value // 当一次投票给node的ID
	myTerm        int64        // 最后的已知的任期
	// TODO: 是否存在 一个并发安全的 list?
	logs          []Wal       // 日志条目
	lastApplyIdx  int         // 最后应用于状态机的日志索引
	lastCommitIdx int         // 最后的提交日志索引
	electionTimer *time.Timer // 用于选举的定时器
}

func (rf *Raft) initLeader() {
	// TODO: 初始化 leader 相关数据状态
	rf.setCurVoteTarget(rf.getId())
}
func (rf *Raft) isLeader() bool {
	return atomic.CompareAndSwapInt32(&rf.roles, 3, 3)
}

// 只有从一个候选人才能变更为领导者
func (rf *Raft) coronation() bool {
	return atomic.CompareAndSwapInt32(&rf.roles, 2, 3)
}

func (rf *Raft) isFollower() bool {
	return atomic.LoadInt32(&rf.roles) == 1
}

func (rf *Raft) following() {
	atomic.StoreInt32(&rf.roles, 1)
}

func (rf *Raft) isCandidate() bool {
	return atomic.LoadInt32(&rf.roles) == 2
}

// 只能从 跟随者 变为候选人
func (rf *Raft) setCandidate() bool {
	return atomic.CompareAndSwapInt32(&rf.roles, 1, 2)
}

func (rf *Raft) getLastCommitIdx() int {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	return rf.lastCommitIdx
}

func (rf *Raft) setLogs(los []Wal) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.logs = los
}
func (rf *Raft) setMyTerm(term int64) {
	atomic.StoreInt64(&rf.myTerm, term)
}

func (rf *Raft) getCurVoteTarget() string {
	return rf.curVoteTarget.Load().(string)
}
func (rf *Raft) setCurVoteTarget(vote string) {
	rf.curVoteTarget.Store(vote)
}

func (rf *Raft) setLastCommitIdx(lastCommitIdx int) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.lastCommitIdx = lastCommitIdx
}

func (rf *Raft) getMyTerm() int64 {
	return atomic.LoadInt64(&rf.myTerm)
}
func (rf *Raft) incrMyTerm() int64 {
	return atomic.AddInt64(&rf.myTerm, 1)
}
func (rf *Raft) getMe() int {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	return rf.me
}

func (rf *Raft) getId() string {
	return rf.id.Load().(string)
}

func (rf *Raft) setId(id string) {
	rf.id.Store(id)
}

type Wal struct {
	term int64
	cmd  string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.getMyTerm()), rf.isLeader()
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
	CandidateTerm int64
	LastLogIdx    int
	LastLogTerm   int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurTerm int64
	IsVote  bool
}

//
// example RequestVote RPC handler.
//
// TODO: 这里可能存在 选票瓜分时 同时竞选成为领导的问题 草你妈的  这一周也没搞定 气死哎呀
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//curVoteTarget := rf.getCurVoteTarget()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.CurTerm = rf.getMyTerm()
	// 如果相同 那么说明同时超时 彼此肯定不投票 选票被瓜分
	if args.CandidateTerm <= reply.CurTerm {
		reply.IsVote = false
		return
	}
	var lastLog Wal
	var lastLogIdx int
	// TODO: 对获取最后的日志这里应该进行抽象
	if len(rf.logs) > 0 {
		lastLog = rf.logs[len(rf.logs)-1]
		lastLogIdx = len(rf.logs) - 1
	}
	//rf.mu.Unlock()
	if lastLog.term > args.CandidateTerm /*|| len(curVoteTarget) != 0*/ || lastLogIdx > args.LastLogIdx {
		reply.IsVote = false
		return
	}
	rf.setMyTerm(args.CandidateTerm)
	rf.setCurVoteTarget(args.CandidateID)
	rf.following()
	// 在投票后重新等待一个选举超时时间,也就是说 选票会抑制跟随者成为候选者,如果节点投票相当于放弃了最近一次的竞选
	fmt.Println(rf.getId(), "在任期", reply.CurTerm, "投票给", args.CandidateID, "后任期变为", rf.getMyTerm())
	rf.electionTimer.Reset(getElectionTimeOut())
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
	LeaderTerm int64
	PreLogIdx  int
	PreLogTerm int64
	LastCommit int
	Logs       []interface{}
}

//
// 附加日志/心跳 rpc 的回复
//
type AppendEntriesReply struct {
	// Your data here (2A).
	CurrTerm int64
	IsOK     bool
}

//
// 附加日志/心跳 rpc 的执行体
//
func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	myTerm := rf.getMyTerm()
	reply.CurrTerm = myTerm
	if args.LeaderTerm < myTerm {
		reply.IsOK = false
		return
	}
	if len(args.Logs) == 0 && args.LeaderTerm >= myTerm {
		// 如果 请求的任期更高 那么就更新自己认为的leader节点
		if args.LeaderTerm > myTerm {
			rf.following()
			rf.setCurVoteTarget(args.LeaderID)
			rf.setMyTerm(args.LeaderTerm)
		}
		reply.IsOK = true
		rf.electionTimer.Reset(getElectionTimeOut())
		return
	}
}

// 附加日志/心跳 rpc 的发送函数
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
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
	rf.setId(strconv.Itoa(me))            // TODO: 应该去除特殊字符
	rf.setCurVoteTarget(strconv.Itoa(me)) // TODO: 应先从持久化数据中恢复
	rf.logs = make([]Wal, 0)
	rf.myTerm = 1 // 初始化的时候 大家都认为自己是1,除非 被快照覆盖
	rf.following()
	go rf.election()
	go rf.heartbeat()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
func getElectionTimeOut() time.Duration {
	rand.Seed(time.Now().UnixNano())
	ts := time.Duration(300+(rand.Int63()%200)) * time.Millisecond
	return ts
}

func (rf *Raft) election() {
	rf.electionTimer = time.NewTimer(getElectionTimeOut())
	defer rf.electionTimer.Stop()
	for {
		select {
		case <-rf.electionTimer.C:
			if !rf.setCandidate() { //成为候选人
				continue
			}
			rf.incrMyTerm()
			rf.setCurVoteTarget("")
			myTerm := rf.getMyTerm()
			me := rf.me
			var lastLogIdx int
			var lastLogTerm int64
			peersLen := len(rf.peers)
			if len(rf.logs) > 0 {
				lastLogIdx, lastLogTerm = len(rf.logs)-1, rf.logs[len(rf.logs)-1].term
			}
			voteArgs, voteRes := RequestVoteArgs{
				CandidateID:   rf.getId(),
				CandidateTerm: myTerm,
				LastLogIdx:    lastLogIdx,
				LastLogTerm:   lastLogTerm,
			}, RequestVoteReply{}
			res := make([]bool, peersLen)
			res[me] = true // 候选人会投自己一票
			for i := 0; i < peersLen; i++ {
				if i == me {
					continue
				}
				if ok := rf.sendRequestVote(i, &voteArgs, &voteRes); ok {
					if voteRes.CurTerm > myTerm {
						// 如果 他认为的民众比他任期更高 那么他就回退到跟随者
						rf.setMyTerm(voteRes.CurTerm)
						rf.following()
						goto f
					}
					res[i] = voteRes.IsVote
				}
			}
			count := 0
			for _, v := range res {
				if v {
					count++
				}
			}
			if count >= (peersLen)/2+1 {
				for !rf.coronation() {
				} // CAS
				fmt.Println(rf.getId(), "在任期", rf.getMyTerm(), "成为领导者", "获得选票", res, "状态是", rf.roles)
				rf.initLeader()
			}
		case <-rf.closeChan:
			return
		}
	f:
		rf.electionTimer.Reset(getElectionTimeOut())
	}
}

// TODO:expected one leader, got none  无法在 5秒内选举出leader 明天 还是去看看论文吧  这个时候 我怀疑自己的理解有问题了 草 还是不能通过 哭唧唧
func (rf *Raft) heartbeat() {
	c := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-c.C:
			if rf.isLeader() {
				id := rf.getId()
				myTerm := rf.getMyTerm()
				peersLen := len(rf.peers)
				me := rf.me
				res := make([]bool, len(rf.peers))
				res[me] = true
				for i := 0; i < peersLen; i++ {
					if i == me {
						continue
					}
					reply := &AppendEntriesReply{}
					ret := rf.sendAppendEntries(i, &AppendEntriesArgs{
						LeaderID:   id,
						LeaderTerm: myTerm,
						Logs:       make([]interface{}, 0),
					}, reply)
					res[i] = ret
					if ret && reply.CurrTerm > myTerm {
						rf.setMyTerm(reply.CurrTerm)
						rf.following()
						rf.electionTimer.Reset(getElectionTimeOut())
						break
					}
				}
				count := 0
				for _, v := range res {
					if v {
						count++
					}
				}
				if count < (peersLen)/2+1 {
					rf.following()
					rf.electionTimer.Reset(getElectionTimeOut())
				}
			}
		case <-rf.closeChan:
			return
		}
	}
}
