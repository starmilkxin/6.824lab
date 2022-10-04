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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// 枚举节点的类型：跟随者、竞选者、领导者
const (
	Follower int = iota
	Candidate
	Leader
)

// AppendEntriesArgs 由leader复制log条目，也可以当做是心跳连接，注释中的rf为leader节点
type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader自身的ID
	PrevLogIndex int        // 预计要从哪里追加的index，因此每次要比当前的len(logs)多1 args初始化为：rf.nextIndex[i] - 1
	PrevLogTerm  int        // 追加新的日志的任期号(这边传的应该都是当前leader的任期号 args初始化为：rf.currentTerm
	Entries      []LogEntry // 预计存储的日志（为空时就是心跳连接）
	LeaderCommit int        // leader的commit index指的是最后一个被大多数机器都复制的日志Index
}

type AppendEntriesReply struct {
	Term        int  // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success     bool //	如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	UpNextIndex int  // 更新的下一个日志索引
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNums *int) {
	if rf.killed() {
		return
	}
	for {
		if rf.killed() {
			return
		}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		time.Sleep(10 * time.Millisecond)
		//fmt.Println("call server:", server)
		if ok {
			break
		}
	}
	//fmt.Println("-------------------------------------")
	//fmt.Printf("server:[%v] => peer:[%v] \n args:[%+v], \n reply:[%+v]\n", rf.me, server, args, reply)
	//fmt.Println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Success {
		rf.nextIndex[server] = reply.UpNextIndex
		fmt.Printf("reply.Success peer[%+v] -> leader[%+v] \n", server, rf.me)
		if *appendNums <= len(rf.peers)/2 {
			*appendNums = *appendNums + 1
			//fmt.Printf("server:[%v] appendNums:[%v], len(rf.peers)/2:[%v], len(rf.peers):[%v]\n", rf.me, *appendNums, len(rf.peers)/2, len(rf.peers))
		}
		if *appendNums > len(rf.peers)/2 {
			// 保证幂等性，不会提交第二次
			*appendNums = 0
			if len(rf.logs) == 0 || rf.logs[len(rf.logs)-1].Term != rf.currentTerm {
				return
			}

			for rf.lastApplied < reply.UpNextIndex-1 {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied,
				}
				rf.applyChan <- applyMsg
				rf.commitIndex = rf.lastApplied
				fmt.Printf("leader:[%v] apply ok! logs' max index is [%v] rf.commitIndex is [%v] applyMsg is [%+v] \n", rf.me, len(rf.logs)-1, rf.commitIndex, applyMsg)
			}
		}
		return
	} else {
		if reply.Term == -1 {
			return
		}
		// 出现网络分区，自己过时
		if reply.Term > rf.currentTerm {
			// 该节点变成追随者,并重置rf状态
			//fmt.Printf("server:[%v], 出现网络分区 args:[%+v], reply:[%+v]\n", rf.me, args, reply)
			rf.status = Follower
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.timer.Reset(rf.GetOverTime())
			rf.persist()
		} else {
			rf.timer.Reset(rf.GetHeartBeatTime())
			//fmt.Printf("server:[%v], server.logs:[%v] peer:[%v], 出现peer的index不一致情况\nargs:[%+v]\nreply:[%+v]\n", rf.me, rf.logs, server, args, reply)
			rf.nextIndex[server] = reply.UpNextIndex
			//fmt.Printf("rf.nextIndex[%v] 被改成了 [%v]\n", server, reply.UpNextIndex)
		}
		return
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		//fmt.Printf("server:[%v] was killed, so appendEntriesReply false \n", rf.me)
		reply.Term = -1
		reply.Success = false
		reply.UpNextIndex = rf.commitIndex + 1
		return
	}

	// 出现网络分区，leader过时
	if args.Term < rf.currentTerm {
		//fmt.Printf("server:[%v] => peer[%v] 发生了出现网络分区，leader过时, args.Term:[%v] < rf.currentTerm:[%v] \n", args.LeaderId, rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.commitIndex + 1
		return
	}
	// 出现网络分区，peer过时
	if args.Term > rf.currentTerm {
		//fmt.Printf("server:[%v] => peer[%v] 发生了出现网络分区，peer过时, args.Term:[%v] > rf.currentTerm:[%v] \n", args.LeaderId, rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.commitIndex = rf.lastApplied
		rf.logs = rf.logs[:rf.commitIndex+1]
		// 这个bug调了半天，只要不是网络分区leader过时这种致命错误，peer就应该继续成为Follower并重置过期时间，而不是争夺现有的leader
		rf.status = Follower
		rf.timer.Reset(rf.GetOverTime())
		rf.persist()
		//fmt.Printf("出现网络分区，peer[%+v]过时,rf.commitIndex: %+v, reply: %+v \n", rf.me, rf.commitIndex, reply)
		reply.Success = false
		reply.UpNextIndex = rf.commitIndex + 1
		return
	}
	reply.Term = rf.currentTerm
	if args.PrevLogIndex != rf.commitIndex {
		reply.Success = false
		reply.UpNextIndex = rf.commitIndex + 1
		//fmt.Printf("server:[%v] => peer[%v] 发生了 args.PrevLogIndex:[%v] != rf.commitIndex:[%v] \n", args.LeaderId, rf.me, args.PrevLogIndex, rf.commitIndex)
		// 这个bug调了半天，只要不是网络分区leader过时这种致命错误，peer就应该继续成为Follower并重置过期时间，而不是争夺现有的leader
		rf.status = Follower
		rf.timer.Reset(rf.GetOverTime())
		return
	}
	if rf.logs[rf.commitIndex].Term != args.PrevLogTerm {
		reply.Success = false
		rf.commitIndex--
		if rf.lastApplied > rf.commitIndex {
			rf.lastApplied--
		}
		rf.logs = rf.logs[:rf.commitIndex+1]
		reply.UpNextIndex = rf.commitIndex + 1
		//fmt.Printf("server:[%v] => peer[%v] 发生了 rf.logs[%v].Term:[%v] != args.PrevLogTerm:[%v] \n", args.LeaderId, rf.me, rf.commitIndex, rf.logs[rf.commitIndex].Term, args.PrevLogTerm)
		// 这个bug调了半天，只要不是网络分区leader过时这种致命错误，peer就应该继续成为Follower并重置过期时间，而不是争夺现有的leader
		rf.status = Follower
		rf.timer.Reset(rf.GetOverTime())
		rf.persist()
		return
	}
	// 对当前的rf进行ticker重置
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.status = Follower
	rf.timer.Reset(rf.GetOverTime())
	rf.persist()
	// 如果存在日志包那么进行追加
	if args.Entries != nil {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
		rf.commitIndex = len(rf.logs) - 1
		rf.persist()
	}
	// 对返回的reply进行赋值
	reply.Term = rf.currentTerm
	reply.Success = true
	reply.UpNextIndex = rf.commitIndex + 1

	// 将日志提交至与Leader相同
	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.logs[rf.lastApplied].Command,
		}
		rf.applyChan <- applyMsg
		fmt.Printf("leader:[%v] peer:[%v] apply ok! logs' max index is [%v] rf.commitIndex is [%v] applyMsg is [%+v] \n", args.LeaderId, rf.me, len(rf.logs)-1, rf.commitIndex, applyMsg)
	}

	return
}

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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int        // 记录当前的任期
	votedFor    int        // 记录当前的任期把票投给了谁
	logs        []LogEntry // 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号

	// Volatile state on all servers:
	commitIndex int // 状态机中已知要被提交的日志条目的索引值(初始化为0，持续递增）
	lastApplied int // 最后一个被追加到状态机日志的索引值

	// Volatile state on leaders:
	nextIndex  []int // 对于每个服务器，要发送到该服务器的下一个日志条目的索引(初始化为leader last log index + 1)
	matchIndex []int // 对于每个服务器，已知在服务器上复制的最高日志条目的索引(初始化为0，单调增加)

	applyChan chan ApplyMsg // 日志都是存在这里client取（2B）

	status   int           // 该节点是什么角色
	overtime time.Duration // 设置超时时间，200-400ms
	timer    *time.Ticker  // 每个节点中的计时器
}

func (rf *Raft) GetOverTime() time.Duration {
	return time.Duration(150+rand.Intn(200)) * time.Millisecond // 随机产生200-350ms
}

func (rf *Raft) GetHeartBeatTime() time.Duration {
	return time.Duration(120) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = false
	if rf.status == Leader {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	//fmt.Println("start persist")
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	//fmt.Println("start readPersist")
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		//fmt.Printf("RaftNode[%d] persist read, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, currentTerm, votedFor, logs)
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int //	需要竞选的人的任期
	CandidateId  int // 需要竞选的人的Id
	LastLogIndex int // 竞选人日志条目最后索引
	LastLogTerm  int // 候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // 投票方的term
	VoteGranted bool // 是否投票给了该竞选人
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 考虑term大小问题
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("vote peer:[%+v] peer.term:[%+v] args:[%+v] \n", rf.me, rf.currentTerm, args)
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	// 竞选者超时
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 自身超时
	if args.Term > rf.currentTerm {
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// 此轮term未曾投票
	if rf.votedFor == -1 {
		// 候选人的日志与接收者的日志不一致
		//if args.LastLogIndex < rf.commitIndex || args.LastLogTerm < rf.logs[rf.commitIndex].Term {
		//	reply.Term = rf.currentTerm
		//	reply.VoteGranted = false
		//	return
		//}
		if args.LastLogTerm < rf.logs[rf.commitIndex].Term || (args.LastLogTerm == rf.logs[rf.commitIndex].Term && args.LastLogIndex < rf.commitIndex) {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.timer.Reset(rf.GetOverTime())
	} else { // 此轮term已投过票
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNums *int) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		// 失败重传
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 自身超时
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.persist()
		*voteNums = 0
		rf.timer.Reset(rf.GetOverTime())
		return ok
	}
	if rf.status == Candidate && reply.VoteGranted && reply.Term == rf.currentTerm {
		(*voteNums)++
		fmt.Printf("peer:%+v -> server:%v voteNums:%v\n", server, rf.me, *voteNums)
	}
	// 晋升为leader
	if *voteNums > (len(rf.peers) / 2) {
		*voteNums = 0
		if rf.status == Leader {
			return ok
		}
		rf.status = Leader
		fmt.Printf("[%v] become the leader\n", rf.me)
		rf.nextIndex = make([]int, len(rf.peers))
		for i, _ := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.logs)
		}
		// 重置为心跳时间
		rf.timer.Reset(rf.GetHeartBeatTime())
	}

	return ok
}

//
//the service using Raft (e.g. a k/v server) wants to start
//agreement on the next command to be appended to Raft's log. if this
//server isn't the leader, returns false. otherwise start the
//agreement and return immediately. there is no guarantee that this
//command will ever be committed to the Raft log, since the leader
//may fail or lose an election. even if the Raft instance has been killed,
//this function should return gracefully.
//
//the first return value is the index that the command will appear at
//if it's ever committed. the second return value is the current
//term. the third return value is true if this server believes it is
//the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	if rf.killed() {
		return index, term, isLeader
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = len(rf.logs) - 1
	term = rf.currentTerm
	if rf.status != Leader {
		return index, term, isLeader
	}
	isLeader = true

	// 初始化日志条目。并进行追加
	appendLog := LogEntry{Term: rf.currentTerm, Command: command}
	rf.logs = append(rf.logs, appendLog)
	rf.persist()
	index = len(rf.logs) - 1
	fmt.Printf("start:[%v] command:[%v] logs.length:[%v]\n", rf.me, command, len(rf.logs))
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.applyChan = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))
	rf.status = Follower
	rf.overtime = rf.GetOverTime()
	rf.timer = time.NewTicker(rf.overtime)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	go rf.ticker()
	return rf
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.status {
			case Follower:
				// 当follwer时间过期后，会成为竞选者
				rf.status = Candidate
				fallthrough
			case Candidate:
				// 初始化自身的任期、并把票投给自己
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.persist()
				votedNums := 1 // 统计自身的票数
				// 每轮选举开始时，重新设置选举超时
				rf.overtime = rf.GetOverTime()
				rf.timer.Reset(rf.overtime)

				// 对其他结点请求投票
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					voteArgs := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: rf.commitIndex,
						LastLogTerm:  rf.logs[rf.commitIndex].Term,
					}
					go rf.sendRequestVote(i, &voteArgs, &RequestVoteReply{}, &votedNums)
				}
			case Leader:
				// 发送日志包，进行同步/心跳
				// 对于正确返回的节点数量
				appendNums := 1
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: rf.commitIndex, // commitIndex为大多数log所认可的commitIndex
					}
					//fmt.Printf("rf.nextIndex[%+v]: %+v \n", i, rf.nextIndex[i])
					if args.PrevLogIndex > len(rf.logs)-1 {
						//fmt.Printf("args.PrevLogIndex %+v -> %+v \n", args.PrevLogIndex, len(rf.logs)-1)
						args.PrevLogIndex = len(rf.logs) - 1
					}
					//fmt.Printf("args.PrevLogIndex: %+v, len(rf.logs): %+v \n", args.PrevLogIndex, len(rf.logs))
					args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
					//fmt.Printf("peer:[%v] 的 nextIndex:[%v]\n", i, rf.nextIndex[i])
					if rf.nextIndex[i] < len(rf.logs) {
						args.Entries = rf.logs[rf.nextIndex[i]:]
					}
					go rf.sendAppendEntries(i, &args, &AppendEntriesReply{}, &appendNums)
				}
				rf.timer.Reset(rf.GetHeartBeatTime())
			}
			rf.mu.Unlock()
		}
	}
}
