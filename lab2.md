# Raft
实现一个简单的raft。
主要分为Leader Election和Log Replication两部分。

首先初始化所有的信息
```golang
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
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
```

ticker函数是每个实体的生命周期
```golang
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
```

当一个follower的时间过期后，便会成为candidate。
candidate时间过期后便会投票给自己，然后重新设置过期时间，开协程调用rpc方法RequestVote向其他所有结点请求投票。
如果失败，那么就反复重传（可以设定阈值做不同处理）。如果返回的数据经过校验为对方同意投票的话，那么自身的票数就增加，过半后晋升为leader。同时初始化好自身信息，并重置心跳时间。

RequestVote为其他服务器所调用，然后被调用方服务器对请求的数据与自身进行对比（竞选者超时，自身超时，是否本轮投票过等等），最终返回投票的结果给调用方。
```golang
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
```

对于leader，则是循环地向其他所有结点发送心跳包，如果有新的logs就一齐发送。

当对方回复成功时，那么就知道此次的消息传递成功了，如果回复失败，则需要判别是对方的原因还是出现了网络分区，自己过时。如果自己过时则需要改变角色为follower，否则是根据对方的回复改变要传递的消息位置。
```golang
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
```

AppendEntries用于被leader调用，当有请求来时，根据请求判断是自身超时还是对方超时。
如果对方超时则返回失败，如果自身超时则修改自身的消息，同时将自身的身份改为follower。
如果双方的日志位置不一致，则也返回失败，降级为follower。
如果双方的最新日志的版本不一致，也返回失败，降级为follower。
如果都没问题，则依旧将自身降级为follower，重置超时时间。如果存在日志包则进行追加，提交日志，最后返回成功。
```golang
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
```