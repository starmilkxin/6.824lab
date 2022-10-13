package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqId    int
	Key      string
	Value    string
	ClientId int64
	Index    int // raft服务层传来的Index
	OpType   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	seqMap    map[int64]int     //为了确保seq只执行一次	clientId / seqId
	waitChMap map[int]chan Op   //传递由下层Raft服务的appCh传过来的command	index / chan(Op)
	kvPersist map[string]string // 存储持久化的KV键值对	K / V
}

func (kv *KVServer) ifDuplicate(clientId int64, seqId int) bool {
	if id, ok := kv.seqMap[clientId]; ok {
		if id == seqId {
			return false
		}
	}
	return true
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	// 封装Op传到下层start
	op := Op{OpType: "Get", Key: args.Key, SeqId: args.SeqId, ClientId: args.ClientId}
	lastIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvPersist[args.Key]
			kv.mu.Unlock()
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	// 封装Op传到下层start
	op := Op{OpType: args.Op, Key: args.Key, SeqId: args.SeqId, ClientId: args.ClientId}
	lastIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.seqMap = make(map[int64]int)
	kv.kvPersist = make(map[string]string)
	kv.waitChMap = make(map[int]chan Op)

	go kv.applyMsgHandlerLoop()
	return kv
}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.waitChMap[index]
	if !ok {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

// 将apply后的command进行处理后统一存储
func (kv *KVServer) applyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			index := msg.CommandIndex
			op := msg.Command.(Op)
			if !kv.ifDuplicate(op.ClientId, op.SeqId) {
				kv.mu.Lock()
				switch op.OpType {
				case "Put":
					kv.kvPersist[op.Key] = op.Value
				case "Append":
					kv.kvPersist[op.Key] += op.Value
				}
				kv.seqMap[op.ClientId] = op.SeqId
				kv.mu.Unlock()
			}
			// 将返回的ch返回waitCh
			kv.getWaitCh(index) <- op
		}
	}
}
