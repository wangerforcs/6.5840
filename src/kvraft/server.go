package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type   string
	Key    string
	Value  string
	Client int
	Query  int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dupm sync.Mutex
	kvMap map[string]string
	dupDetect map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	value, ok := kv.kvMap[args.Key]
	if ok {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()

	kv.dupm.Lock()
	delete(kv.dupDetect, args.LastString)
	kv.dupm.Unlock()
}

// unlike in lab 2, neither Put nor Append should return a value.
// this is already reflected in the PutAppendReply struct.
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	qstring := getQueryString(args.Client, args.Query)
	kv.dupm.Lock()
	delete(kv.dupDetect, args.LastString)
	_, Ok := kv.dupDetect[qstring]
	if Ok {
		kv.dupm.Unlock();
		reply.Err = OK
		return
	}
	kv.dupDetect[qstring] = ""
	kv.dupm.Unlock();

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.kvMap[args.Key] = args.Value
	reply.Err = OK
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	qstring := getQueryString(args.Client, args.Query)
	kv.dupm.Lock();
	delete(kv.dupDetect, args.LastString)
	reply.Err = OK
	_, Ok := kv.dupDetect[qstring]
	if Ok {
		kv.dupm.Unlock();
		return
	}
	kv.dupm.Unlock();
	
	kv.mu.Lock()
	value, ok := kv.kvMap[args.Key]
	var dupvalue string
	if ok {
		kv.kvMap[args.Key] = value + args.Value
		dupvalue = value
	} else {
		kv.kvMap[args.Key] = args.Value
		dupvalue = ""
	}
	kv.mu.Unlock()

	kv.dupm.Lock();
	kv.dupDetect[qstring] = dupvalue
	kv.dupm.Unlock();
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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

	return kv
}
