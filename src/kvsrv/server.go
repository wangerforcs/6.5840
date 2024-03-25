package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	kvm sync.Mutex
	dupm sync.Mutex

	// Your definitions here.
	kvMap map[string]string
	dupdetect map[string]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.kvm.Lock()
	defer kv.kvm.Unlock()
	value, ok := kv.kvMap[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.dupm.Lock()
	_, Ok := kv.dupdetect[putQueryString(args.Key, args.Value, args.Query, args.Client)]
	if Ok {
		kv.dupm.Unlock();
		return
	}
	kv.dupdetect[putQueryString(args.Key, args.Value, args.Query, args.Client)] = ""
	kv.dupm.Unlock();

	kv.kvm.Lock()
	defer kv.kvm.Unlock()
	kv.kvMap[args.Key] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.dupm.Lock();
	_, Ok := kv.dupdetect[putQueryString(args.Key, args.Value, args.Query, args.Client)]
	if Ok {
		reply.Value = kv.dupdetect[putQueryString(args.Key, args.Value, args.Query, args.Client)]
		kv.dupm.Unlock();
		return
	}
	kv.dupm.Unlock();
	kv.kvm.Lock()
	value, ok := kv.kvMap[args.Key]
	if ok {
		kv.kvMap[args.Key] = value + args.Value
		reply.Value = value
	} else {
		kv.kvMap[args.Key] = args.Value
		reply.Value = ""
	}
	kv.kvm.Unlock()
	kv.dupm.Lock();
	kv.dupdetect[putQueryString(args.Key, args.Value, args.Query, args.Client)] = reply.Value
	kv.dupm.Unlock();
}

func (kv *KVServer) Received(args *ReceivedArgs, reply* ReceivedReply) {
	kv.dupm.Lock()
	defer kv.dupm.Unlock()
	delete(kv.dupdetect, args.Hash)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvMap = make(map[string]string)
	kv.dupdetect = make(map[string]string)
	// You may need initialization code here.
	return kv
}
