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
	dupDetect map[string]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.kvm.Lock()
	value, ok := kv.kvMap[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	kv.kvm.Unlock()

	kv.dupm.Lock()
	delete(kv.dupDetect, args.LastString)
	kv.dupm.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	qstring := getQueryString(args.Client, args.Query)
	kv.dupm.Lock()
	delete(kv.dupDetect, args.LastString)
	_, Ok := kv.dupDetect[qstring]
	if Ok {
		kv.dupm.Unlock();
		return
	}
	kv.dupDetect[qstring] = ""
	kv.dupm.Unlock();

	kv.kvm.Lock()
	defer kv.kvm.Unlock()
	kv.kvMap[args.Key] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	qstring := getQueryString(args.Client, args.Query)
	kv.dupm.Lock();
	delete(kv.dupDetect, args.LastString)
	_, Ok := kv.dupDetect[qstring]
	if Ok {
		reply.Value = kv.dupDetect[qstring]
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
	kv.dupDetect[qstring] = reply.Value
	kv.dupm.Unlock();
}


func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvMap = make(map[string]string)
	kv.dupDetect = make(map[string]string)
	// You may need initialization code here.
	return kv
}
