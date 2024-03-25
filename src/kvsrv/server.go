package kvsrv

import (
	"fmt"
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
	mu sync.Mutex

	// Your definitions here.
	kvMap map[string]string
	dupdetect map[string]bool
}

func getQueryString(key string, q int, c int) string{
	return key + fmt.Sprintf("%d", q) + fmt.Sprintf("%d", c)
}

func putQueryString(key string, value string, q int, c int) string{
	return key + value + fmt.Sprintf("%d", q) + fmt.Sprintf("%d", c)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, Ok := kv.dupdetect[getQueryString(args.Key, args.Query, args.Client)]
	if Ok {
		return
	}
	// kv.dupdetect[getQueryString(args.Key, args.Query, args.Client)] = true
	value, ok := kv.kvMap[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, Ok := kv.dupdetect[putQueryString(args.Key, args.Value, args.Query, args.Client)]
	if Ok {
		return
	}
	// kv.dupdetect[putQueryString(args.Key, args.Value, args.Query, args.Client)] = true
	kv.kvMap[args.Key] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, Ok := kv.dupdetect[putQueryString(args.Key, args.Value, args.Query, args.Client)]
	if Ok {
		return
	}
	// kv.dupdetect[putQueryString(args.Key, args.Value, args.Query, args.Client)] = true
	value, ok := kv.kvMap[args.Key]
	if ok {
		kv.kvMap[args.Key] = value + args.Value
		reply.Value = value
	} else {
		kv.kvMap[args.Key] = args.Value
		reply.Value = ""
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvMap = make(map[string]string)
	kv.dupdetect = make(map[string]bool)
	// You may need initialization code here.

	return kv
}
