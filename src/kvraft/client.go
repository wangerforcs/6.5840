package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu sync.Mutex
	leaderId int
	clientId int
	queryId  int
	LastString string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{}
	reply := GetReply{}
	args.LastString = ck.LastString
	args.Key = key
	ok := false
	tryleader := ck.leaderId
	for{
		ok = ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok{
			tryleader = (tryleader+1)%len(ck.servers)
			continue
		}
		if reply.Err == OK{
			ck.leaderId = tryleader
			break
		}else if reply.Err == ErrNoKey{
			break
		}else{
			tryleader = (tryleader+1)%len(ck.servers)
		}
	
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ok := false
	tryleader := ck.leaderId
	args := PutAppendArgs{}
	reply := PutAppendReply{}
	args.Key = key
	args.LastString = ck.LastString
	args.Value = value
	args.Client = ck.clientId
	args.Query = ck.queryId
	ck.queryId++
	for{
		ok = ck.servers[tryleader].Call("KVServer."+op, &args, &reply)
		if !ok{
			tryleader = (tryleader+1)%len(ck.servers)
			continue
		}
		if reply.Err == OK{
			ck.leaderId = tryleader
			break
		}else if reply.Err == ErrNoKey{
			break
		}else{
			tryleader = (tryleader+1)%len(ck.servers)
		}
	}
	ck.LastString = getQueryString(args.Client, args.Query)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
