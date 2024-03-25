package kvsrv

import (
	"crypto/rand"
	"math/big"
	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int
	queryId  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clientId = int(nrand())
	ck.queryId = 1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{}
	reply := GetReply{}
	args.Client = ck.clientId
	args.Query = ck.queryId
	ck.queryId++
	args.Key = key
	ok := false
	for !ok {
		ok = ck.server.Call("KVServer.Get", &args, &reply)
	}
	newargs := ReceivedArgs{}
	newreply := ReceivedReply{}
	newargs.Hash = getQueryString(key, args.Query, args.Client)
	ck.server.Call("KVServer.Received", &newargs, &newreply)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	ok := false
	args := PutAppendArgs{}
	reply := PutAppendReply{}
	args.Key = key
	args.Value = value
	args.Client = ck.clientId
	args.Query = ck.queryId
	ck.queryId++
	for !ok {
		ok = ck.server.Call("KVServer."+op, &args, &reply)
	}
	newargs := ReceivedArgs{}
	newreply := ReceivedReply{}
	newargs.Hash = putQueryString(key, value, args.Query, args.Client)
	ck.server.Call("KVServer.Received", &newargs, &newreply)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
