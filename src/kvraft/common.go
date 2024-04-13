package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	GET = "Get"
	PUT = "Put"
	APPEND = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	LastString string
	Client int
	Query int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	LastString string
}

type GetReply struct {
	Err   Err
	Value string
}

func getQueryString(q int, c int) string{
	return fmt.Sprintf("%d ", q) + fmt.Sprintf("%d", c)
}