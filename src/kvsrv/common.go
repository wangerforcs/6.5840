package kvsrv
import "fmt"

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Query int
	Client int
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Query int
	Client int
}

type GetReply struct {
	Value string
}

type ReceivedArgs struct{
	Hash string
}

type ReceivedReply struct{
}

func getQueryString(key string, q int, c int) string{
	return key + fmt.Sprintf("%d", q) + fmt.Sprintf("%d", c)
}

func putQueryString(key string, value string, q int, c int) string{
	return key + value + fmt.Sprintf("%d", q) + fmt.Sprintf("%d", c)
}
