package kvsrv
import "fmt"

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	LastString string
	Qstring string
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	LastString string
}

type GetReply struct {
	Value string
}

func getQueryString(q int, c int) string{
	return fmt.Sprintf("%d ", q) + fmt.Sprintf("%d", c)
}
