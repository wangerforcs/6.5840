package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	id := os.Getpid()
	ttype := NULL
	tid := NULL
	for {
		args := RequestTaskArgs{
			WorkId:   id,
			TaskType: ttype,
			TaskId:   tid,
		}
		reply := RequestTaskReply{}
		call("Coordinator.RequestTask", &args, &reply)
		if reply.TaskType == MAP {
			file, _ := os.Open(reply.File)
			content, _ := ioutil.ReadAll(file)
			kva := mapf(reply.File, string(content))
			hashMap := make(map[int][]KeyValue)
			for _, kv := range kva {
				reduceIndex := ihash(kv.Key) % reply.ReduceNum
				hashMap[reduceIndex] = append(hashMap[reduceIndex], kv)
			}
			for i := 0; i < reply.ReduceNum; i++ {
				tmpFile, _ := os.Create(tmpMapOutFile(id, reply.TaskId, i))
				for _, kv := range hashMap[i] {
					fmt.Fprintf(tmpFile, "%v %v\n", kv.Key, kv.Value)
				}
				tmpFile.Close()
			}
		} else if reply.TaskType == REDUCE {
			var lines []string
			for i := 0; i < reply.MapNum; i++ {
				file, _ := os.Open(finalMapOutFile(i, reply.TaskId))
				content, _ := ioutil.ReadAll(file)
				lines = append(lines, strings.Split(string(content), "\n")...)
			}
			intermediate := []KeyValue{}
			for _, line := range lines {
				if line == "" {
					continue
				}
				parts := strings.Split(line, " ")
				intermediate = append(intermediate, KeyValue{Key: parts[0], Value: parts[1]})
			}
			sort.Sort(ByKey(intermediate))

			ofile, _ := os.Create(tmpReduceOutFile(id, reply.TaskId))
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				// reducef 处理key和value数组，将所有value加和，由于每个单词出现一次就记录一条，因此也就是得到数组长度
				output := reducef(intermediate[i].Key, values)
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			ofile.Close()
		} else {
			break
		}
		ttype = reply.TaskType
		tid = reply.TaskId
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
