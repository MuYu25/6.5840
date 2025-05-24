package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := MessageSend{}
		reply := MessageReply{}
		call("Coordinator.RequestTask", &args, &reply)
		switch reply.TaskType {
		case MapTask:
			HandleMapTask(&reply, mapf)
		case ReduceTask:
			HandleReduceTask(&reply, reducef)
		case Wait:
			time.Sleep(time.Second)
		case Exit:
			os.Exit(0)
		default:
			time.Sleep(time.Second)
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

func HandleMapTask(reply *MessageReply, mapf func(string, string) []KeyValue) {

	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
		return
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
		return
	}
	file.Close()
	kva := mapf(reply.FileName, string(content))

	// 按照key进行分组，相同的key会存放在相同的intermaeidate file中
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		intermediate[r] = append(intermediate[r], kv)
	}

	for r, kva := range intermediate {
		// 文件中使用taskid,避免多个worker对同一个文件，每个文件还是独立进行读写。
		oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, r)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create Temp %v: %v", oname, err)
		}

		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			if err := enc.Encode(kv); err != nil {
				log.Fatalf("failed to encode kv pair: %v", err)
			}
		}
		ofile.Close()

		err = os.Rename(ofile.Name(), oname)
		if err != nil {
			log.Fatalf("failed to rename file %s to %s: %v", ofile.Name(), oname, err)
		}

		if _, err := os.Stat(oname); err != nil {
			log.Fatalf("donot exist file: %v: %v", oname, err)
		}

	}

	args := MessageSend{
		TaskID:         reply.TaskID,
		CompleteStatus: MapTaksComplete,
	}

	call("Coordinator.ReportTask", &args, &MessageReply{})

}

func generateFileName(r int, NMap int) []string {
	var fileName []string
	for TaskID := 0; TaskID < NMap; TaskID++ {
		fileName = append(fileName, fmt.Sprintf("mr-%d-%d", TaskID, r))
	}
	return fileName
}

func HandleReduceTask(reply *MessageReply, reducef func(string, []string) string) {
	var intermediate []KeyValue
	intermediateFiles := generateFileName(reply.TaskID, reply.NMap)
	for _, filename := range intermediateFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	oname := fmt.Sprintf("mr-out-%v", reply.TaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
	}
	for i := 0; i < len(intermediate); {
		j := i
		values := make([]string, 0)
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			values = append(values, intermediate[j].Value)
			j++
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), oname)
	args := MessageSend{
		TaskID:         reply.TaskID,
		CompleteStatus: ReduceTaskComplete,
	}
	call("Coordinator.ReportTask", args, &MessageReply{})

}
