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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		// send an RPC to the coordinator asking for a task
		args := WorkerArgs{}
		reply := WorkerReply{}
		ok := call("Coordinator.ScheduleTask", &args, &reply)
		if !ok {
			log.Fatalf("Failed to schedule task")
		}

		// execute the task assigned by the coordinator
		switch reply.Type {
		case MapTask:
			doMapTask(mapf, &reply)
		case ReduceTask:
			doReduceTask(reducef, &reply)
		case Wait:
			time.Sleep(1 * time.Second)
		case Complete:
			return
		default:
			log.Fatalf("Worker: %v is not recognized", reply.Type)
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, reply *WorkerReply) {
	// read input data file
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()

	// execute map function
	intermediate := []KeyValue{}
	kva := mapf(reply.FileName, string(content))
	intermediate = append(intermediate, kva...)

	// divide intermediate keys into buckets
	buckets := make([][]KeyValue, reply.NReduce)
	for i := range buckets {
		buckets[i] = make([]KeyValue, 0)
	}
	for _, kv := range intermediate {
		idx := ihash(kv.Key) % reply.NReduce
		buckets[idx] = append(buckets[idx], kv)
	}

	// write output into files
	for i := range buckets {
		oname := fmt.Sprintf("mr-%v-%v", reply.MapTaskNumber, i)
		ofile, _ := os.CreateTemp("", oname+"*")
		enc := json.NewEncoder(ofile)
		for _, kv := range buckets[i] {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write to %v", oname)
			}
		}
		os.Rename(ofile.Name(), oname)
		ofile.Close()
	}

	// send a finish message to the coordinator
	args := WorkerArgs{reply.MapTaskNumber, 0}
	call("Coordinator.MapTaskFinish", &args, &WorkerReply{})
}

func doReduceTask(reducef func(string, []string) string, reply *WorkerReply) {
	// read intermediate data from map workers
	intermediate := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		iname := fmt.Sprintf("mr-%v-%v", i, reply.ReduceTaskNumber)
		ifile, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open %v", iname)
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ifile.Close()
	}

	// sort all data by intermediate keys
	sort.Sort(ByKey(intermediate))

	// call reduce on each distinct key in intermediate[],
	// and output the result to mr-out-x
	oname := fmt.Sprintf("mr-out-%v", reply.ReduceTaskNumber)
	ofile, _ := os.CreateTemp("", oname+"*")
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
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	os.Rename(ofile.Name(), oname)
	ofile.Close()

	// send a finish message to the coordinator
	args := WorkerArgs{0, reply.ReduceTaskNumber}
	call("Coordinator.ReduceTaskFinish", &args, &WorkerReply{})
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
