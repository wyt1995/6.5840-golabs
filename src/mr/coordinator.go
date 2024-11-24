package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files        []string
	nMap         int
	nReduce      int
	nMapDone     int
	nReduceDone  int
	mapStatus    []int
	reduceStatus []int
	mu           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ScheduleTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nMapDone < c.nMap {
		c.scheduleMapTask(reply)
	} else if c.nReduceDone < c.nReduce {
		c.scheduleReduceTask(reply)
	} else {
		reply.Type = Complete
	}
	return nil
}

func (c *Coordinator) scheduleMapTask(reply *WorkerReply) {
	task := -1
	for i := 0; i < c.nMap; i++ {
		if c.mapStatus[i] == 0 {
			task = i
			break
		}
	}

	if task == -1 {
		reply.Type = Wait
	} else {
		reply.Type = MapTask
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		reply.FileName = c.files[task]
		reply.MapTaskNumber = task
		c.mapStatus[task] = 1

		// start goroutine to monitor task status
		go func(num int) {
			time.Sleep(10 * time.Second)
			c.mu.Lock()
			defer c.mu.Unlock()

			// if still not finished, assume the map worker has failed
			if c.mapStatus[num] == 1 {
				c.mapStatus[num] = 0
			}
		}(task)
	}
}

func (c *Coordinator) scheduleReduceTask(reply *WorkerReply) {
	task := -1
	for i := 0; i < c.nReduce; i++ {
		if c.reduceStatus[i] == 0 {
			task = i
			break
		}
	}

	if task == -1 {
		reply.Type = Wait
	} else {
		reply.Type = ReduceTask
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		reply.ReduceTaskNumber = task
		c.reduceStatus[task] = 1

		go func(num int) {
			time.Sleep(10 * time.Second)
			c.mu.Lock()
			defer c.mu.Unlock()

			if c.reduceStatus[num] == 1 {
				c.reduceStatus[num] = 0
			}
		}(task)
	}
}

func (c *Coordinator) MapTaskFinish(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	num := args.MapTaskNumber
	if c.mapStatus[num] != 2 {
		c.mapStatus[num] = 2
		c.nMapDone++
	}
	return nil
}

func (c *Coordinator) ReduceTaskFinish(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	num := args.ReduceTaskNumber
	if c.reduceStatus[num] != 2 {
		c.reduceStatus[num] = 2
		c.nReduceDone++
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.nReduce == c.nReduceDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.nMapDone = 0
	c.nReduceDone = 0
	c.mapStatus = make([]int, c.nMap)
	c.reduceStatus = make([]int, c.nReduce)

	c.server()
	return &c
}
