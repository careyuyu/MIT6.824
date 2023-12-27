package mr

import (
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce int

	curWorkerNum   int
	curWorkerNumMU sync.Mutex

	mapJobStatus      map[string]bool //true if the map file with key is done
	mapJobStatusMU    sync.Mutex      //mu for map job status
	mapJobPool        chan MapJob     //channel for remaining map job
	mapJobRemaining   int
	mapJobRemainingMU sync.Mutex

	reduceJobStatus      map[int]bool //split the reduce job with nReduce, if true the job is done
	reduceJobStatusMU    sync.Mutex
	reduceJobPool        chan int
	reduceJobRemaining   int
	reduceJobRemainingMU sync.Mutex

	jobsDone bool
}

type MapJob struct {
	key string
	id  string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// checks after 10 seconds if a map work has been done. if not, push the map work back to remaining task
func (c *Coordinator) checkMapWorkDone(job MapJob) {
	time.Sleep(time.Second * 10)
	c.mapJobStatusMU.Lock()
	defer c.mapJobStatusMU.Unlock()
	if !c.mapJobStatus[job.key] {
		go func() { c.mapJobPool <- job }()
	}
}

func (c *Coordinator) checkReduceWorkDone(batchNum int) {
	time.Sleep(time.Second * 10)
	c.reduceJobStatusMU.Lock()
	defer c.reduceJobStatusMU.Unlock()
	if !c.reduceJobStatus[batchNum] {
		go func() { c.reduceJobPool <- batchNum }()
	}
}

func (c *Coordinator) assignWorkerNum() int {
	c.curWorkerNumMU.Lock()
	ret := c.curWorkerNum
	c.curWorkerNum += 1
	c.curWorkerNumMU.Unlock()
	return ret
}

// RPC handler for workers to get a work
func (c *Coordinator) GetWork(args *GetWorkArgs, reply *GetWorkReply) error {
	fmt.Printf("GetWork is called\n")
	if c.mapJobRemaining != 0 { //map jobs not finished, assign to worker if there's any remaining from job channel
		select {
		case mapJob := <-c.mapJobPool: //there's job in channel
			//assign worker number if not assigned
			if args.WorkerId == -1 {
				reply.AssignedWorkerId = c.assignWorkerNum()
			} else {
				reply.AssignedWorkerId = args.WorkerId
			}
			fmt.Printf("map job %v is assigned to %v\n", mapJob.key, reply.AssignedWorkerId)
			reply.WorkType = "map"
			reply.WorkKey = mapJob.key
			reply.WorkId = mapJob.id
			reply.NReduce = c.nReduce
			go c.checkMapWorkDone(mapJob) //thread to check if assigned work is done
		default: //map job not done and no job left in channel, should idle
			reply.WorkType = "idle"
		}
	} else if c.reduceJobRemaining != 0 { //map jobs are done, assign reduce jobs
		select {
		case batchNum := <-c.reduceJobPool:
			fmt.Printf("reduce job %v is assigned to %v\n", batchNum, args.WorkerId)
			reply.WorkType = "reduce"
			reply.ReduceBatch = batchNum
			go c.checkReduceWorkDone(batchNum)
		default: //map job not done and no job left in channel, should idle
			reply.WorkType = "idle"
		}
	} else { // all jobs done
		reply.WorkType = "done"
	}
	return nil
}

func (c *Coordinator) preprocessReduceTask() {
	for batchNum := 0; batchNum < c.nReduce; batchNum++ {
		go func(batchN int) { c.reduceJobPool <- batchN }(batchNum)
	}
}

// RPC handler for workers to report a completion of work
func (c *Coordinator) ReportWorkDone(args *ReportWorkDoneArgs, reply *ReportWorkDoneReply) error {
	if args.WorkType == "map" { //for map job report
		jobKey := args.WorkKey
		//first check if the assigned work has done by other worker
		c.mapJobStatusMU.Lock()
		defer c.mapJobStatusMU.Unlock()
		c.mapJobRemainingMU.Lock()
		defer c.mapJobRemainingMU.Unlock()
		if !c.mapJobStatus[jobKey] { //map job is not finished by others, handle it
			c.mapJobStatus[jobKey] = true
			c.mapJobRemaining -= 1
			fmt.Printf("map job %v is done, remaining %v\n", jobKey, c.mapJobRemaining)
		}
		if c.mapJobRemaining == 0 {
			c.preprocessReduceTask()
		}
	} else { //handle reduce job report
		c.reduceJobStatusMU.Lock()
		defer c.reduceJobStatusMU.Unlock()
		c.reduceJobRemainingMU.Lock()
		defer c.reduceJobRemainingMU.Unlock()
		if !c.reduceJobStatus[args.WorkBatch] {
			c.reduceJobStatus[args.WorkBatch] = true
			c.reduceJobRemaining -= 1
			fmt.Printf("reduce job %v is done, remaining %v\n", args.WorkBatch, c.reduceJobRemaining)
		}
		if c.reduceJobRemaining == 0 { //TODO handle finish
			c.jobsDone = true
		}
	}
	return nil
}

func deleteAllFilesInDir(dirPath string) error {
	// Walk through the directory
	return filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip the directory itself
		if path == dirPath {
			return nil
		}

		// Check if it's a file (and not a directory)
		if !info.IsDir() {
			// Remove the file
			err := os.Remove(path)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.jobsDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:            nReduce,
		mapJobPool:         make(chan MapJob),
		mapJobStatus:       make(map[string]bool),
		reduceJobPool:      make(chan int),
		reduceJobStatus:    make(map[int]bool),
		reduceJobRemaining: nReduce,
		curWorkerNum:       0,
	}
	for i, v := range files {
		go func(jobKey string, jobId int) { c.mapJobPool <- MapJob{id: strconv.Itoa(jobId), key: jobKey} }(v, i)
		c.mapJobStatus[v] = false
		c.mapJobRemaining += 1
	}
	deleteAllFilesInDir("./intermediateFiles")
	fmt.Println("coordinator initialized")
	c.server()
	return &c
}
