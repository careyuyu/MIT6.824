package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerStatus struct {
	workerN                     int
	mapf                        func(string, string) []KeyValue
	reducef                     func(string, []string) string
	nReduce                     int
	intermediateFileInitialized bool
	intermediateFiles           []*os.File
	intermediateFileEncoders    []*json.Encoder
	intermediateFileClosed      bool
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
	workerIns := WorkerStatus{
		workerN:                     -1,
		mapf:                        mapf,
		reducef:                     reducef,
		nReduce:                     -1,
		intermediateFileInitialized: false,
		intermediateFileClosed:      false,
	}
	// Your worker implementation here.
	done := false
	for !done {
		done = workerIns.CallGetWork()
		time.Sleep(time.Second)
	}
}

// create the intermediate files for the worker, if failed will return false
func (w *WorkerStatus) initializeIntermediateFiles() bool {
	for i := 0; i < w.nReduce; i++ {
		//src / mr / gfs / intermediateFiles
		filename := fmt.Sprintf("./intermediateFiles/mr-%d-%d.txt", w.workerN, i)
		file, err := os.Create(filename)
		if err != nil {
			fmt.Printf("creation of mr-%d-%d failed due to %v\n", w.workerN, i, err.Error())
			return false
		}
		w.intermediateFiles = append(w.intermediateFiles, file)
		w.intermediateFileEncoders = append(w.intermediateFileEncoders, json.NewEncoder(file))
	}
	return true
}

func (w *WorkerStatus) closeIntermediateFiles() {
	if w.intermediateFileClosed {
		return
	}
	for _, file := range w.intermediateFiles {
		file.Close()
	}
	w.intermediateFileClosed = true
}

// CallGetWork get a work, return true if ready for new job, false otherwise
func (w *WorkerStatus) CallGetWork() bool {
	args := GetWorkArgs{WorkerN: w.workerN}
	reply := GetWorkReply{}
	ok := call("Coordinator.GetWork", &args, &reply)
	if ok {
		//fmt.Printf("getWork() completed for worker: %v\n", reply.AssignedWorkerN)
		switch reply.WorkType {
		case "map":
			fmt.Printf("got map job %v\n", reply.WorkKey)
			if !w.intermediateFileInitialized {
				w.workerN = reply.AssignedWorkerN
				w.nReduce = reply.NReduce
				success := w.initializeIntermediateFiles()
				if !success {
					return false
				}
			}
			w.handleMapJob(reply.WorkKey)
			return false
		case "reduce":
			w.closeIntermediateFiles()
			fmt.Printf("got reduce job %v\n", reply.WorkKey)
			w.handleReduceJob(reply.ReduceBatch)
			return false
		case "idle":
			fmt.Printf("got idle job\n")
			time.Sleep(time.Second * 5)
			return false
		case "done":
			fmt.Printf("done, no task left\n")
			return true
		default:
			fmt.Printf("got no job\n")
			w.closeIntermediateFiles()
			return true
		}
	} else {
		fmt.Printf("getWork() failed\n")
		return true
	}
}

// function to handle map job,
// it should read the file with jobKey, use map function, and append result to intermediate file
// the intermediate file has batch number associated with key's hash
func (w *WorkerStatus) handleMapJob(jobKey string) {
	fmt.Printf("handling map job %v \n", jobKey)
	filename := fmt.Sprintf("../main/%v", jobKey)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := w.mapf(filename, string(content))
	for _, kv := range kva {
		//fmt.Printf("%v, %v\n", kv.Key, kv.Value)
		fileIndex := ihash(kv.Key) % w.nReduce
		//enc := json.NewEncoder(w.intermediateFiles[fileIndex])
		err := w.intermediateFileEncoders[fileIndex].Encode(&kv)
		if err != nil {
			fmt.Printf("writing intermediate file from %v failed\n", w.workerN)
			break
		}
	}

	w.CallReportMapDone(jobKey)
}

func (w *WorkerStatus) CallReportMapDone(jobKey string) {
	args := ReportWorkDoneArgs{WorkType: "map", WorkKey: jobKey}
	reply := ReportWorkDoneReply{}
	ok := call("Coordinator.ReportWorkDone", &args, &reply)
	if ok {
		fmt.Printf("CallReportMapDone ok\n")
	} else {
		fmt.Printf("CallReportMapDone failed\n")
	}
}

// handle a reduce job received, should read input from all files of the batchNum,
// append them to intermediate, sort the intermediate, then apply reduce, then write output to mr-out-%batchNum
func (w *WorkerStatus) handleReduceJob(batchNum int) {
	fmt.Printf("handling reduce job %v \n", batchNum)
	//intermediate := []KeyValue{}
	files, err := filepath.Glob(fmt.Sprintf("./intermediateFiles/mr-*-%v.txt", batchNum))
	if err != nil {
		fmt.Printf("Error finding files for reduce job %v\n", batchNum)
	}
	for _, file := range files {
		fmt.Printf("reading file: %v \n", file)
	}

	w.CallReportReduceDone(batchNum)
}

func (w *WorkerStatus) CallReportReduceDone(batchNum int) {
	args := ReportWorkDoneArgs{WorkType: "reduce", WorkBatch: batchNum}
	reply := ReportWorkDoneReply{}
	ok := call("Coordinator.ReportWorkDone", &args, &reply)
	if ok {
		fmt.Printf("CallReportReduceDone ok\n")
	} else {
		fmt.Printf("CallReportReduceDone failed\n")
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
