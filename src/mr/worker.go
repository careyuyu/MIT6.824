package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
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
func (a KeyValue) String() string {
	return fmt.Sprintf("%v: %v", a.Key, a.Value)
}

type WorkerStatus struct {
	workerN                     int
	mapf                        func(string, string) []KeyValue
	reducef                     func(string, []string) string
	nReduce                     int
	intermediateFileInitialized bool
	intermediateFilesName       []string
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
	}
	// Your worker implementation here.
	done := false
	for !done {
		done = workerIns.CallGetWork()
		time.Sleep(time.Second)
	}
}

// create the intermediate files for the worker, if failed will return false
func (w *WorkerStatus) initIntermediateFiles() bool {
	w.intermediateFilesName = make([]string, w.nReduce)
	for i := 0; i < w.nReduce; i++ {
		// create gfs/intermediateFiles/files.txt
		filename := fmt.Sprintf("./intermediateFiles/mr-%d-%d.txt", w.workerN, i)
		w.intermediateFilesName[i] = filename
		if file, err := os.Create(filename); err != nil {
			fmt.Printf("creation of mr-%d-%d failed due to %v\n", w.workerN, i, err.Error())
			return false
		} else {
			file.Close() //close the file after creation
		}
	}
	w.intermediateFileInitialized = true
	return true
}

func closeIntermediateFiles(fileList []*os.File) {
	for _, file := range fileList {
		if err := file.Close(); err != nil {
			continue
		}
	}
}

// CallGetWork get a work, return false if ready for new job, true if all jobs done
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
				success := w.initIntermediateFiles()
				if !success {
					return true
				}
			}
			w.handleMapJob(reply.WorkKey)
			return false
		case "reduce":
			//w.closeIntermediateFiles()
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
			//w.closeIntermediateFiles()
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
	//read the file for map task
	fmt.Printf("handling map job %v \n", jobKey)
	filename := fmt.Sprintf("../main/%v", jobKey)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return //return on err so CallReportMapDone not called
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return //return on err so CallReportMapDone not called
	}
	file.Close()
	kva := w.mapf(jobKey, string(content))
	//write the intermediate kv values to intermediate files
	//first create the encoder for each files
	intermediateFiles := make([]*os.File, len(w.intermediateFilesName))
	encoders := make([]*json.Encoder, len(w.intermediateFilesName))
	for i, intermediateFileName := range w.intermediateFilesName {
		if file, err := os.OpenFile(intermediateFileName, os.O_WRONLY|os.O_APPEND, 0644); err != nil {
			return
		} else {
			intermediateFiles[i] = file
			encoders[i] = json.NewEncoder(file)
		}
	}
	//write the kv values to file
	for _, kv := range kva {
		//fmt.Printf("%v, %v\n", kv.Key, kv.Value)
		fileIndex := ihash(kv.Key) % w.nReduce
		if err := encoders[fileIndex].Encode(&kv); err != nil {
			fmt.Printf("writing intermediate file from %v failed -- %v\n", w.workerN, err.Error())
			return //return on err so CallReportMapDone not called
		}
	}
	//close the files
	closeIntermediateFiles(intermediateFiles)

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
	kva := []KeyValue{}

	//get all files for the batch mr-*-batchNum.txt
	files, err := filepath.Glob(fmt.Sprintf("./intermediateFiles/mr-*-%v.txt", batchNum))
	if err != nil {
		fmt.Printf("Error finding files for reduce job %v\n", batchNum)
	}
	//append the kv pairs to kva for each file
	//return at any err so CallReportReduceDone don't get called
	for _, fileName := range files {
		fmt.Printf("reading file: %v \n", fileName)
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Printf("error reading file: %v \n", fileName)
			file.Close()
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err.Error() != "EOF" {
					fmt.Println("ERROR decoding file", err.Error())
					file.Close()
					return
				}
				file.Close()
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	//sort the kva for the batch
	sort.Sort(ByKey(kva))
	//create a temp file for the result
	oname := fmt.Sprintf("mr-out-%v-tempBy-%v", batchNum, w.workerN)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to temp file
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := w.reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
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
