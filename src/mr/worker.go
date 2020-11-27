package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	// Your worker implementation here.
	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		reqWorkArgs := ReqWorkArgs{}
		reqWorkReply := ReqWorkReply{}
		CallReqWork(&reqWorkArgs, &reqWorkReply)

		switch reqWorkReply.TaskType {
		case "map":
			if reqWorkReply.InputFileName == "" {
				time.Sleep(1 * time.Second)
				continue
			}
			kvs := DealWithMap(reqWorkReply, mapf)
			intermediateFileSuffix := fmt.Sprintf("inter-m-%d-%s", reqWorkReply.SaltForIntermediate, strings.Replace(reqWorkReply.InputFileName, "../", "", -1))
			intermediateFileNames := WriteTointermediate(intermediateFileSuffix, kvs, reqWorkReply.NReduce)
			workDoneArgs := WorkDoneArgs{
				TaskType:              "map",
				InputFileName:         reqWorkReply.InputFileName,
				IntermediateFileNames: intermediateFileNames,
			}
			workDoneReply := WorkDoneReply{}
			CallWorkDone(&workDoneArgs, &workDoneReply)
		case "reduce":
			if reqWorkReply.ReduceID == -1 {
				time.Sleep(1 * time.Second)
				continue
			}
			DealWithReduce(&reqWorkReply, reducef)
			workDoneArgs := WorkDoneArgs{
				TaskType: "reduce",
				OutPutID: reqWorkReply.ReduceID,
			}
			workDoneReply := WorkDoneReply{}
			CallWorkDone(&workDoneArgs, &workDoneReply)
		default:
			log.Fatal("worker: inrecognized task type")
		}
	}
}
func WriteTointermediate(intermediateFileSuffix string, kvs []KeyValue, nReduce int) []string {
	encs := make([]*json.Encoder, nReduce)
	intermediateFileNames := make([]string, nReduce)
	oFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		intermediateFileNames[i] = fmt.Sprintf("%s-%d", intermediateFileSuffix, i)
		var err error
		oFiles[i], err = os.OpenFile(intermediateFileNames[i], os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatal("error when open to write intermediate file, error: ", err)
		}
		encs[i] = json.NewEncoder(oFiles[i])
	}
	defer func() {
		for _, oFile := range oFiles {
			oFile.Close()
		}
	}()
	for _, kv := range kvs {
		err := encs[ihash(kv.Key)%nReduce].Encode(&kv)
		if err != nil {
			log.Fatal("error when encoding kv error: ", err)
		}
	}
	return intermediateFileNames
}
func DealWithMap(reqWorkReply ReqWorkReply, mapf func(string, string) []KeyValue) []KeyValue {
	inputFileName := reqWorkReply.InputFileName
	content := ReadFile(inputFileName)
	return mapf(inputFileName, content)
}

func DealWithReduce(reqWorkReply *ReqWorkReply, reducef func(string, []string) string) {
	oFile, err := ioutil.TempFile("", "temp")
	if err != nil {
		log.Fatal("create temp file error: ", err)
	}
	defer oFile.Close()

	var intermediate []KeyValue
	for _, intermediateFile := range reqWorkReply.IntermediateFiles {
		intermediate = append(intermediate, ReadintermediateFile(intermediateFile)...)
	}
	sort.Sort(ByKey(intermediate))
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	oFileName := fmt.Sprintf("mr-out-%d", reqWorkReply.ReduceID)
	os.Rename(oFile.Name(), oFileName)
}

func ReadintermediateFile(fileName string) []KeyValue {
	oFile, err := os.Open(fileName)
	if err != nil {
		log.Fatal("error when open to read intermediate file error: ", err)
	}
	defer oFile.Close()
	dec := json.NewDecoder(oFile)
	var kvs []KeyValue
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

// call request work rpc

func CallReqWork(reqWorkArgs *ReqWorkArgs, reqWorkReply *ReqWorkReply) {
	MasterAlive := call("Master.ReqWork", reqWorkArgs, reqWorkReply)
	if !MasterAlive {
		os.Exit(0)
	}
}

// call when work is Done
func CallWorkDone(workDoneArgs *WorkDoneArgs, workDoneReply *WorkDoneReply) {
	MasterAlive := call("Master.WorkDone", workDoneArgs, workDoneReply)
	if !MasterAlive {
		os.Exit(0)
	}
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

func ReadFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return string(content)
}
