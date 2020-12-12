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

type Master struct {
	// Your definitions here.
	sync.Mutex
	NReduce int

	Phase string

	InputFiles       []string
	InputFileNum     int
	InputPtr         int
	FailedInputFile  []string
	DoneInputChanMap map[string]chan bool

	SaltForIntermediate  int
	IntermediateFiles    []string
	IntermediateFilesMap map[int][]string
	DoneInputFiles       map[string]bool
	ReduceID             int
	FailedRecudeTask     []int

	DoneReduceChanMap map[int]chan bool
	ReduceIdDone      map[int]bool
	CanExit           bool
}

func (m *Master) GetPhase() string {
	return m.Phase
}
func (m *Master) SetPhase(phase string) {
	m.Phase = phase
}
func (m *Master) NextSaltForIntermediate() int {
	salt := m.SaltForIntermediate
	m.SaltForIntermediate++
	return salt
}
func (m *Master) NextInputFile() string {
	var inputFile string
	if len(m.FailedInputFile) > 0 {
		inputFile = m.FailedInputFile[0]
		m.FailedInputFile = m.FailedInputFile[1:]
	} else {
		if m.InputPtr >= m.InputFileNum {
			inputFile = ""
		} else {
			inputFile = m.InputFiles[m.InputPtr]
			m.InputPtr++
		}
	}
	return inputFile
}
func (m *Master) NextReduceID() int {
	var reduceID int
	if len(m.FailedRecudeTask) > 0 {
		reduceID = m.FailedRecudeTask[0]
		m.FailedRecudeTask = m.FailedRecudeTask[1:]
	} else if m.ReduceID < m.NReduce {
		reduceID = m.ReduceID
		m.ReduceID++
	} else {
		reduceID = -1
	}
	return reduceID
}
func (m *Master) GetIntermediateFilesByID(ID int) []string {
	if IntermediateFiles, ok := m.IntermediateFilesMap[ID]; ok {
		return IntermediateFiles
	}
	return []string{}
}
func (m *Master) AddFailedInputFile(fileName string) {
	m.FailedInputFile = append(m.FailedInputFile, fileName)
}
func (m *Master) AddFailedReduceTask(reduceID int) {
	m.FailedRecudeTask = append(m.FailedRecudeTask, reduceID)
}

// Your code here -- RPC handlers for the worker to call.

//request work rpc handler
func (m *Master) ReqWork(args *ReqWorkArgs, reply *ReqWorkReply) error {
	m.Lock()
	defer m.Unlock()
	reply.TaskType = m.GetPhase()
	if reply.TaskType == "map" {
		m.AssignMapTask(reply)
		go m.SetUpMapTimer(reply.InputFileName)
	} else if reply.TaskType == "reduce" {
		m.AssignReduceTask(reply)
		go m.SetUpReduceTimer(reply.ReduceID)
	}
	return nil
}

func (m *Master) AssignMapTask(reply *ReqWorkReply) {
	reply.InputFileName = m.NextInputFile()
	reply.NReduce = m.NReduce
	reply.SaltForIntermediate = m.NextSaltForIntermediate()

	if _, ok := m.DoneInputChanMap[reply.InputFileName]; !ok {
		m.DoneInputChanMap[reply.InputFileName] = make(chan bool)
	}
}

func (m *Master) AssignReduceTask(reply *ReqWorkReply) {
	reply.ReduceID = m.NextReduceID()
	if reply.ReduceID != -1 {
		reply.IntermediateFiles = m.GetIntermediateFilesByID(reply.ReduceID)
	}

	//if it exist do not create a new one, due to it is fine to get result from previous slow reduce worker
	if _, ok := m.DoneReduceChanMap[reply.ReduceID]; !ok {
		m.DoneReduceChanMap[reply.ReduceID] = make(chan bool)
	}
}

//timer for hearing word done by map task
func (m *Master) SetUpMapTimer(inputFileName string) {
	timer := time.NewTimer(10 * time.Second)
	select {
	case <-timer.C:
		m.Lock()
		defer m.Unlock()
		if _, ok := m.DoneInputFiles[inputFileName]; !ok {
			m.FailedInputFile = append(m.FailedInputFile, inputFileName)
		}
		return
	case <-m.DoneInputChanMap[inputFileName]:
		return
	}
}

func (m *Master) SetUpReduceTimer(reduceID int) {
	timer := time.NewTimer(10 * time.Second)
	select {
	case <-timer.C:
		m.Lock()
		defer m.Unlock()
		if _, ok := m.ReduceIdDone[reduceID]; !ok {
			m.FailedRecudeTask = append(m.FailedRecudeTask, reduceID)
		}
		return
	case <-m.DoneReduceChanMap[reduceID]:
		return
	}
}
func (m *Master) WorkDone(args *WorkDoneArgs, reply *WorkDoneReply) error {
	m.Lock()
	defer m.Unlock()
	if m.GetPhase() != args.TaskType {
		return nil
	}

	switch m.GetPhase() {
	case "map":
		m.MapWorkDone(args, reply)
	case "reduce":
		m.ReduceWorkDone(args, reply)
	default:
	}
	return nil
}

func (m *Master) MapWorkDone(args *WorkDoneArgs, reply *WorkDoneReply) {
	if _, ok := m.DoneInputFiles[args.InputFileName]; ok {
		return
	}

	m.DoneInputFiles[args.InputFileName] = true
	m.DoneInputChanMap[args.InputFileName] <- true
	m.IntermediateFiles = append(m.IntermediateFiles, args.IntermediateFileNames...)
	//assuming that files are of ascending order and do not expect blank
	for i := 0; i < len(args.IntermediateFileNames); i++ {
		m.IntermediateFilesMap[i] = append(m.IntermediateFilesMap[i], args.IntermediateFileNames[i])
	}
	if m.IsMapsComplete() {
		m.SetPhase("reduce")
	}
}

func (m *Master) IsMapsComplete() bool {
	CompleteInputFiles := len(m.DoneInputFiles) == m.InputFileNum
	return CompleteInputFiles
}

func (m *Master) ReduceWorkDone(args *WorkDoneArgs, reply *WorkDoneReply) {
	m.ReduceIdDone[args.OutPutID] = true
	m.DoneReduceChanMap[args.OutPutID] <- true
	CompleteAllReduceTasks := len(m.ReduceIdDone) == m.NReduce
	if CompleteAllReduceTasks {
		m.Exit()
	}
}
func (m *Master) Exit() {

}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.
	m.Lock()
	defer m.Unlock()
	return m.CanExit
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.NReduce = nReduce
	m.ReduceID = 0
	m.DoneInputFiles = make(map[string]bool)
	m.IntermediateFilesMap = make(map[int][]string)
	m.DoneInputChanMap = make(map[string]chan bool)
	m.DoneReduceChanMap = make(map[int]chan bool)
	m.InputFiles = files
	m.ReduceIdDone = make(map[int]bool)
	m.InputFileNum = len(files)
	m.SetPhase("map")
	m.CanExit = false
	m.server()
	return &m
}
