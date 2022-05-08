package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

func init() {
	logFile, err := os.OpenFile("./coordinator.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("open log file failed, err:", err)
		return
	}
	log.SetOutput(logFile)
	log.SetPrefix("TRACE: ")
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Llongfile)
	log.Printf("init")
}

const (
	TaskTimeout = 10 // 超时时间，秒，这里的时间需要小于测试脚本的超时时间
)

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int
	nMap    int
	//phase   SchedulePhase
	tasks map[JobType][]*Task

	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

type heartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Heartbeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := heartbeatMsg{response, make(chan struct{})}
	c.heartbeatCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) schedule() {
	c.initMapPhase()
	go func() {
		for {
			select {
			case msg := <-c.heartbeatCh:
				c.handleHeartbeatMsg(&msg)
				log.Printf("HeartbeatResponse: %#v\n", msg.response)
				msg.ok <- struct{}{}
			case msg := <-c.reportCh:
				c.handleReportMsg(&msg)
				log.Printf("ReportResponse: %#v\n", msg.request)
				msg.ok <- struct{}{}
			}
		}
	}()
}

func (c *Coordinator) initMapPhase() {
	// create map task
	for _, file := range c.files {
		c.tasks[MapJob] = append(c.tasks[MapJob], &Task{
			fileName: file,
			id:       len(c.tasks[MapJob]),
			status:   Waiting,
		})
	}
	// create reduce task
	for i := 0; i < c.nReduce; i++ {
		c.tasks[ReduceJob] = append(c.tasks[ReduceJob], &Task{
			fileName: "",
			id:       len(c.tasks[ReduceJob]),
			status:   Waiting,
		})
	}
}

func (c *Coordinator) handleHeartbeatMsg(msg *heartbeatMsg) {
	if !c.checkTasksDone(MapJob, msg) {
		return
	}
	if !c.checkTasksDone(ReduceJob, msg) {
		return
	}
	msg.response.JobType = CompleteJob
	c.doneCh <- struct{}{}
	return
}

// 检查任务是否完成了，没有完成则返回心跳信息
func (c *Coordinator) checkTasksDone(jobType JobType, msg *heartbeatMsg) bool {
	allTasksDone := true
	for _, task := range c.tasks[jobType] {
		// 是否超时？
		if task.status == Doing && time.Now().Sub(task.startTime).Seconds() > TaskTimeout {
			task.status = Waiting
		}
		if task.status == Waiting {
			msg.response.JobType = jobType
			// use TaskId or ReduceNo by JobType, so both assign task.id
			msg.response.TaskId = task.id
			msg.response.FileName = task.fileName
			msg.response.NReduce = c.nReduce

			task.startTime = time.Now()
			task.status = Doing
			return false
		}
		if task.status != Success {
			allTasksDone = false
		}
	}
	if !allTasksDone {
		msg.response.JobType = WaitJob
		return false
	}
	return true
}

func (c *Coordinator) handleReportMsg(msg *reportMsg) {
	// 成功，失败
	for _, task := range c.tasks[msg.request.JobType] {
		if task.id == msg.request.TaskId {
			task.status = msg.request.TaskStatus
			return
		}
	}
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
	log.Println("unix:", sockname)
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	//ret := false

	// Your code here.
	//for {
	select {
	case <-c.doneCh:
		time.Sleep(3 * time.Second)
		return true
	default:
		return false
	}
	//}

	//return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// TODO why write to worker.log?
	log.Printf("MakeCoordinator: files: %v nReduce:%d", files, nReduce)
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.tasks = map[JobType][]*Task{}
	c.tasks[MapJob] = make([]*Task, 0, c.nMap)
	c.tasks[ReduceJob] = make([]*Task, 0, c.nReduce)
	c.heartbeatCh = make(chan heartbeatMsg)
	c.reportCh = make(chan reportMsg)
	c.doneCh = make(chan struct{})

	c.schedule()

	c.server()
	return &c
}
