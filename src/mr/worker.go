package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

func init() {
	logFile, err := os.OpenFile("./worker.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("open log file failed, err:", err)
		return
	}
	log.SetOutput(logFile)
	log.SetPrefix("TRACE: ")
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Llongfile)
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
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

	// Your worker implementation here.
	for {
		response := doHeartbeat()
		log.Printf("worker: receive coordinator's heartbeat %v \n", response)
		if response == nil {
			log.Printf("response is nil, continue")
			continue
		}
		switch response.JobType {
		case MapJob:
			doMapTask(mapf, response)
		case ReduceJob:
			doReduceTask(reducef, response)
		case WaitJob:
			time.Sleep(1 * time.Second)
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.JobType))
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// 心跳检测，请求任务
func doHeartbeat() (reply *HeartbeatResponse) {
	ok := call("Coordinator.Heartbeat", &HeartbeatRequest{}, &reply)
	if ok {
		log.Printf("HeartbeatResponse: %v\n", reply)
	} else {
		log.Printf("Heartbeat call failed!: %v\n", reply)
	}
	return
}

// 执行map，并汇报任务
func doMapTask(mapf func(string, string) []KeyValue, response *HeartbeatResponse) {
	defer func() {
		status := Success
		if err := recover(); err != nil { //产生了panic异常
			log.Printf("doMapTask panic:%s", err)
			status = Waiting // 通知coordinator重试
		}
		// 汇报任务
		doReport(MapJob, response.TaskId, status)
	}()

	file, err := os.Open(response.FileName)
	if err != nil {
		panic(fmt.Sprintf("cannot open %v", response.FileName))
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		panic(fmt.Sprintf("cannot read %v", response.FileName))
	}
	file.Close()
	kva := mapf(response.FileName, string(content))

	// store intermediate key/value pairs in files
	files := map[string]*os.File{}
	for _, kv := range kva {
		y := ihash(kv.Key) % response.NReduce
		name := fmt.Sprintf("mr-%d-%d", response.TaskId, y)
		if files[name] == nil {
			files[name], _ = ioutil.TempFile(".", name)
		}
		enc := json.NewEncoder(files[name])
		if err = enc.Encode(kv); err != nil {
			panic(fmt.Sprintf("Encode error: %v", err.Error()))
		}
	}
	for n, f := range files {
		f.Close()
		// 重命名，保证map原子性
		os.Rename(f.Name(), n)
	}
}

// 执行reduce，并汇报任务
func doReduceTask(reducef func(string, []string) string, response *HeartbeatResponse) {
	defer func() {
		status := Success
		if err := recover(); err != nil { //产生了panic异常
			log.Printf("doReduceTask panic:%s", err)
			status = Waiting // 通知coordinator重试
		}
		// 汇报任务
		doReport(ReduceJob, response.TaskId, status)
	}()

	// read files
	intermediate := make([]KeyValue, 0, 100)
	dir, _ := ioutil.ReadDir(".")
	for _, fi := range dir {
		// 过滤指定格式
		reg, _ := regexp.Compile(fmt.Sprintf(`mr-\d+-%d`, response.TaskId))
		ok := !fi.IsDir() && reg.MatchString(fi.Name())
		if ok {
			file, _ := os.ReadFile(fi.Name())
			dec := json.NewDecoder(bytes.NewReader(file))
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
		}
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", response.TaskId)
	ofile, _ := ioutil.TempFile(".", oname)

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	os.Rename(ofile.Name(), oname)
}

// 汇报任务
func doReport(jobType JobType, taskId int, status TaskStatus) (reply ReportResponse) {
	request := &ReportRequest{
		JobType:    jobType,
		TaskId:     taskId,
		TaskStatus: status,
	}
	ok := call("Coordinator.Report", request, &reply)
	if ok {
		log.Printf("ReportResponse: %v\n", reply)
	} else {
		log.Printf("Report call failed!\n")
	}
	return
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

	log.Println(err)
	return false
}
