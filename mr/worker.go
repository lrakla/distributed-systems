package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int)  {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
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

	for { // works as a while True
		args := &GetTaskArgs{}
		reply := &GetTaskReply{}
		log.Println("Calling Master.GetTask...")
		call("Master.GetTask",args, reply)
		job := reply.Job //since it is a ptr, reply is what Master returns
		if job.JobType == NoJob {
			break
		}
		switch job.JobType {
		case MapJob:
			log.Println("Got a MapJob, working...")
			log.Printf("File names include: %v", job.FileNames[0])
			doMap(job, reply.TaskId, mapf)
		case ReduceJob:
			log.Println("Got a ReduceJob, working...")
			log.Printf("File names include: %v, %v, %v", job.FileNames[0], job.FileNames[1], job.FileNames[2])
			doReduce(reply.Job, reply.TaskId, reducef)
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

func doMap(job Job, taskId string,mapf func(string, string) []KeyValue){
	file, err := os.Open(job.FileNames[0])
	if err != nil {
		log.Fatalf("Cannot open %v ", job.FileNames)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v ", job.FileNames)
	}
	file.Close()
	kv := mapf(job.FileNames[0], string(content))
	sort.Sort(ByKey(kv))
	tmpfile := make([] *os.File, job.NReduce)
	for i:=0; i<job.NReduce; i++ {
		tmpfile[i], err = ioutil.TempFile("./","temp_map_")
		if err != nil {
			log.Fatal("Cannot create temp file")
		}
	}
	defer func() {
		for i:=0; i< job.NReduce; i++{
			tmpfile[i].Close()
		}
	} ()

	for _,kv_i := range kv {
		hash := ihash(kv_i.Key) % job.NReduce
		fmt.Fprintf(tmpfile[hash], "%v %v\n", kv_i.Key, kv_i.Value)
	}
	for i:=0; i< job.NReduce; i++{
		taskIdentifier := strings.Split(job.FileNames[0],"-")[1]
		os.Rename(tmpfile[i].Name(), "mr-"+taskIdentifier+"-"+strconv.Itoa(i))
	}
	newArgs := &ReportSuccessArgs{job, taskId}
	newReply := &ReportSuccessReply{}
	log.Println("Job finished, calling Master.ReportSuccess")
	log.Printf("JobType is %v", job.JobType)
	call("Master.ReportSuccess", newArgs, newReply)

}


func doReduce(job Job, taskId string,reducef func(string, []string) string){
	kva := make([][]KeyValue, len(job.FileNames))
	for i, filename := range job.FileNames{
		f,err := os.Open(filename)
		if err != nil{
			log.Fatalf("Cannot open file %v", filename)
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan(){
			tokens := strings.Split(scanner.Text()," ")
			kva[i] = append(kva[i], KeyValue{tokens[0], tokens[1]})
		}
	}
	ids := make([]int, len(job.FileNames)) // array of zeros of len filenames
	ofile,_ := ioutil.TempFile("./","temp_reduce_")
	defer ofile.Close()
	values := []string{}
	prevKey := ""
	for {
		findNext := false
		var nextI int
		for i, kv := range kva{
			if ids[i]  < len(kv){
				if !findNext{
					findNext = true
					nextI = i
				} else if strings.Compare(kv[ids[i]].Key, kva[nextI][ids[nextI]].Key)	< 0 {
					nextI = i
				}		
			
			
			}
		}
		if findNext{
			nextKV := kva[nextI][ids[nextI]]
			if prevKey == "" {
				prevKey = nextKV.Key
				values = append(values,nextKV.Value)
			} else {
				if nextKV.Key == prevKey{
					values = append(values, nextKV.Value)
				} else{
					fmt.Fprintf(ofile,"%v %v\n", prevKey, reducef(prevKey, values))
					prevKey = nextKV.Key
					values = []string{nextKV.Value}
				}
			}
			ids[nextI]++
		} else {
			break
		}

	}
	if prevKey != ""{
		fmt.Fprintf(ofile, "%v %v\n", prevKey, reducef(prevKey, values))
	}
	taskIdentifier := strings.Split(job.FileNames[0], "-")[2]
	os.Rename(ofile.Name(), "mr-out-"+taskIdentifier)
	newArgs := &ReportSuccessArgs{job, taskId}
	newReply := &ReportSuccessReply{}
	log.Println("Job finished, calling Master.ReportSuccess")
	log.Printf("JobType is %v", job.JobType)
	call("Master.ReportSuccess", newArgs, newReply)
}

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
