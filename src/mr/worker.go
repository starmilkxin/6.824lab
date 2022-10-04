package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		// 申请任务
		reply := RequestForTask()
		if reply.Task.Type == -1 {
			return
		}
		if reply.Task.Type == Running {
			continue
		}
		if reply.Task.Type == MapType {
			for _, filename := range reply.Task.Files {
				file, err := os.Open(filename)
				defer file.Close()
				if err != nil {
					log.Fatalf("cannot open %v \n", filename)
					return
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v \n", filename)
					return
				}
				// map操作
				kva := mapf(filename, string(content))
				// 将key的hash值%reduceN相同的kv，存储在同一个tmp文件中
				k_kva := make(map[int][]KeyValue)
				for _, kv := range kva {
					k_kva[ihash(kv.Key)%reply.ReduceN] = append(k_kva[ihash(kv.Key)%reply.ReduceN], kv)
				}
				for id, kva := range k_kva {
					name := "mr-tmp-" + strconv.Itoa(reply.Task.MapId) + "-" + strconv.Itoa(id)
					ofile, _ := os.Create(name)
					enc := json.NewEncoder(ofile)
					if err := enc.Encode(kva); err != nil {
						log.Fatalf("cannont json and write %v \n", err)
					}
					ofile.Close()
				}
				// 上报任务结果
				RequestOK(&RpcArgs{
					TaskType: MapType,
					MapId:    reply.Task.MapId,
					ReduceId: reply.Task.ReduceId,
				})
			}
		} else if reply.Task.Type == ReduceType {
			intermediate := []KeyValue{}
			tmp := []KeyValue{}
			for _, filename := range reply.Task.Files {
				//log.Printf("reduceTask filenames: %v \n", filename)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v \n", filename)
					return
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot readall %v \n", filename)
					return
				}
				if err := json.Unmarshal(content, &tmp); err != nil {
					log.Println("filename is: ", filename)
					log.Println("content is: ", content)
					log.Fatalf("cannot json.Unmarshal %v %v \n", filename, err)
					return
				}
				intermediate = append(intermediate, tmp...)
			}
			// 排序用于接下来的计算
			sort.Sort(ByKey(intermediate))

			oname := "mr-out-" + strconv.Itoa(reply.Task.ReduceId)
			ofile, _ := os.Create(oname)
			defer ofile.Close()
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
			RequestOK(&RpcArgs{
				TaskType: ReduceType,
				MapId:    reply.Task.MapId,
				ReduceId: reply.Task.ReduceId,
			})
		}
	}
}

func RequestForTask() *RpcReply {
	args := RpcArgs{}
	reply := &RpcReply{Task: &Task{}}
	call("Master.DistributeTask", &args, reply)
	return reply
}

func RequestOK(args *RpcArgs) *RpcReply {
	reply := &RpcReply{}
	call("Master.ReceiveTask", args, reply)
	return reply
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
