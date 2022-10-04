package mr

import (
	"io/ioutil"
	"log"
	"regexp"
	"strconv"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

type Master struct {
	// 记录需要map的mapId，0表示未完成，1表示已完成
	MapTasks map[int]int
	// mapId和文件名的映射
	MapIdDic map[int]string
	// 记录需要reduce的桶编号，0表示未完成，1表示已完成
	ReduceTasks map[int]int
	// 需要执行的map的数量
	MapTaskN int
	// 需要执行的reduce的数量
	ReduceTaskN int

	// 记录任务的元数据
	TypeInfoList []*TypeInfo

	// 记录总MapTask数
	MapN int
	// 记录总ReduceTask数
	ReduceN int

	MapLock      sync.Mutex
	ReduceLock   sync.Mutex
	TypeInfoLock sync.Mutex
}

const (
	Done = iota
	NotDone
	Doing
)

type TypeInfo struct {
	Type     int
	MapId    int
	ReduceId int
	// 任务开始时间
	StartTime time.Time
	// 任务deadline
	EndTime time.Time
}

type Task struct {
	Type     int
	Files    []string
	MapId    int
	ReduceId int
}

const (
	Running = iota
	MapType
	ReduceType
)

func NewMapTask() *Task {
	return &Task{
		Type:     MapType,
		Files:    make([]string, 0),
		MapId:    0,
		ReduceId: 0,
	}
}

func NewReduceTask() *Task {
	return &Task{
		Type:     ReduceType,
		Files:    make([]string, 0),
		MapId:    0,
		ReduceId: 0,
	}
}

// 给worker分发对应的Task
func (m *Master) DistributeTask(args *RpcArgs, reply *RpcReply) error {
	if m.MapTaskN > 0 {
		m.MapLock.Lock()
		defer m.MapLock.Unlock()
		if m.MapTaskN > 0 {
			for mapId, status := range m.MapTasks {
				if status == NotDone {
					reply.Task = NewMapTask()
					reply.Task.MapId = mapId
					reply.Task.Files = append(reply.Task.Files, m.MapIdDic[mapId])
					reply.ReduceN = m.ReduceN
					// ReduceId由kv值的ihash(k)%reduceTaskN决定
					m.MapTasks[mapId] = Doing
					// 记录任务元数据
					typeInfo := &TypeInfo{
						Type:      reply.Task.Type,
						MapId:     reply.Task.MapId,
						ReduceId:  reply.Task.ReduceId,
						StartTime: time.Now(),
						EndTime:   time.Now().Add(time.Second * 10),
					}
					m.TypeInfoLock.Lock()
					m.TypeInfoList = append(m.TypeInfoList, typeInfo)
					m.TypeInfoLock.Unlock()
					return nil
				}
			}
			return nil
		}
	}
	if m.ReduceTaskN > 0 {
		m.ReduceLock.Lock()
		defer m.ReduceLock.Unlock()
		if m.ReduceTaskN > 0 {
			for reduceId, status := range m.ReduceTasks {
				if status == NotDone {
					reply.Task = NewReduceTask()
					reply.Task.ReduceId = reduceId
					reply.ReduceN = m.ReduceN
					// 扫描本地对应的reduce文件名，添加到string切片中
					if files, err := ioutil.ReadDir("./"); err != nil {
						log.Fatal("cannot readDir %v", err)
					} else {
						for _, file := range files {
							// 正则表达式
							if match, _ := regexp.MatchString("mr-tmp-.*-"+strconv.Itoa(reduceId), file.Name()); match == true {
								reply.Task.Files = append(reply.Task.Files, file.Name())
							}
						}
					}
					m.ReduceTasks[reduceId] = Doing
					// 记录任务元数据
					typeInfo := &TypeInfo{
						Type:      reply.Task.Type,
						MapId:     reply.Task.MapId,
						ReduceId:  reply.Task.ReduceId,
						StartTime: time.Now(),
						EndTime:   time.Now().Add(time.Second * 10),
					}
					m.TypeInfoLock.Lock()
					m.TypeInfoList = append(m.TypeInfoList, typeInfo)
					m.TypeInfoLock.Unlock()
					//log.Printf("give one reducetask reduceId %v", reply.Task.ReduceId)
					return nil
				}
			}
			return nil
		}
	}
	reply.Task = &Task{
		Type: -1,
	}
	return nil
}

// 根据上报的Task，调整其状态
func (m *Master) ReceiveTask(args *RpcArgs, reply *RpcReply) error {
	taskType := args.TaskType
	if taskType == MapType && m.MapTaskN > 0 {
		m.MapLock.Lock()
		defer m.MapLock.Unlock()
		if m.MapTasks[args.MapId] == Doing {
			m.MapTasks[args.MapId] = Done
			m.MapTaskN--
			if m.MapTaskN == 0 {
				//log.Println("mapTask all done")
			}
		}
		return nil
	}
	if taskType == ReduceType && m.ReduceTaskN > 0 {
		m.ReduceLock.Lock()
		defer m.ReduceLock.Unlock()
		if m.ReduceTasks[args.ReduceId] == Doing {
			m.ReduceTasks[args.ReduceId] = Done
			m.ReduceTaskN--
			if m.ReduceTaskN == 0 {
				//log.Println("reduceTask all done")
			}
		}
		return nil
	}
	return nil
}

// 监控元数据切片，将过期的任务从元数据切片中删除，并将任务状态从Doing改为NotDone
func (m *Master) monitorMetaData() error {
	for {
		idx := 0
		m.TypeInfoLock.Lock()
		for _, typeInfo := range m.TypeInfoList {
			// 删除任务过期的任务
			if typeInfo.EndTime.Unix() > time.Now().Unix() {
				m.TypeInfoList[idx] = typeInfo
				idx++
			} else {
				// 将任务状态从Doing改为NotDone
				if typeInfo.Type == MapType {
					m.MapTasks[typeInfo.MapId] = NotDone
				} else if typeInfo.Type == ReduceType {
					m.ReduceTasks[typeInfo.ReduceId] = NotDone
				}
			}
		}
		// 长度缩小为有效长度
		m.TypeInfoList = m.TypeInfoList[:idx]
		m.TypeInfoLock.Unlock()
		time.Sleep(time.Second * 5)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "127.0.0.1:1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	go m.monitorMetaData()
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	if m.MapTaskN == 0 && m.ReduceTaskN == 0 {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		MapTasks:    make(map[int]int),
		MapIdDic:    make(map[int]string),
		ReduceTasks: make(map[int]int),
		MapTaskN:    len(files),
		ReduceTaskN: nReduce,
		MapN:        len(files),
		ReduceN:     nReduce,
	}
	// 初始化MapTasks和MapIdDic
	for idx, fileName := range files {
		m.MapTasks[idx] = NotDone
		m.MapIdDic[idx] = fileName
		//log.Printf("add mapTask fileName: %v \n", fileName)
	}
	// 初始化ReduceTasks
	for idx := 0; idx < nReduce; idx++ {
		m.ReduceTasks[idx] = NotDone
	}
	//log.Println("already add all")
	m.server()
	return &m
}
