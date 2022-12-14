# MapReduce
lab1的任务为MapReduce。通过MapRecude模型，来统计多个文件中文本的单词数量。
具体来说，有多个文件，通过一个master将它们分发给多个worker来进行map任务，之后再由master将多个map任务的结果分发给多个worker进行reduce任务，达到最终的目标。

先来看看map和reduce的函数
map是将字符串映射为一个列表，列表中的元素为kv键值对，k为单词字符串，v为1代表数量
reduce就是计算列表的个数
```golang
//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func Map(filename string, contents string) []mr.KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}
```

## master
master仅有一个，总体流程为先初始化所有任务信息，之后根据worker的任务请求分发任务并记录元数据，根据worker的任务完成请求改变元数据状态，当所有任务都完成后便终止运行。

首先是master的启动
```golang
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeMaster(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
```

在MakeMaster中，我们初始化了MapTasks和ReduceTasks，用来记录任务的个数和对应的文件名
```golang
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
```

当map任务和reduce任务都完成后，master便会终止运行
```golang
func (m *Master) Done() bool {
	ret := false
	if m.MapTaskN == 0 && m.ReduceTaskN == 0 {
		ret = true
	}
	return ret
}
```

因为不能无限等待worker去执行任务，所以我们开一个协程，监控元数据，定时清理过期的任务，以便重新分配给其他worker
```golang
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
```

rpc方法，给worker分发对应的Task，此处只有当map任务都完成后才会分发reduce任务
```golang
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
```

rpc方法，根据worker发来的请求，调整元数据任务的状态
```golang
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
```

## worker
worker可以有多个。worker不停地向master请求任务，并在任务完成后请求master告诉任务完成的情况。当所有任务都完成后或者master终止运行后，worker便也终止运行。

先是worker的启动
```golang
func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	mr.Worker(mapf, reducef)
}
```

worker循环向master申请任务
```golang
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
```