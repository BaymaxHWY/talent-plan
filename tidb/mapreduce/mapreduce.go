package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// ReduceF function from MIT 6.824 LAB1
type ReduceF func(key string, values []string) string

// MapF function from MIT 6.824 LAB1
type MapF func(filename string, contents string) []KeyValue

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "mapPhase"
	reducePhase          = "reducePhase"
)

type task struct {
	dataDir    string
	jobName    string
	mapFile    string   // only for map, the input file
	phase      jobPhase // are we in mapPhase or reducePhase?
	taskNumber int      // this task's index in the current phase
	nMap       int      // number of map tasks
	nReduce    int      // number of reduce tasks
	mapF       MapF     // map function used in this job
	reduceF    ReduceF  // reduce function used in this job
	wg         sync.WaitGroup
}

// MRCluster represents a map-reduce cluster.
type MRCluster struct {
	nWorkers int
	wg       sync.WaitGroup
	taskCh   chan *task
	exit     chan struct{}
}

var singleton = &MRCluster{
	nWorkers: runtime.NumCPU(),
	taskCh:   make(chan *task),
	exit:     make(chan struct{}),
}

func init() {
	singleton.Start()
}

// GetMRCluster returns a reference to a MRCluster.
func GetMRCluster() *MRCluster {
	return singleton
}

// NWorkers returns how many workers there are in this cluster.
func (c *MRCluster) NWorkers() int { return c.nWorkers }

// Start starts this cluster.
func (c *MRCluster) Start() {
	for i := 0; i < c.nWorkers; i++ {
		c.wg.Add(1)
		go c.worker()
	}
}

func (c *MRCluster) worker() {
	defer c.wg.Done()
	for {
		select {
		case t := <-c.taskCh:
			if t.phase == mapPhase {
				content, err := ioutil.ReadFile(t.mapFile)
				if err != nil {
					panic(err)
				}

				fs := make([]*os.File, t.nReduce)
				bs := make([]*bufio.Writer, t.nReduce)
				for i := range fs {
					rpath := reduceName(t.dataDir, t.jobName, t.taskNumber, i)
					fs[i], bs[i] = CreateFileAndBuf(rpath)
				}
				results := t.mapF(t.mapFile, string(content))
				for _, kv := range results {
					enc := json.NewEncoder(bs[ihash(kv.Key)%t.nReduce])
					if err := enc.Encode(&kv); err != nil {
						log.Fatalln(err)
					}
				}
				for i := range fs {
					SafeClose(fs[i], bs[i])
				}
			} else {
				// YOUR CODE HERE :)
				// hint: don't encode results returned by ReduceF, and just output
				// them into the destination file directly so that users can get
				// results formatted as what they want.

				// 1. 读取中间文件--需要获得文件名，再进行读取
				// 2. 对其中的kv键值对进行排序--sort库函数
				// 3. 对每个key调用reduceF--需要一个排序好了的valueslice
				// 4. 将reduceF的输出写入磁盘--已经给出了方法
				mapFiles := make([]*os.File, t.nMap)
				mapFileBuffers := make([]*bufio.Reader, t.nMap)
				contents := make([]*json.Decoder, t.nMap)
				// 获得文件句柄和decoder数组
				for i := 0; i < t.nMap; i++ {
					mapFilePath := reduceName(t.dataDir, t.jobName, i, t.taskNumber)
					mapFiles[i], mapFileBuffers[i] = OpenFileAndBuf(mapFilePath)
					contents[i] = json.NewDecoder(mapFileBuffers[i])
				}
				// 读取文件内容
				// 将相同的键值的value存储到一起
				kvList := make(map[string][]string)
				for i := 0; i < t.nMap; i++ {
					for { // 需要持读取文件
						var kv KeyValue
						err := contents[i].Decode(&kv)
						if err != nil {
							if err.Error() != "EOF" {
								fmt.Printf("Failed to read from REDUCE input file %s: %v\n", mapFiles[i].Name(), err)
							}
							break
						}
						kvList[kv.Key] = append(kvList[kv.Key], kv.Value)
					}
				}
				// 对 Key 进行排序，数据量太大（无法读入内存时，才进行排序）
				keys := make([]string, len(kvList))
				i := 0
				for k := range kvList {
					keys[i] = k
					i++
				}
				//sort.Strings(keys)
				outFile := mergeName(t.dataDir, t.jobName, t.taskNumber)
				out, outBuffer := CreateFileAndBuf(outFile)
				for k, v := range kvList {
					reduced := t.reduceF(k, v)
					WriteToBuf(outBuffer, reduced)
				}
				SafeClose(out, outBuffer)
			}
			t.wg.Done()
		case <-c.exit:
			return
		}
	}
}

// Shutdown shutdowns this cluster.
func (c *MRCluster) Shutdown() {
	close(c.exit)
	c.wg.Wait()
}

// Submit submits a job to this cluster.
func (c *MRCluster) Submit(jobName, dataDir string, mapF MapF, reduceF ReduceF, mapFiles []string, nReduce int) <-chan []string {
	notify := make(chan []string)
	go c.run(jobName, dataDir, mapF, reduceF, mapFiles, nReduce, notify)
	return notify
}

func (c *MRCluster) run(jobName, dataDir string, mapF MapF, reduceF ReduceF, mapFiles []string, nReduce int, notify chan<- []string) {
	// map phase
	nMap := len(mapFiles)
	tasks := make([]*task, 0, nMap)
	for i := 0; i < nMap; i++ {
		t := &task{
			dataDir:    dataDir,
			jobName:    jobName,
			mapFile:    mapFiles[i],
			phase:      mapPhase,
			taskNumber: i,
			nReduce:    nReduce,
			nMap:       nMap,
			mapF:       mapF,
		}
		t.wg.Add(1)
		tasks = append(tasks, t)
		go func() { c.taskCh <- t }()
	}
	for _, t := range tasks {
		t.wg.Wait()
	}

	// reduce phase
	// YOUR CODE HERE :D
	// 并发向worker发送reduce task
	tasks = make([]*task, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		t := &task{
			dataDir:    dataDir,
			jobName:    jobName,
			mapFile:    "",
			phase:      reducePhase,
			taskNumber: i,
			nMap:       nMap,
			nReduce:    nReduce,
			mapF:       nil,
			reduceF:    reduceF,
			wg:         sync.WaitGroup{},
		}
		t.wg.Add(1)
		tasks = append(tasks, t)
		go func() {c.taskCh <- t}() // 防止死锁
	}
	outFiles := make([]string, 0, nReduce)
	for _, t := range tasks {
		t.wg.Wait() // 等处理完
		file := mergeName(t.dataDir, t.jobName, t.taskNumber)
		outFiles = append(outFiles, file)
	}
	notify <- outFiles
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

func reduceName(dataDir, jobName string, mapTask int, reduceTask int) string {
	return path.Join(dataDir, "mrtmp."+jobName+"-"+strconv.Itoa(mapTask)+"-"+strconv.Itoa(reduceTask))
}

func mergeName(dataDir, jobName string, reduceTask int) string {
	return path.Join(dataDir, "mrtmp."+jobName+"-res-"+strconv.Itoa(reduceTask))
}
