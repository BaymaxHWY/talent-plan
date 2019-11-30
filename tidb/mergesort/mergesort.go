package main

import (
	"fmt"
	"math"
	"sort"
)

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.

// 方便从bigData[dataIndex]中选择候补接替被选上的minData[dataIndex]的值
type MinDataMap struct {
	num       int64
	dataIndex int
}

type Int64 []int64

func MergeSort(src []int64) {
	// 多路归并过程
	// 1. 将大量数据分为许多小块(例如16M个整数的数组将其划分为16个容量为1M的数组)
	// 2. 对每一个小容量数组进行Quick Sort（使用多个goroutine并发执行）
	// 3. 取每一个小容量数组的第一个（即该数组中的最小值）构建一个minData数组
	// 4. 从minData中选取最小值填入输出数组src
	// 5. 被选出的最小值那个位置由其所属的小容量数组中的后一位补上
	// 6. 循环4、5步，直到输出所有小容量数组到src中
	var (
		bigData []Int64      // 存储已经排序好的小容量数组
		minData []MinDataMap // 存储每个小容量数组的最小值和其对应小容量数组的索引
		n       int
		N       int // 小容量数组的大小
	)
	N = 1 << 21
	if N >= len(src) {
		n = 1
	}
	if len(src)%N == 0 {
		n = len(src) / N
	} else {
		n = len(src)/N + 1
	}

	reply := make(chan []int64, n)
	inputData := make(chan []int64, n)
	for i := 0; i < n; i++ {
		go InternalSort(inputData, reply)
	}
	// 分割大容量数组src
	k := 0
	s := k * N
	e := k*N + N
	for ; k < n; k++ {
		if s >= len(src) {
			break
		}
		if e > len(src) {
			inputData <- src[s:]
			k += 1
			break
		} else {
			inputData <- src[s:e]
		}
		s = e
		e = e + N
	}
Loop:
	for {
		select {
		case r := <-reply: // 接收已排序小容量数组
			tem := append([]int64{}, r...)
			minData = append(minData, MinDataMap{
				num:       r[0],
				dataIndex: len(bigData),
			})
			bigData = append(bigData, tem[1:])
			k--
			if k <= 0 {
				break Loop
			}
		}
	}
	// 外部排序方法1：时间太长
	// 1. 从minData中选择一个最小的加入src 这一步耗时太多
	// 2. 同时把其同属bigData的后一位填补
	// 3. 再循环1、2
	// 外部排序方法2：
	// 将方法1的minData改为小根堆
	buildHeap(minData)
	for srcIndex := 0; srcIndex < len(src); srcIndex++ {
		src[srcIndex] = minData[0].num
		if len(bigData[minData[0].dataIndex]) <= 0 { // 当某一小容量数组已经被选完了就将其对应的minData"沉底"
			minData[0].num = math.MaxInt64
		} else {
			minData[0].num = bigData[minData[0].dataIndex][0]
			bigData[minData[0].dataIndex] = bigData[minData[0].dataIndex][1:]
		}
		heapfiy(minData, 0) // 调整小根堆
	}
}

// 内部排序
func InternalSort(inputData chan []int64, reply chan []int64) {
	for {
		select {
		case data := <-inputData:
			sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })
			reply <- data
		}
	}
}

// 堆排序
// 因为堆是一棵完全二叉树，所以可以用数组表述
// 设节点的下标为i，其父节点下标为(i-1)/2，其左、右孩子下标分别为：2*i + 1,2*i + 2

// 对任意数组src将其构建为大根堆
func buildHeap(src []MinDataMap) {
	lastParent := (len(src) - 2) / 2
	for i := lastParent; i >= 0; i-- {
		heapfiy(src, i)
	}
}

// 从给点节点i开始向下调整堆src，小根堆
func heapfiy(src []MinDataMap, i int) {
	if i >= len(src) { // 递归出口
		return
	}
	left := 2*i + 1
	right := 2*i + 2
	minI := i
	if left < len(src) && src[minI].num > src[left].num { // 预防left、right越界
		minI = left
	}
	if right < len(src) && src[minI].num > src[right].num {
		minI = right
	}
	if i != minI {
		src[i], src[minI] = src[minI], src[i]
		heapfiy(src, minI)
	}
}

func main() {
	src := []int64{3552821701521292051, 7121270439079161434, 3302184880548658924, 369070239755460047, 5909703508719890330, 8063920649244728675, 77046509556037532, 6425660779179500320, 907779747608496557, 3035163545255756336, 8545762606340925243, 7678724819980687822, 5622234311827137131, 3330316506258160137, 5302026568330972670, 4901795240116280495, 4352213064668526371}
	//src := []int64 {1, 7, 9, 4, 3, 66, 44, 100, 22, 234}
	expect := make([]int64, len(src))
	copy(expect, src)
	MergeSort(src)
	fmt.Println("src1:", src)
	sort.Slice(expect, func(i, j int) bool {
		return expect[i] < expect[j]
	})
	fmt.Println("src2:", expect)
}
