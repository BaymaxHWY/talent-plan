package main

import (
	"runtime"
	"sort"
	"sync"
)

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.


// 思路：
// 1. 将src分割为多份进行内部排序
// 2. 将src的排序结果再进行外部排序（利用堆排序）


type minData struct { // 存储一份已经拍好序的数组
	data []int64
	inx int // 记录下标
}

func MergeSort(src []int64) {
	conc := runtime.NumCPU() // 并发的数量
	if len(src) < conc {
		conc = len(src) // 最多并发不超过数组长度
	}
	var wg sync.WaitGroup
	wg.Add(conc)
	size := len(src)/conc // 每次内部排序的大小

	for i := 0; i < conc; i++ {
		go func(i int) {
			offset := i * size // 本次排序的起点
			var tmp []int64
			if i == conc - 1 { // 最后一个必须包含剩下所有的
				tmp = src[offset:]
			}else {
				tmp = src[offset:offset + size]
			}
			sort.Slice(tmp, func(a, b int) bool {return tmp[a] < tmp[b]})
			wg.Done()
		}(i)
	}
	wg.Wait() // 注意阻塞
	// 准备数据
	result := make([]minData, conc)
	for i := 0; i < conc; i++ {
		offset := i * size
		var tmp []int64
		if i == conc - 1 { // 最后一个必须包含剩下所有的
			tmp = src[offset:]
		}else {
			tmp = src[offset:offset + size]
		}
		result[i].data = make([]int64, len(tmp))
		copy(result[i].data, tmp)
	}
	// 外部排序
	heap := make([]int, len(result))
	for i, _ := range result {
		heap[i] = i
	}
	resultOut := make([]int64, 0, len(src))
	heapBuild(result, heap) // 建堆
	for len(heap) > 0 {
		// 取堆顶元素
		idx_res := heap[0]
		// 加入结果数组
		resultOut = append(resultOut, result[idx_res].data[result[idx_res].inx])
		// 是否还有后续新元素加入heap
		if result[idx_res].inx < len(result[idx_res].data)-1 {
			result[idx_res].inx += 1 // 后移
			heap = append(heap, idx_res) // 再次加入
		}
		// 如何取出堆顶元素：1.将尾元素与堆顶元素进行交换 2.再取出尾元素（原堆顶），再进行heapfit
		heap[0], heap[len(heap)-1] = heap[len(heap)-1], heap[0]
		heap = heap[:len(heap)-1]
		heapFit(result, heap, 0)
	}
	copy(src, resultOut)
}

// 建立好堆后返回result的下标
func heapBuild(source []minData, heap []int) {
	parent := (len(heap) - 1) / 2 // 起始点
	for ; parent >= 0; parent-- {
		heapFit(source, heap, parent)
	}
}
// 调整堆，小跟堆
func heapFit(source []minData, heap []int, parent int) {
	leftChild := parent * 2 + 1
	rightChild := parent * 2 + 2
	max := parent
	if leftChild < len(heap) {
		mIndex := source[heap[max]].inx
		mData := source[heap[max]].data
		lIndex := source[heap[leftChild]].inx
		lData := source[heap[leftChild]].data

		if mData[mIndex] > lData[lIndex] {
			max = leftChild
		}
	}
	if rightChild < len(heap) {
		mIndex := source[heap[max]].inx
		mData := source[heap[max]].data
		rIndex := source[heap[rightChild]].inx
		rData := source[heap[rightChild]].data

		if mData[mIndex] > rData[rIndex] {
			max = rightChild
		}
	}
	if max != parent {
		heap[max], heap[parent] = heap[parent], heap[max]
		heapFit(source, heap, max)
	}
}