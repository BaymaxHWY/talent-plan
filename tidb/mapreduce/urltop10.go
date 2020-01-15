package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// URLTop10 .
func URLTop10(nWorkers int) RoundsArgs {
	// YOUR CODE HERE :)
	// And don't forget to document your idea.
	var args RoundsArgs
	// round 1: 对url进行统计
	args = append(args, RoundArgs{
		MapFunc:    MyURLCountMap,
		ReduceFunc: MyURLCountReduce,
		NReduce:    nWorkers,
	})
	// round 2: 直接在map阶段就进行排序、每个文件选出最多的10个
	args = append(args, RoundArgs{
		MapFunc:    MyURLTop10Map,
		ReduceFunc: MyTop10Reduce,
		NReduce:    1,
	})
	return args
}

//MyURLCountMap 第一轮mapF只对文件按行进行分割
func MyURLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(string(contents), "\n")
	kvs := make([]KeyValue, 0, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		kvs = append(kvs, KeyValue{Key: l})
	}
	return kvs
}

//MyURLCountReduce 第一轮reduceF对每个url进行统计
func MyURLCountReduce(key string, values []string) string {
	return fmt.Sprintf("%s %s\n", key, strconv.Itoa(len(values)))
}

//MyURLTop10Map 第二轮mapF选出统计数前10的url（相比example是并发的进行sort排序）
func MyURLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0, len(lines))
	url2cn := make(map[string]int, len(lines))
	for _, l := range lines {
		tmp := strings.Split(l, " ")
		if len(tmp) < 2 {
			continue
		}
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		url2cn[tmp[0]] = n
	}
	urllist, cntlist := TopN(url2cn, 10)
	for i := range urllist {
		kvs = append(kvs, KeyValue{
			Key:   "",
			Value: fmt.Sprintf("%s %s", urllist[i], strconv.Itoa(cntlist[i])),
		})
	}
	return kvs
}

//MyTop10Reduce 第二轮reduceF，因为输入已经是拍好序的
func MyTop10Reduce(key string, values []string) string {
	cnts := make(map[string]int, len(values))
	for _, v := range values {
		v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] = n
	}

	us, cs := TopN(cnts, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}