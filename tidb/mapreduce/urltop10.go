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
	// mapF进行统计
	// reduceF进行排序
	args = append(args, RoundArgs{
		MapFunc:    MyURLTop10Map,
		ReduceFunc: MyTop10Reduce,
		NReduce:    1,
	})
	return args
}
//MyURLTop10Map 直接统计url出现的次数
func MyURLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0, len(lines))
	url2cn := make(map[string]int, len(lines))
	for _, l := range lines {
		if len(l) == 0 {
			continue
		}
		url2cn[l]++
	}
	for k, v := range url2cn {
		kvs = append(kvs, KeyValue{Value:fmt.Sprintf("%s %s", k, strconv.Itoa(v))})
	}
	return kvs
}

//MyTop10Reduce 对输入进行排序（注意cnts的初始化需要累加<区别urltop10_example.go>）
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
		cnts[tmp[0]] += n
	}

	us, cs := TopN(cnts, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}