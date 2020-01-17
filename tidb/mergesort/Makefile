.PHONY: all

all: test bench

test:
	go test

bench:
	go test -bench Benchmark -run xx -count 5 -benchmem

bh:
	go test -bench Benchmark -run xx -count 5 -benchmem -cpuprofile=cpu.profile -blockprofile=block.profile -memprofile=mem.profile

memp:
	go tool pprof -http=:8080 ./talent.test ./mem.profile
cpup:
	go tool pprof -http=:8080 ./talent.test ./cpu.profile
blockp:
	go tool pprof -http=:8080 ./talent.test ./block.profile