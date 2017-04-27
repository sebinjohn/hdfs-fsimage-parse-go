# hdfs-fsimage-parse-go
A Golang port of https://github.com/sebinjohn/hdfs-fsimage-parse

## First Steps

```
$ cd hadoop_protocols

$ mkdir ${GOPATH}/{hadoop_common,hadoop_hdfs,hadoop_hdfs_fsimage}

$ protoc \
	--go_out=${GOPATH}/src/hadoop_common \
	-I$(pwd)/common common/Security.proto

$ protoc \
	--go_out=Mhdfs.proto=hadoop_hdfs,MSecurity.proto=hadoop_common:${GOPATH}/src/hadoop_hdfs \
	-I${GOPATH}src/hadoop_common \
	-I$(pwd) hdfs/*.proto

$ protoc \
	--go_out=Mhdfs.proto=hadoop_hdfs,Macl.proto=hadoop_hdfs,Mxattr.proto=hadoop_hdfs:${GOPATH}/src/hadoop_hdfs_fsimage \
	-I${GOPATH}/hadoop_common \
	-I$(pwd) hadoop/hdfs/fsimage/fsimage.proto
```

## Run

`go run *.go <path to hdfs fsimage>`
