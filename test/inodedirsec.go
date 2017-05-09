package main

import (
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	pb "hadoop_hdfs_fsimage"
	"log"
	"os"
)

func ParseInodeDirectorySection(info *pb.FileSummary_Section, imageFile *os.File, ch chan ParentChildren) {
	var (
		parent   uint64                             = 0
		children []uint64                           = []uint64{}
		dirEntry *pb.INodeDirectorySection_DirEntry = &pb.INodeDirectorySection_DirEntry{}
	)
	length := info.GetLength()
	dirSectionBytes, err := readFile(imageFile, int64(info.GetOffset()), int64(length))
	logIfErr(err)
	for a := length; a > 0; {
		i, c := binary.Uvarint(dirSectionBytes)
		if c <= 0 {
			log.Fatal("buf too small(0) or overflows(-1)")
		}
		newPos := uint64(c) + i
		tmpBuf := dirSectionBytes[c:newPos]
		if err = proto.Unmarshal(tmpBuf, dirEntry); err != nil {
			log.Fatal(err)
		}
		parent = dirEntry.GetParent()
		children = dirEntry.GetChildren()
		ch <- ParentChildren{parent, children}
		a -= newPos
		dirSectionBytes = dirSectionBytes[newPos:]
	}
	close(ch)
}
