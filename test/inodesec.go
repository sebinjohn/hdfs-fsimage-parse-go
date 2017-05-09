package main

import (
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	pb "hadoop_hdfs_fsimage"
	"log"
	"os"
)

func getTotalInodes(bytes []byte) (uint64, uint64) {
	i, c := binary.Uvarint(bytes)
	if c <= 0 {
		log.Fatal("buf too small(0) or overflows(-1): ", c)
	}
	newPos := uint64(c) + i
	tmpBuf := bytes[c:newPos]

	inodeSection := &pb.INodeSection{}
	if err := proto.Unmarshal(tmpBuf, inodeSection); err != nil {
		log.Fatal(err)
	}
	totalInodes := inodeSection.GetNumInodes()
	return totalInodes, newPos
}

func ParseInodeSection(info *pb.FileSummary_Section, imageFile *os.File, ch chan INode) {
	var (
		inodeType pb.INodeSection_INode_Type
	)
	inodeSectionBytes, err := readFile(imageFile, int64(info.GetOffset()), int64(info.GetLength()))
	logIfErr(err)
	totalInodes, newPos := getTotalInodes(inodeSectionBytes)

	for a := uint64(0); a < totalInodes; a++ {
		inodeSectionBytes = inodeSectionBytes[newPos:]
		i, c := binary.Uvarint(inodeSectionBytes)
		if c <= 0 {
			log.Fatal("buf too small(0) or overflows(-1): ", c, a)
		}
		newPos = uint64(c) + i
		tmpBuf := inodeSectionBytes[c:newPos]
		inode := &pb.INodeSection_INode{}
		if err = proto.Unmarshal(tmpBuf, inode); err != nil {
			log.Fatal(err)
		}
		inodeType = inode.GetType()
		id := inode.GetId()
		inodeS := INode{}
		if inodeType == 1 {
			inodeS = INode{inode.GetName(), id, FILE}
		} else if inodeType == 2 {
			inodeS = INode{inode.GetName(), id, DIRECTORY}
		} else {
			inodeS = INode{inode.GetName(), id, SYMLINK}
		}
		ch <- inodeS
	}
	close(ch)
}
