package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	pb "hadoop_hdfs_fsimage"
	"io"
	"log"
	"os"
)

func logIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	fileName := os.Args[1]
	fInfo, err := os.Stat(fileName)
	logIfErr(err)

	f, err := os.Open(fileName)
	logIfErr(err)

	// create a slice of 4 bytes long from the end

	fileLength := fInfo.Size()
	fileSummaryLengthStart := fileLength - 4
	var x = make([]byte, 4)
	_, err = f.ReadAt(x, fileSummaryLengthStart)
	if err != nil {
		if err != io.EOF {
			log.Fatal(err)
		}
	}

	var fSummaryLength int32
	bReader := bytes.NewReader(x)
	err = binary.Read(bReader, binary.BigEndian, &fSummaryLength)
	logIfErr(err)

	fSummaryLength64 := int64(fSummaryLength)
	readAt := fileLength - fSummaryLength64 - 4
	var fSummaryBytes = make([]byte, fSummaryLength)
	_, err = f.ReadAt(fSummaryBytes, readAt)
	if err != nil {
		if err != io.EOF {
			log.Fatal(err)
		}
	}

	fileSummary := &pb.FileSummary{}
	_, c := binary.Uvarint(fSummaryBytes)
	if c <= 0 {
		log.Fatal("buf too small(0) or overflows(-1): ", c)
	}
	fSummaryBytes = fSummaryBytes[c:]
	if err = proto.Unmarshal(fSummaryBytes, fileSummary); err != nil {
		log.Fatal(err)
	}

	var sectionMap = make(map[string]*pb.FileSummary_Section)

	for _, value := range fileSummary.GetSections() {
		sectionMap[value.GetName()] = value
	}

	inodeSectionInfo := sectionMap["INODE"]
	parseInodeSection(inodeSectionInfo, f)

	inodeDirectorySectionInfo := sectionMap["INODE_DIR"]
	parseInodeDirectorySection(inodeDirectorySectionInfo, f)

	fmt.Println("Parse further")
	fmt.Println(sectionMap)
}

func parseInodeSection(info *pb.FileSummary_Section, imageFile *os.File) {
	var inodeSectionBytes = make([]byte, info.GetLength())
	_, err := imageFile.ReadAt(inodeSectionBytes, int64(info.GetOffset()))
	logIfErr(err)

	i, c := binary.Uvarint(inodeSectionBytes)
	if c <= 0 {
		log.Fatal("buf too small(0) or overflows(-1): ", c)
	}
	newPos := uint64(c) + i
	tmpBuf := inodeSectionBytes[c:newPos]

	inodeSection := &pb.INodeSection{}
	if err = proto.Unmarshal(tmpBuf, inodeSection); err != nil {
		log.Fatal(err)
	}
	totalInodes := inodeSection.GetNumInodes()

	var names = make([]string, totalInodes)
	for a := uint64(0); a < totalInodes; a++ {
		inodeSectionBytes = inodeSectionBytes[newPos:]
		i, c = binary.Uvarint(inodeSectionBytes)
		if c <= 0 {
			log.Fatal("buf too small(0) or overflows(-1): ", c, a)
		}
		newPos = uint64(c) + i
		tmpBuf = inodeSectionBytes[c:newPos]
		inode := &pb.INodeSection_INode{}
		if err = proto.Unmarshal(tmpBuf, inode); err != nil {
			log.Fatal(err)
		}
		names[a] = string(inode.GetName())
	}
	count := 0
	for _, v := range names {
		if v == "part-m-00000" {
			count++
		}
	}
	fmt.Println("count of part-m-00000: ", count)
	fmt.Println("Last 10 names", names[totalInodes-10:])
}

func parseInodeDirectorySection(info *pb.FileSummary_Section, imageFile *os.File) {
	startPos := int64(info.GetOffset())
	length := info.GetLength()
	dirSectionBytes := make([]byte, length)
	// inode directory section has repeated directory entry messages
	_, err := imageFile.ReadAt(dirSectionBytes, startPos)
	logIfErr(err)
	childParent := make(map[uint64]uint64)
	dirEntry := &pb.INodeDirectorySection_DirEntry{}
	for a := uint64(length); a > 0; {
		i, c := binary.Uvarint(dirSectionBytes)
		if c <= 0 {
			log.Fatal("buf too small(0) or overflows(-1)")
		}
		newPos := uint64(c) + i
		tmpBuf := dirSectionBytes[c:newPos]
		if err = proto.Unmarshal(tmpBuf, dirEntry); err != nil {
			log.Fatal(err)
		}
		parent := dirEntry.GetParent()
		children := dirEntry.GetChildren()
		lengthChildren := len(children)
		for j := 0; j < lengthChildren; j++ {
			childParent[children[j]] = parent
		}
		a -= newPos
		dirSectionBytes = dirSectionBytes[newPos:]
	}
	fmt.Println("Number of nodes:", len(childParent))
}
