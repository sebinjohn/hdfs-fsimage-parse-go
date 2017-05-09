package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	pb "hadoop_hdfs_fsimage"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

func logIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

const FILE_SUM_BYTES = 4

func main() {
	fileName := os.Args[1]
	fInfo, err := os.Stat(fileName)
	logIfErr(err)

	f, err := os.Open(fileName)
	logIfErr(err)
	fileLength := fInfo.Size()
	fSummaryLength := decodeFileSummaryLength(fileLength, f)
	sectionMap := parseFileSummary(f, fileLength, fSummaryLength)
	inodeSectionInfo := sectionMap["INODE"]
	ch := make(chan INode, 100000)
	go ParseInodeSection(inodeSectionInfo, f, ch)
	i := 0
	inodesMap := make(map[uint64]INode)
	for inode := range ch {
		inodesMap[inode.Id] = inode
	}

	fmt.Println("Total INodes: ", len(inodesMap))

	inodeDirSectionInfo := sectionMap["INODE_DIR"]
	chDirSec := make(chan ParentChildren, 1000)
	go ParseInodeDirectorySection(inodeDirSectionInfo, f, chDirSec)
	i = 0
	time.Sleep(1 * time.Millisecond)
	parChildMap := make(map[uint64][]uint64)
	if err != nil {
		log.Fatal(nil)
	}

	for pc := range chDirSec {
		parChildMap[pc.Parent] = pc.Children
	}
	fmt.Println("Total Directories: ", i)
	paths := make(chan string, 10000)
	go findPath(parChildMap, inodesMap, paths)
	a, err := os.Create("file.txt")
	w := bufio.NewWriterSize(a, 1000000)
	for path := range paths {
		fmt.Fprintln(w, path)
	}
	w.Flush()
	a.Close()
}

func parseFileSummary(imageFile *os.File, fileLength int64, fSummaryLength int32) map[string]*pb.FileSummary_Section {
	var (
		sectionMap                  = make(map[string]*pb.FileSummary_Section)
		fileSummary *pb.FileSummary = &pb.FileSummary{}
	)
	// last 4 bytes says how many bytes should be read from end to get the FileSummary message
	fSummaryLength64 := int64(fSummaryLength)
	readAt := fileLength - fSummaryLength64 - FILE_SUM_BYTES

	fSummaryBytes := make([]byte, fSummaryLength)
	_, err := imageFile.ReadAt(fSummaryBytes, readAt)
	if err != nil {
		if err != io.EOF {
			log.Fatal(err)
		}
	}

	_, c := binary.Uvarint(fSummaryBytes)
	if c <= 0 {
		log.Fatal("buf too small(0) or overflows(-1): ", c)
	}

	fSummaryBytes = fSummaryBytes[c:]
	if err = proto.Unmarshal(fSummaryBytes, fileSummary); err != nil {
		log.Fatal(err)
	}

	for _, value := range fileSummary.GetSections() {
		sectionMap[value.GetName()] = value
	}
	return sectionMap
}
func decodeFileSummaryLength(fileLength int64, imageFile *os.File) int32 {
	var (
		fSumLenBytes   = make([]byte, FILE_SUM_BYTES)
		fSummaryLength int32
	)
	fileSummaryLengthStart := fileLength - FILE_SUM_BYTES
	bReader := bytes.NewReader(fSumLenBytes)
	_, err := imageFile.ReadAt(fSumLenBytes, fileSummaryLengthStart)
	if err != nil {
		if err != io.EOF {
			log.Fatal(err)
		}
	}
	if err = binary.Read(bReader, binary.BigEndian, &fSummaryLength); err != nil {
		log.Fatal(err)
	}
	return fSummaryLength
}
func findPath(pm map[uint64][]uint64, names map[uint64]INode, ch chan string) {
	rootId := uint64(16385)
	children, ok := pm[rootId]
	if !ok {
		log.Fatal("No children to /")
	}
	var wg sync.WaitGroup
	for _, c := range children {
		wg.Add(1)
		go func(child uint64) {
			defer wg.Done()
			findSubPath("", pm, names, child, ch)
		}(c)
	}
	wg.Wait()
	fmt.Println("All go routines are over. closing the channel")
	close(ch)
	fmt.Println("Path Counter: ", pathCounter)
}

var pathCounter int = 0

func findSubPath(constructedPath string, parChildrenMap map[uint64][]uint64, inodeNames map[uint64]INode,
	curInodeId uint64, ch chan string) {

	var children []uint64
	children, ok1 := parChildrenMap[curInodeId]
	inode, _ := inodeNames[curInodeId]
	numberOfChildren := len(children)

	if !ok1 || numberOfChildren == 0 {
		ch <- constructedPath + "/" + string(inode.Name)
		pathCounter++
		return
	}

	for _, child := range children {
		findSubPath(constructedPath+"/"+string(inode.Name), parChildrenMap, inodeNames, child, ch)
	}
	return
}
