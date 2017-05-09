package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	pb "hadoop_hdfs_fsimage"
	"io"
	"log"
	"os"
	"sync"
)

func logIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

const FILE_SUM_BYTES = 4

func main() {
	imagePathPtr := flag.String("image", "", "path to the fsimage file")
	outputPathPtr := flag.String("out", "", "output path")
	flag.Parse()
	fileName := *imagePathPtr
	outputPath := *outputPathPtr
	if fileName == "" || outputPath == "" {
		fmt.Println("Usage: ./main -image=<path to image> -out=<output path>")
		log.Fatal("")
	}

	fInfo, err := os.Stat(fileName)
	logIfErr(err)

	f, err := os.Open(fileName)
	logIfErr(err)
	fileLength := fInfo.Size()
	fSummaryLength := decodeFileSummaryLength(fileLength, f)
	sectionMap := parseFileSummary(f, fileLength, fSummaryLength)
	inodeSectionInfo := sectionMap["INODE"]
	inodeCh := make(chan INode, 100000)
	go ParseInodeSection(inodeSectionInfo, f, inodeCh)
	inodesMap := make(map[uint64]INode)
	for inode := range inodeCh {
		inodesMap[inode.Id] = inode
	}
	fmt.Println("Total INodes: ", len(inodesMap))

	inodeDirSectionInfo := sectionMap["INODE_DIR"]
	chDirSec := make(chan ParentChildren, 1000)
	go ParseInodeDirectorySection(inodeDirSectionInfo, f, chDirSec)
	parChildMap := make(map[uint64][]uint64)
	if err != nil {
		log.Fatal(nil)
	}
	for pc := range chDirSec {
		parChildMap[pc.Parent] = pc.Children
	}

	pathsChan := make(chan string, 10000)
	go findPath(parChildMap, inodesMap, pathsChan)
	a, err := os.Create(os.Args[2])
	w := bufio.NewWriterSize(a, 1000000)
	for path := range pathsChan {
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
	var (
		rootId uint64 = 16385
		wg     sync.WaitGroup
	)
	children, ok := pm[rootId]
	if !ok {
		ch <- "/"
		close(ch)
		return
	} else {
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
	}
}

func findSubPath(
	constructedPath string,
	parChildrenMap map[uint64][]uint64,
	inodeNames map[uint64]INode,
	curInodeId uint64,
	ch chan string) {

	children, ok1 := parChildrenMap[curInodeId]
	inode, _ := inodeNames[curInodeId]
	p := constructedPath + "/" + string(inode.Name)
	if !ok1 || len(children) == 0 {
		ch <- p
		return
	}
	for _, child := range children {
		findSubPath(p, parChildrenMap, inodeNames, child, ch)
	}
}
