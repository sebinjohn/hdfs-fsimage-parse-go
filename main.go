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

const (
	FILE_SUM_BYTES = 4
	ROOT_INODE_ID  = 16385
)

// Global Variables
var pathCounter int = 0

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

	fileLength := fInfo.Size()
	fSummaryLength := decodeFileSummaryLength(fileLength, f)
	sectionMap := parseFileSummary(f, fileLength, fSummaryLength)

	inodeSectionInfo := sectionMap["INODE"]
	inodeNames, entityCount := parseInodeSection(inodeSectionInfo, f)

	inodeDirectorySectionInfo := sectionMap["INODE_DIR"]
	parChildrenMap := parseInodeDirectorySection(inodeDirectorySectionInfo, f)
	fmt.Println("Root INode ID: ", ROOT_INODE_ID)
	fmt.Println("Total Number of Files: ", entityCount.Files)
	fmt.Println("Total Number of Directories: ", entityCount.Directories)
	fmt.Println("Total Number of Symlinks: ", entityCount.Symlinks)
	rootTreeNode := findChildren(parChildrenMap, inodeNames, ROOT_INODE_ID)
	CountTreeNodes(*rootTreeNode)
	paths := make([]string, pathCounter)
	var index int = 0
	for _, child := range rootTreeNode.Children {
		fmt.Println("Processing " + string(child.Name))
		constructPath("", child, &paths, &index)
	}
	p := 0
	for _, c := range paths {
		if len(c) != 0 {
			p++
		}
	}
	fmt.Println("No of Paths: ", p)
	fmt.Println("First 10 paths")
	fmt.Println(paths[:10])

	fmt.Println("Parse further")
	fmt.Println(sectionMap)
	f.Close()
}

func constructPath(constructedPath string, node *INodeTree, paths *[]string, index *int) {
	children := node.Children
	if children == nil || len(children) == 0 {
		(*paths)[*index] = constructedPath + "/" + string(node.Name)
		(*index)++
	} else {
		for _, child := range children {
			constructPath(constructedPath+"/"+string(node.Name), child, paths, index)
		}
	}
}

func findChildren(parChildrenMap map[ParentId][]uint64, inodeNames map[InodeId]INode, curInodeId InodeId) *INodeTree {
	var children []uint64
	children, ok1 := parChildrenMap[ParentId(curInodeId)]
	inode, _ := inodeNames[curInodeId]
	numberOfChildren := len(children)

	if !ok1 || numberOfChildren == 0 {
		pathCounter++
		return &INodeTree{inode, nil}
	}

	refs := make([]*INodeTree, numberOfChildren)
	for i, child := range children {
		refs[i] = findChildren(parChildrenMap, inodeNames, InodeId(child))
	}
	pathCounter++
	return &INodeTree{inode, refs}
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

func parseInodeSection(info *pb.FileSummary_Section, imageFile *os.File) (map[InodeId]INode, EntityCount) {
	var (
		inodeSectionBytes        = make([]byte, info.GetLength())
		nameIdMap                = make(map[InodeId]INode)
		files             uint32 = 0
		dirs              uint32 = 0
		symlinks          uint32 = 0
		inodeType         pb.INodeSection_INode_Type
	)
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
		inodeType = inode.GetType()
		id := InodeId(inode.GetId())
		if inodeType == 1 {
			nameIdMap[id] = INode{inode.GetName(), id, FILE}
			files++
		} else if inodeType == 2 {
			nameIdMap[id] = INode{inode.GetName(), id, DIRECTORY}
			dirs++
		} else {
			nameIdMap[id] = INode{inode.GetName(), id, SYMLINK}
			symlinks++
		}
	}
	entityCount := EntityCount{Files: files, Directories: dirs, Symlinks: symlinks}
	return nameIdMap, entityCount
}

func parseInodeDirectorySection(info *pb.FileSummary_Section, imageFile *os.File) map[ParentId][]uint64 {
	var (
		parChildrenMap = make(map[ParentId][]uint64)
	)
	startPos := int64(info.GetOffset())
	length := info.GetLength()
	dirSectionBytes := make([]byte, length)
	_, err := imageFile.ReadAt(dirSectionBytes, startPos)
	logIfErr(err)
	dirEntry := &pb.INodeDirectorySection_DirEntry{}
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
		parent := ParentId(dirEntry.GetParent())
		children := dirEntry.GetChildren()
		parChildrenMap[parent] = children
		a -= newPos
		dirSectionBytes = dirSectionBytes[newPos:]
	}
	return parChildrenMap
}
