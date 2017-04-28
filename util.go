package main

import (
	"sort"
	"strconv"
)

type HDFSFileName string
type NameCount uint32

type ChildrenCount int
type ParentID uint64

func (i ChildrenCount) String() string {
	return strconv.Itoa(int(i))
}

func (i ParentID) String() string {
	return strconv.FormatUint(uint64(i), 10)
}

type NameCountPair struct {
	Name  HDFSFileName
	Count NameCount
}

func (p NameCountPair) String() string {
	return string(p.Name) + " " + strconv.FormatUint(uint64(p.Count), 10)
}

type NameCountPairList []NameCountPair

func (p NameCountPairList) Len() int { return len(p) }

func (p NameCountPairList) Less(i, j int) bool { return p[i].Count < p[j].Count }

func (p NameCountPairList) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func SortByNameCount(m map[HDFSFileName]NameCount) NameCountPairList {
	pl := make(NameCountPairList, len(m))
	i := 0
	for k, v := range m {
		pl[i] = NameCountPair{k, v}
		i++
	}
	sort.Sort(sort.Reverse(pl))
	return pl
}

type ChildrenCountPair struct {
	Parid      ParentID
	ChildCount ChildrenCount
}

type ChildrenCountPairList []ChildrenCountPair

func (p ChildrenCountPair) String() string {
	return p.Parid.String() + " " + p.ChildCount.String()
}

func (p ChildrenCountPairList) Len() int { return len(p) }

func (p ChildrenCountPairList) Less(i, j int) bool { return p[i].ChildCount < p[j].ChildCount }

func (p ChildrenCountPairList) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func SortByChildCount(data map[ParentID]ChildrenCount) ChildrenCountPairList {
	pl := make(ChildrenCountPairList, len(data))
	i := 0
	for k, v := range data {
		pl[i] = ChildrenCountPair{k, v}
		i++
	}
	sort.Sort(sort.Reverse(pl))
	return pl
}
