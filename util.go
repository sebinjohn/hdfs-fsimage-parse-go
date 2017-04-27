package main

import (
	"sort"
	"strconv"
)

type Pair struct {
	Key   string
	Value uint32
}

func (p Pair) String() string { return string(p.Key) + " " + strconv.FormatUint(uint64(p.Value), 10) }

type PairList []Pair

func (p PairList) Len() int { return len(p) }

func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }

func (p PairList) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func SortByValue(m map[string]uint32) PairList {
	pl := make(PairList, len(m))
	i := 0
	for k, v := range m {
		pl[i] = Pair{k, v}
		i++
	}
	sort.Sort(sort.Reverse(pl))
	return pl
}
