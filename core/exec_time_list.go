package core

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"sort"
	"time"
)

type Index int

const listSize = 1000

func (i *Index) increase() {
	*i ++
	if *i == listSize {
		*i = 0
	}
}

type ExecTimeList struct {
	list         [listSize]time.Duration
	next         Index
	last         Index
	last10Start  Index
	last100Start Index
	validCnt     int64
}

func NewExecTimeList() ExecTimeList {
	list := ExecTimeList{
		list:         [listSize]time.Duration{},
		last:         listSize - 1,
		last10Start:  listSize - 10,
		last100Start: listSize - 100,
	}
	return list
}

func (l *ExecTimeList) addExecTime(execTime time.Duration) {
	l.list[l.next] = execTime
	l.next.increase()
	l.last.increase()
	l.last10Start.increase()
	l.last100Start.increase()
	if l.validCnt < listSize {
		l.validCnt++
	}
}

func (l *ExecTimeList) lastXXStats(start Index, scope int64) (time.Duration, time.Duration, bool) {
	if l.validCnt < scope {
		return 0, 0, false
	}

	var (
		sum      int64
		statList = make([]int, 0, scope)
	)
	for index := start; index != l.next; index.increase() {
		statList = append(statList, int(l.list[index]))
		sum += int64(l.list[index])
	}
	sort.Ints(statList)
	return time.Duration(sum / scope), time.Duration((statList[scope/2-1] + statList[scope/2]) / 2), true
}

func (l *ExecTimeList) allStats() (time.Duration, time.Duration, bool) {
	if l.validCnt < listSize {
		return 0, 0, false
	}

	var (
		sum      int64
		statList = make([]int, 0, listSize)
	)
	for _, execTime := range l.list {
		statList = append(statList, int(execTime))
		sum += int64(execTime)
	}
	sort.Ints(statList)
	return time.Duration(sum / listSize), time.Duration((statList[listSize/2-1] + statList[listSize/2]) / 2), true
}

func (l *ExecTimeList) print(number uint64) {
	avgContext := []interface{}{
		"number", number,
		"execution", common.PrettyDuration(l.list[l.last]),
	}
	medContext := avgContext

	if last10Avg, last10Med, ok := l.lastXXStats(l.last10Start, 10); ok {
		avgContext = append(avgContext, "last10", common.PrettyDuration(last10Avg))
		medContext = append(medContext, "last10", common.PrettyDuration(last10Med))
	}
	if last100Avg, last100Med, ok := l.lastXXStats(l.last100Start, 100); ok {
		avgContext = append(avgContext, "last100", common.PrettyDuration(last100Avg))
		medContext = append(medContext, "last100", common.PrettyDuration(last100Med))
	}
	if last1000Avg, last1000Med, ok := l.allStats(); ok {
		avgContext = append(avgContext, "last1000", common.PrettyDuration(last1000Avg))
		medContext = append(medContext, "last1000", common.PrettyDuration(last1000Med))
	}

	log.Info("Block average execution time statistics", avgContext...)
	log.Info("Block median execution time statistics", medContext...)
}
