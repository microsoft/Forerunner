package optipreplayer

import (
	"container/heap"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
)

type TransactionPool map[common.Address]types.Transactions

func (p TransactionPool) String() string {
	retStr := "{"
	for sender, txns := range p {
		retStr += sender.Hex() + ":["
		for i, txn := range txns {
			if i > 0 {
				retStr += ", "
			}
			var priceStr string
			var raw = txn.GasPrice().String()
			for len(raw) > 0 {
				newEnd := len(raw) - 3
				if newEnd > 0 {
					priceStr = "," + raw[newEnd:] + priceStr
					raw = raw[:newEnd]
				} else {
					priceStr = raw + priceStr
					raw = ""
				}
			}
			retStr += fmt.Sprintf("%s:%d-%v", txn.Hash().TerminalString(), txn.Nonce(), priceStr)
		}
		retStr += "],"
	}
	retStr += "}"
	return retStr
}

func (p TransactionPool) size() int {
	size := 0
	for _, txns := range p {
		size += len(txns)
	}
	return size
}

func (p TransactionPool) gas() uint64 {
	size := uint64(0)
	for _, txns := range p {
		for _, txn := range txns {
			size += txn.Gas()
		}
	}
	return size
}

func (p TransactionPool) addTxn(sender common.Address, txn *types.Transaction) bool {
	senderTxCount := len(p[sender])
	if senderTxCount > 0 {
		prevNonce := p[sender][senderTxCount-1].Nonce()
		if txn.Nonce() != prevNonce+1 {
			return false
		}
	}
	p[sender] = append(p[sender], txn)
	return true
}

func (p TransactionPool) copy() TransactionPool {
	newPool := make(TransactionPool, len(p))
	for addr, txns := range p {
		newPool[addr] = make(types.Transactions, len(txns))
		for i, txn := range txns {
			newPool[addr][i] = txn
		}
	}
	return newPool
}

func (p TransactionPool) txnsPoolDiff(p2 TransactionPool) TransactionPool {
	keep := make(TransactionPool)
	for addr, txns1 := range p {
		if txns2, ok := p2[addr]; ok {
			diff := types.TxDifference(txns1, txns2)
			if len(diff) > 0 {
				keep[addr] = diff
			}
		} else {
			keep[addr] = txns1
		}
	}
	return keep
}

func (p TransactionPool) isTxnsPoolLarge(p2 TransactionPool) bool {
	for addr, txns1 := range p {
		if txns2, ok := p2[addr]; ok {
			if len(types.TxDifference(txns1, txns2)) > 0 {
				return true
			}
		} else {
			return true
		}
	}
	return false
}

const (
	// minerRecallSize is area of previous blocks when count miners.
	minerRecallSize = 1000
	// activeMinerThreshold is the threshold of active miner added in white list.
	activeMinerThreshold = 2
)

type MinerList struct {
	list       [minerRecallSize]common.Address
	next       int
	count      map[common.Address]int
	top5Active []common.Address
}

func NewMinerList(chain *core.BlockChain) MinerList {
	minerList := MinerList{
		list:       [minerRecallSize]common.Address{},
		count:      make(map[common.Address]int),
		top5Active: make([]common.Address, 5),
	}
	nextBlk := chain.CurrentBlock().NumberU64() + 1
	start := nextBlk - minerRecallSize
	for index := range minerList.list {
		coinbase := chain.GetBlockByNumber(start + uint64(index)).Coinbase()
		minerList.list[index] = coinbase
		minerList.count[coinbase]++
	}
	minerList.updateTop5Active()
	return minerList
}

func (l *MinerList) updateTop5Active() {
	minerList := make([]common.Address, 0, len(l.count))
	for miner := range l.count {
		minerList = append(minerList, miner)
	}
	sort.Slice(minerList, func(i, j int) bool {
		return l.count[minerList[i]] > l.count[minerList[j]]
	})
	copy(l.top5Active, minerList[:5])
}

func (l *MinerList) addMiner(newMiner common.Address) {
	oldMiner := l.list[l.next]
	l.list[l.next] = newMiner
	l.next++
	if l.next == minerRecallSize {
		l.next = 0
	}
	l.count[oldMiner]--
	l.count[newMiner]++

	l.updateTop5Active()
}

func (l *MinerList) getWhiteList() map[common.Address]struct{} {
	var whiteList = make(map[common.Address]struct{})
	for miner, count := range l.count {
		if count >= activeMinerThreshold {
			whiteList[miner] = struct{}{}
		}
	}
	return whiteList
}

const (
	balance   State = 1 << iota
	nonce     State = 1 << iota
	code      State = 1 << iota
	suicided  State = 1 << iota
	all             = balance | nonce | code | suicided
	coinbase        = balance
	timestamp       = nonce
	gasLimit        = code
)

type State byte

type ReadState struct {
	State
	storage map[common.Hash]struct{}
}
type ReadChain = State
type WriteState ReadState

func NewReadState(r *state.ReadState) *ReadState {
	readState := &ReadState{
		storage: make(map[common.Hash]struct{}),
	}
	if r.Empty != nil {
		readState.State = all
		return readState
	} else {
		readState.State = suicided
	}
	if r.Balance != nil {
		readState.State |= balance
	}
	if r.Nonce != nil {
		readState.State |= nonce
	}
	if r.CodeHash != nil {
		readState.State |= code
	}
	for key := range r.Storage {
		readState.storage[key] = struct{}{}
	}
	return readState
}

func NewReadChain(r state.ReadChain) ReadChain {
	var readChain State
	if r.Coinbase != nil {
		readChain |= coinbase
	}
	if r.Timestamp != nil {
		readChain |= timestamp
	}
	if r.GasLimit != nil {
		readChain |= gasLimit
	}
	return readChain
}

func NewWriteState(w *state.WriteState) *WriteState {
	writeState := &WriteState{
		storage: make(map[common.Hash]struct{}),
	}
	if w.Balance != nil {
		writeState.State |= balance
	}
	if w.Nonce != nil {
		writeState.State |= nonce
	}
	if w.Code != nil {
		writeState.State |= code
	}
	for key := range w.DirtyStorage {
		writeState.storage[key] = struct{}{}
	}
	return writeState
}

type ReadStates map[common.Address]*ReadState
type WriteStates map[common.Address]*WriteState

func (r ReadStates) merge(readStates ReadStates) {
	for addr, readState := range readStates {
		if s, ok := r[addr]; ok {
			s.State |= readState.State
		} else {
			r[addr] = &ReadState{
				State:   readState.State,
				storage: make(map[common.Hash]struct{}, len(readState.storage)),
			}
		}
		for key := range readState.storage {
			r[addr].storage[key] = struct{}{}
		}
	}
}

func (r *ReadChain) merge(readChain ReadChain) {
	*r |= readChain
}

func (w WriteStates) merge(writeStates WriteStates) {
	for addr, writeState := range writeStates {
		if s, ok := w[addr]; ok {
			s.State |= writeState.State
		} else {
			w[addr] = &WriteState{
				State:   writeState.State,
				storage: make(map[common.Hash]struct{}, len(writeState.storage)),
			}
		}
		for key := range writeState.storage {
			w[addr].storage[key] = struct{}{}
		}
	}
}

type RWRecord struct {
	readStates ReadStates
	ReadChain
	writeStates WriteStates
}

func NewRWRecord(rw *cache.RWRecord) *RWRecord {
	if rw == nil {
		return &RWRecord{
			readStates:  make(ReadStates),
			writeStates: make(WriteStates),
		}
	}
	readStates := make(ReadStates)
	for addr, rState := range rw.RState {
		readStates[addr] = NewReadState(rState)
	}
	writeStates := make(WriteStates)
	for addr, wState := range rw.WState {
		writeStates[addr] = NewWriteState(wState)
	}
	return &RWRecord{
		readStates:  readStates,
		ReadChain:   NewReadChain(rw.RChain),
		writeStates: writeStates,
	}
}

func (r *RWRecord) isCoinbaseDep() bool {
	return r.ReadChain&coinbase > 0
}

func (r *RWRecord) isTimestampDep() bool {
	return r.ReadChain&timestamp > 0
}

func (r *RWRecord) isChainDep() bool {
	return r.ReadChain > 0
}

func (r *RWRecord) merge(rwrecord *RWRecord) {
	r.readStates.merge(rwrecord.readStates)
	r.ReadChain.merge(rwrecord.ReadChain)
	r.writeStates.merge(rwrecord.writeStates)
}

func (r *RWRecord) isRWOverlap(w *RWRecord) bool {
	for addr, writeState := range w.writeStates {
		if readState, ok := r.readStates[addr]; ok {
			if readState.State&writeState.State > 0 {
				return true
			}
			for key := range writeState.storage {
				if _, ok := readState.storage[key]; ok {
					return true
				}
			}
		}
	}
	return false
}

type Header struct {
	coinbase common.Address
	time     uint64
	gasLimit uint64
}
type TxnOrder []common.Hash

func (o TxnOrder) String() string {
	retStr := "["
	for i, hash := range o {
		if i > 0 {
			retStr += ","
		}
		retStr += fmt.Sprintf("%d:%s", i, hash.TerminalString())
	}
	retStr += "]"
	return retStr
}

type TxnGroup struct {
	hash common.Hash
	txns TransactionPool
	*RWRecord

	valid int32

	preplayCount int
	priority     int

	parent *types.Block
	header Header

	txnCount   int
	orderCount *big.Int

	startList   []int
	subpoolList []TransactionPool
	nextInList  []map[common.Address]int
	nextOrder   chan TxnOrder
}

func (g *TxnGroup) setInvalid() {
	atomic.StoreInt32(&g.valid, 0)
}

func (g *TxnGroup) setValid() {
	atomic.StoreInt32(&g.valid, 1)
}

func (g *TxnGroup) isValid() bool {
	return atomic.LoadInt32(&g.valid) == 1
}

func (g *TxnGroup) isSubInOrder(subpoolIndex int) bool {
	subPool := g.subpoolList[subpoolIndex]
	nextIn := g.nextInList[subpoolIndex]
	for from, txns := range subPool {
		if nextIn[from] < len(txns) {
			return false
		}
	}
	return true
}

func (g *TxnGroup) divideTransactionPool() {
	g.txnCount = g.txns.size()
	g.orderCount = new(big.Int).SetUint64(1)
	var (
		inPoolList int
		poolCpy    = g.txns.copy()
	)
	for inPoolList < g.txnCount {
		var (
			maxPriceFrom = make([]common.Address, 0)
			maxPrice     = new(big.Int)
		)
		for from, txns := range poolCpy {
			if len(txns) == 0 {
				continue
			}
			headTxn := txns[0]
			headGasPrice := headTxn.GasPrice()
			cmp := headGasPrice.Cmp(maxPrice)
			switch {
			case cmp > 0:
				maxPrice = headGasPrice
				maxPriceFrom = []common.Address{from}
			case cmp == 0:
				maxPriceFrom = append(maxPriceFrom, from)
			}
		}
		var (
			maxPriceTotal  int64
			maxPriceCounts []int64
			subPool        = make(TransactionPool)
		)
		for _, from := range maxPriceFrom {
			var (
				index         int
				txns          = poolCpy[from]
				txnsSize      = len(txns)
				maxPriceCount int64
			)
			for ; index < txnsSize; index++ {
				cmp := txns[index].GasPrice().Cmp(maxPrice)
				switch {
				case cmp == 0:
					maxPriceCount++
				case cmp < 0:
					break
				}
			}
			maxPriceTotal += maxPriceCount
			maxPriceCounts = append(maxPriceCounts, maxPriceCount)
			if index == txnsSize {
				subPool[from] = poolCpy[from]
				delete(poolCpy, from)
			} else {
				subPool[from] = txns[:index]
				poolCpy[from] = txns[index:]
			}
		}
		factor := getFactorial(maxPriceTotal)
		for _, count := range maxPriceCounts {
			factor.Div(factor, getFactorial(count))
		}
		g.orderCount.Mul(g.orderCount, factor)
		g.startList = append(g.startList, inPoolList)
		g.subpoolList = append(g.subpoolList, subPool)
		nextIn := make(map[common.Address]int)
		for from := range subPool {
			nextIn[from] = 0
		}
		g.nextInList = append(g.nextInList, nextIn)

		inPoolList += subPool.size()
	}
}

var getFactorial = func() func(n int64) *big.Int {
	factorCache := map[int64]*big.Int{
		0: new(big.Int).SetUint64(1),
		1: new(big.Int).SetUint64(1),
		2: new(big.Int).SetUint64(2),
		3: new(big.Int).SetUint64(6),
	}
	return func(n int64) *big.Int {
		if factor, ok := factorCache[n]; ok {
			return new(big.Int).Set(factor)
		}
		var factor = new(big.Int).SetInt64(1)
		for mul := n; mul >= 1; mul-- {
			if cache, ok := factorCache[mul]; ok {
				return factor.Mul(factor, cache)
			}
			factor.Mul(factor, new(big.Int).SetInt64(mul))
		}
		return factor
	}
}()

type TaskQueue struct {
	sync.RWMutex
	groups []*TxnGroup
}

func NewTaskQueue() *TaskQueue {
	return &TaskQueue{
		groups: make([]*TxnGroup, 0),
	}
}

func (q *TaskQueue) Len() int           { return len(q.groups) }
func (q *TaskQueue) Less(i, j int) bool { return q.groups[i].priority < q.groups[j].priority }
func (q *TaskQueue) Swap(i, j int)      { q.groups[i], q.groups[j] = q.groups[j], q.groups[i] }

func (q *TaskQueue) Push(x interface{}) {
	q.groups = append(q.groups, x.(*TxnGroup))
}

func (q *TaskQueue) Pop() interface{} {
	old := q.groups
	n := len(old)
	x := old[n-1]
	q.groups = old[:n-1]
	return x
}

func (q *TaskQueue) haveTask() bool {
	q.RLock()
	defer q.RUnlock()
	return len(q.groups) > 0
}

func (q *TaskQueue) countTask() int {
	q.RLock()
	defer q.RUnlock()
	return len(q.groups)
}

func (q *TaskQueue) pushTask(group *TxnGroup) {
	q.Lock()
	defer q.Unlock()
	heap.Push(q, group)
}

func (q *TaskQueue) popTask() *TxnGroup {
	q.Lock()
	defer q.Unlock()
	if len(q.groups) > 0 {
		return heap.Pop(q).(*TxnGroup)
	} else {
		return nil
	}
}

type PreplayLog struct {
	packageCnt  uint64
	buildCnt    uint64
	deadlineCnt uint64
	sync.RWMutex
	groupLog     map[common.Hash]struct{}
	groupEnd     map[common.Hash]struct{}
	groupExecCnt uint64
	txnLog       map[common.Hash]struct{}
	txnExecCnt   uint64
}

func NewPreplayLog() *PreplayLog {
	return &PreplayLog{
		groupLog: make(map[common.Hash]struct{}),
		groupEnd: make(map[common.Hash]struct{}),
		txnLog:   make(map[common.Hash]struct{}),
	}
}

func (l *PreplayLog) reportNewPackage() {
	l.packageCnt++
}

func (l *PreplayLog) reportNewBuild() {
	l.buildCnt++
}

func (l *PreplayLog) reportNewDeadline() {
	l.deadlineCnt++
}

func (l *PreplayLog) reportNewGroup(group *TxnGroup) {
	l.Lock()
	defer l.Unlock()

	if _, ok := l.groupLog[group.hash]; !ok {
		l.groupLog[group.hash] = struct{}{}
		for _, txns := range group.txns {
			for _, txn := range txns {
				l.txnLog[txn.Hash()] = struct{}{}
			}
		}
	}
}

func (l *PreplayLog) reportGroupEnd(group *TxnGroup) {
	l.Lock()
	defer l.Unlock()

	if _, ok := l.groupLog[group.hash]; ok {
		l.groupEnd[group.hash] = struct{}{}
	}
}

func (l *PreplayLog) reportGroupPreplay(group *TxnGroup) {
	l.Lock()
	defer l.Unlock()

	l.groupExecCnt++
	l.txnExecCnt += uint64(group.txnCount)
}

func (l *PreplayLog) printAndClearLog(block uint64, remain int) {
	l.RLock()
	log.Info("In last block", "number", block,
		"package(build)", fmt.Sprintf("%d(%d)", l.packageCnt, l.buildCnt), "deadline", l.deadlineCnt,
		"group", fmt.Sprintf("%d(%d)-%d", len(l.groupLog), len(l.groupEnd), l.groupExecCnt),
		"transaction", fmt.Sprintf("%d-%d", len(l.txnLog), l.txnExecCnt), "remain", remain,
	)
	l.RUnlock()

	l.Lock()
	defer l.Unlock()

	l.packageCnt = 0
	l.buildCnt = 0
	l.deadlineCnt = 0
	l.groupLog = make(map[common.Hash]struct{})
	l.groupEnd = make(map[common.Hash]struct{})
	l.groupExecCnt = 0
	l.txnLog = make(map[common.Hash]struct{})
	l.txnExecCnt = 0
}
