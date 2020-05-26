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
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type PackageType int

const (
	TYPE0 PackageType = iota
)

type TransactionPool map[common.Address]types.Transactions

func (p TransactionPool) String() string {
	retStr := "{"
	var first = true
	for sender, txns := range p {
		if first {
			first = false
		} else {
			retStr += ","
		}
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
		retStr += "]"
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
		copy(newPool[addr], txns)
	}
	return newPool
}

func (p TransactionPool) isTxnsPoolLarge(p2 TransactionPool) bool {
	for addr, txns1 := range p {
		if txns2, ok := p2[addr]; ok {
			if types.IsTxnsPoolLarge(txns1, txns2) {
				return true
			}
		} else {
			return true
		}
	}
	return false
}

func (p TransactionPool) filter(check func(sender common.Address, tx *types.Transaction) bool) TransactionPool {
	var p2 = make(TransactionPool)
	for from, txns := range p {
		for _, txn := range txns {
			if check(from, txn) {
				p2[from] = make(types.Transactions, txns.Len())
				copy(p2[from], txns)
				break
			}
		}
	}
	return p2
}

func (p TransactionPool) isTxnIn(sender common.Address, txn *types.Transaction) bool {
	if txns, ok := p[sender]; ok {
		txnsLen := txns.Len()
		index := sort.Search(txnsLen, func(i int) bool {
			return txns[i].Nonce() >= txn.Nonce()
		})
		return index < txnsLen && txns[index].Nonce() == txn.Nonce() && txns[index].Hash() == txn.Hash()
	} else {
		return false
	}
}

const (
	// minerRecallSize is area of previous blocks when count miners.
	minerRecallSize = 1000
	// topActiveCount is count of top active miner for forecast when preplay.
	topActiveCount = 5
	// activeMinerThreshold is the threshold of active miner added in white list.
	activeMinerThreshold = 2
)

type MinerList struct {
	list      [minerRecallSize]common.Address
	next      int
	count     map[common.Address]int
	whiteList map[common.Address]struct{}
	topActive [topActiveCount]common.Address
}

func NewMinerList(chain *core.BlockChain) *MinerList {
	minerList := &MinerList{
		list:  [minerRecallSize]common.Address{},
		count: make(map[common.Address]int),
	}
	nextBlk := chain.CurrentBlock().NumberU64() + 1
	start := nextBlk - minerRecallSize
	for index := range minerList.list {
		coinbase := chain.GetBlockByNumber(start + uint64(index)).Coinbase()
		minerList.list[index] = coinbase
		minerList.count[coinbase]++
	}
	minerList.updateWhiteList()
	minerList.updateTopActive()
	return minerList
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

	l.updateWhiteList()
	l.updateTopActive()
}

func (l *MinerList) updateWhiteList() {
	l.whiteList = make(map[common.Address]struct{})
	for miner, count := range l.count {
		if count >= activeMinerThreshold {
			l.whiteList[miner] = struct{}{}
		}
	}
}

func (l *MinerList) updateTopActive() {
	minerList := make([]common.Address, 0, len(l.count))
	for miner := range l.count {
		minerList = append(minerList, miner)
	}
	sort.Slice(minerList, func(i, j int) bool {
		return l.count[minerList[i]] > l.count[minerList[j]]
	})
	copy(l.topActive[:], minerList[:topActiveCount])
}

type Trigger struct {
	Name        string
	ExecutorNum int

	IsTxsInDetector bool

	IsChainHeadDrive bool

	IsTxsNumDrive        bool
	PreplayedTxsNumLimit uint64

	IsTxsRatioDrive        bool
	PreplayedTxsRatioLimit float64

	IsBlockCntDrive           bool
	PreplayedBlockNumberLimit uint64

	IsPriceRankDrive        bool
	PreplayedPriceRankLimit uint64
}

func NewTrigger(name string, executorNum int) *Trigger {
	return &Trigger{
		Name:        name,
		ExecutorNum: executorNum,
	}
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

func (s ReadState) String() string {
	retStr := fmt.Sprintf("%b", s.State)
	var keyList []string
	for key := range s.storage {
		keyList = append(keyList, key.Hex())
	}
	if len(keyList) > 0 {
		retStr += "-" + fmt.Sprintf("%v", keyList)
	}
	return retStr
}

func (r ReadChain) String() string {
	return fmt.Sprintf("ReadChain: %b", r)
}

func (s WriteState) String() string {
	retStr := fmt.Sprintf("%b", s.State)
	var keyList []string
	for key := range s.storage {
		keyList = append(keyList, key.Hex())
	}
	if len(keyList) > 0 {
		retStr += "-" + fmt.Sprintf("%v", keyList)
	}
	return retStr
}

func NewReadState(r *state.ReadState) *ReadState {
	readState := &ReadState{
		storage: make(map[common.Hash]struct{}),
	}
	if r.Empty != nil {
		readState.State = all
	} else {
		readState.State = suicided
		if r.Balance != nil {
			readState.State |= balance
		}
		if r.Nonce != nil {
			readState.State |= nonce
		}
		if r.CodeHash != nil {
			readState.State |= code
		}
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
	if w.Suicided != nil {
		writeState.State |= suicided
	}
	for key := range w.DirtyStorage {
		writeState.storage[key] = struct{}{}
	}
	return writeState
}

type ReadStates map[common.Address]*ReadState
type WriteStates map[common.Address]*WriteState

func (r ReadStates) String() string {
	var ret string
	var first = true
	for addr, readState := range r {
		if first {
			first = false
		} else {
			ret += ", "
		}
		ret += addr.Hex() + ":" + readState.String()
	}
	return "ReadStates:{" + ret + "}"
}

func (w WriteStates) String() string {
	var ret string
	var first = true
	for addr, writeState := range w {
		if first {
			first = false
		} else {
			ret += ", "
		}
		ret += addr.Hex() + ":" + writeState.String()
	}
	return "WriteStates:{" + ret + "}"
}

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

func (r *RWRecord) String() string {
	return fmt.Sprintf("RWRecord:{%v, %v, %v}", r.readStates, r.ReadChain, r.writeStates)
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

type OrderAndHeader struct {
	order  TxnOrder
	header Header
}

type TxnGroup struct {
	// Core content in txn group
	hash    common.Hash
	txnPool TransactionPool
	txnList [][]byte
	*RWRecord

	// Just as its name
	valid int32

	// Constant information during preplay, access without lock
	parent       *types.Block
	txnCount     int
	chainFactor  int
	orderCount   *big.Int
	startList    []int
	subpoolList  []TransactionPool
	basicPreplay bool
	addrNotCopy  map[common.Address]struct{}

	// Update after every preplay, access with lock
	preplayMu       sync.RWMutex
	preplayCount    int
	preplayFinish   map[common.Hash]int
	preplayFail     map[common.Hash][]string
	preplayOrderLog []TxnOrder
	preplayTimeLog  []uint64
	priority        int
	isPriorityConst bool

	// Use for produce order and header
	nextInList         []map[common.Address]int
	nextOrderAndHeader chan OrderAndHeader // order and header read from this channel is read-only

	// Use for return round ID
	roundIDCh chan uint64
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

func (g *TxnGroup) defaultInit(parent *types.Block, basicPreplay bool) {
	atomic.StoreInt32(&g.valid, 1)
	g.parent = parent
	g.txnCount = g.txnPool.size()
	g.chainFactor = 1
	g.orderCount = new(big.Int).SetInt64(1)
	g.basicPreplay = basicPreplay
	g.addrNotCopy = make(map[common.Address]struct{})
	g.preplayFinish = make(map[common.Hash]int)
	g.preplayFail = make(map[common.Hash][]string)
}

func (g *TxnGroup) updateByPreplay(resultMap map[common.Hash]*cache.ExtraResult, orderAndHeader OrderAndHeader) {
	g.preplayMu.Lock()
	defer g.preplayMu.Unlock()

	g.preplayCount++
	for _, txns := range g.txnPool {
		for _, txn := range txns {
			if result, ok := resultMap[txn.Hash()]; ok {
				if result.Status == "will in" {
					g.preplayFinish[txn.Hash()]++
				} else {
					g.preplayFail[txn.Hash()] = append(g.preplayFail[txn.Hash()], fmt.Sprintf("%s:%s", result.Status, result.Reason))
				}
			} else {
				g.preplayFail[txn.Hash()] = append(g.preplayFail[txn.Hash()], "Nil result map in executor")
			}
		}
	}
	g.preplayOrderLog = append(g.preplayOrderLog, orderAndHeader.order)
	g.preplayTimeLog = append(g.preplayTimeLog, orderAndHeader.header.time)
	if !g.isPriorityConst {
		g.priority = g.preplayCount * g.txnCount
	}
}

func (g *TxnGroup) getPreplayCount() int {
	g.preplayMu.RLock()
	defer g.preplayMu.RUnlock()

	return g.preplayCount
}

func (g *TxnGroup) haveTxnFinished(txn common.Hash) bool {
	g.preplayMu.RLock()
	defer g.preplayMu.RUnlock()

	return g.preplayFinish[txn] > 0
}

func (g *TxnGroup) getTxnFailReason(txn common.Hash) string {
	g.preplayMu.RLock()
	defer g.preplayMu.RUnlock()

	return strings.Join(g.preplayFail[txn], ",")
}

func (g *TxnGroup) getFirstPreplayOrder() TxnOrder {
	g.preplayMu.RLock()
	defer g.preplayMu.RUnlock()

	return g.preplayOrderLog[0]
}

func (g *TxnGroup) evaluatePreplay(groundOrder, orderBefore TxnOrder) (bool, bool) {
	g.preplayMu.RLock()
	defer g.preplayMu.RUnlock()

	orderBeforeSize := len(orderBefore)
	groundOrderMap := make(map[common.Hash]struct{}, len(groundOrder))
	for _, txn := range groundOrder {
		groundOrderMap[txn] = struct{}{}
	}

	snapMiss := true
	for _, order := range g.preplayOrderLog {
		if len(order) < orderBeforeSize {
			panic("Detect missing txn but not find in before process")
		}
		var snapHit = true
		for _, txnInOrder := range order[:orderBeforeSize] {
			if _, ok := groundOrderMap[txnInOrder]; !ok {
				snapHit = false
				break
			}
		}
		if snapHit {
			snapMiss = false
			break
		}
	}
	if snapMiss {
		return false, false
	}

	for _, order := range g.preplayOrderLog {
		var orderHit = true
		for index, txnInOrder := range order[:orderBeforeSize] {
			if txnInOrder != orderBefore[index] {
				orderHit = false
				break
			}
		}
		if orderHit {
			return true, true
		}
	}
	return true, false
}

func (g *TxnGroup) getTimestampOrderLog() []uint64 {
	g.preplayMu.RLock()
	defer g.preplayMu.RUnlock()

	return g.preplayTimeLog
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
	mu         sync.RWMutex
	lastUpdate time.Time

	packageCnt       int
	simplePackageCnt int
	taskBuildCnt     int

	deadlineLog map[uint64]struct{}
	groupLog    map[common.Hash]*TxnGroup
	groupEnd    map[common.Hash]struct{}
	txnLog      map[common.Hash]struct{}

	groupExecCnt uint64
	txnExecCnt   uint64
}

func NewPreplayLog() *PreplayLog {
	return &PreplayLog{
		lastUpdate:  time.Now(),
		deadlineLog: make(map[uint64]struct{}),
		groupLog:    make(map[common.Hash]*TxnGroup),
		groupEnd:    make(map[common.Hash]struct{}),
		txnLog:      make(map[common.Hash]struct{}),
	}
}

func (l *PreplayLog) reportNewPackage(simple bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.packageCnt++
	if simple {
		l.simplePackageCnt++
	}
}

func (l *PreplayLog) reportNewTaskBuild() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.taskBuildCnt++
}

func (l *PreplayLog) reportNewDeadline(deadline uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.deadlineLog[deadline] = struct{}{}
}

func (l *PreplayLog) reportNewGroup(group *TxnGroup) (originGroup *TxnGroup, exist bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if originGroup, exist = l.groupLog[group.hash]; !exist {
		l.groupLog[group.hash] = group
		for _, txns := range group.txnPool {
			for _, txn := range txns {
				l.txnLog[txn.Hash()] = struct{}{}
			}
		}
	}
	return
}

func (l *PreplayLog) isGroupExist(group *TxnGroup) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, exist := l.groupLog[group.hash]
	return exist
}

func (l *PreplayLog) reportGroupEnd(group *TxnGroup) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.groupLog[group.hash]; ok {
		l.groupEnd[group.hash] = struct{}{}
	}
}

func (l *PreplayLog) reportGroupPreplay(group *TxnGroup) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.groupExecCnt++
	l.txnExecCnt += uint64(group.txnCount)
}

func (l *PreplayLog) searchGroupHitGroup(currentTxn *types.Transaction, sender common.Address) map[common.Hash]*TxnGroup {
	l.mu.RLock()
	defer l.mu.RUnlock()

	groupHitGroup := make(map[common.Hash]*TxnGroup)
	for groupHash, group := range l.groupLog {
		for _, txn := range group.txnPool[sender] {
			if txn.Hash() == currentTxn.Hash() {
				groupHitGroup[groupHash] = group
				break
			}
		}
	}
	return groupHitGroup
}

func (l *PreplayLog) getDeadlineHistory() string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	history := make([]uint64, 0, len(l.deadlineLog))
	for ddl := range l.deadlineLog {
		history = append(history, ddl)
	}
	sort.Slice(history, func(i, j int) bool {
		return history[i] < history[j]
	})
	if size := len(history); size > 0 {
		return fmt.Sprintf("%d-%d-[%d:%d]", len(history), history[size-1]-history[0]+1, history[0], history[size-1])
	} else {
		return ""
	}
}

func (l *PreplayLog) disableGroup() {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for _, group := range l.groupLog {
		group.setInvalid()
	}
}

func (l *PreplayLog) printAndClearLog(block uint64, remain int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	context := []interface{}{"number", block, "package", fmt.Sprintf("%d(%d)", l.packageCnt, l.simplePackageCnt)}
	if l.packageCnt > 0 {
		context = append(context, "avgPackage", common.PrettyDuration(int64(time.Since(l.lastUpdate))/int64(l.packageCnt)))
	}
	context = append(
		context, "build", l.taskBuildCnt, "deadline", len(l.deadlineLog),
		"group", fmt.Sprintf("%d(%d)-%d", len(l.groupLog), len(l.groupEnd), l.groupExecCnt),
		"transaction", fmt.Sprintf("%d-%d", len(l.txnLog), l.txnExecCnt),
		"remain", remain, "duration", common.PrettyDuration(time.Since(l.lastUpdate)),
	)
	log.Info("In last block", context...)

	l.lastUpdate = time.Now()
	l.packageCnt = 0
	l.simplePackageCnt = 0
	l.taskBuildCnt = 0
	l.deadlineLog = make(map[uint64]struct{})
	l.groupLog = make(map[common.Hash]*TxnGroup)
	l.groupEnd = make(map[common.Hash]struct{})
	l.groupExecCnt = 0
	l.txnLog = make(map[common.Hash]struct{})
	l.txnExecCnt = 0
}
