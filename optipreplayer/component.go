package optipreplayer

import (
	"bytes"
	"container/heap"
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"math/big"
	"sort"
	"strings"
	"sync"
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

func (p TransactionPool) addPool(other TransactionPool) (added int) {
	for sender, txns := range other {
		for _, txn := range txns {
			if p.addTxn(sender, txn) {
				added++
			}
		}
	}
	return
}

func (p TransactionPool) copy() TransactionPool {
	newPool := make(TransactionPool, len(p))
	for addr, txns := range p {
		newPool[addr] = make(types.Transactions, len(txns))
		copy(newPool[addr], txns)
	}
	return newPool
}

func (p TransactionPool) replaceCopy(replace map[*types.Transaction]*types.Transaction) TransactionPool {
	newPool := make(TransactionPool, len(p))
	for addr, txns := range p {
		newPool[addr] = make(types.Transactions, len(txns))
		for index, txn := range txns {
			if newTxn, ok := replace[txn]; ok {
				newPool[addr][index] = newTxn
			} else {
				newPool[addr][index] = txn
			}
		}
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
	balance   = 1 << iota
	nonce     = 1 << iota
	code      = 1 << iota
	suicided  = 1 << iota
	coinbase  = 1 << iota
	timestamp = 1 << iota
	gasLimit  = 1 << iota
	all       = balance | nonce | code | suicided
)

type BaseState byte

type AccountState struct {
	BaseState
	storage map[common.Hash]struct{}
}

func (s AccountState) String() string {
	retStr := fmt.Sprintf("%b", s.BaseState)
	var keyList []string
	for key := range s.storage {
		keyList = append(keyList, key.Hex())
	}
	if len(keyList) > 0 {
		retStr += "-" + fmt.Sprintf("%v", keyList)
	}
	return retStr
}

func (s AccountState) Copy() AccountState {
	var newS = ReadState{
		BaseState: s.BaseState,
		storage:   make(map[common.Hash]struct{}),
	}
	for key := range s.storage {
		newS.storage[key] = struct{}{}
	}
	return newS
}

type ReadState = AccountState
type ReadChain = BaseState
type WriteState = AccountState

func NewReadState(r *state.ReadState) *ReadState {
	readState := &ReadState{
		storage: make(map[common.Hash]struct{}),
	}
	if r.Empty != nil {
		readState.BaseState = all
	} else {
		readState.BaseState = suicided
		if r.Balance != nil {
			readState.BaseState |= balance
		}
		if r.Nonce != nil {
			readState.BaseState |= nonce
		}
		if r.CodeHash != nil {
			readState.BaseState |= code
		}
	}
	for key := range r.Storage {
		readState.storage[key] = struct{}{}
	}
	return readState
}

func NewReadChain(r state.ReadChain) ReadChain {
	var readChain BaseState
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
		writeState.BaseState |= balance
	}
	if w.Nonce != nil {
		writeState.BaseState |= nonce
	}
	if w.Code != nil {
		writeState.BaseState |= code
	}
	if w.Suicided != nil {
		writeState.BaseState |= suicided
	}
	for key := range w.DirtyStorage {
		writeState.storage[key] = struct{}{}
	}
	return writeState
}

func (s ReadChain) String() string {
	return fmt.Sprintf("ReadChain: %b", s)
}

func (s ReadChain) Copy() ReadChain {
	return s
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

func (r ReadStates) Copy() ReadStates {
	newStates := make(ReadStates)
	for addr, readState := range r {
		newState := readState.Copy()
		newStates[addr] = &newState
	}
	return newStates
}

func (w WriteStates) Copy() WriteStates {
	newStates := make(WriteStates)
	for addr, writeState := range w {
		newState := writeState.Copy()
		newStates[addr] = &newState
	}
	return newStates
}

func (r ReadStates) merge(readStates ReadStates) {
	for addr, readState := range readStates {
		if s, ok := r[addr]; ok {
			s.BaseState |= readState.BaseState
		} else {
			r[addr] = &ReadState{
				BaseState: readState.BaseState,
				storage:   make(map[common.Hash]struct{}, len(readState.storage)),
			}
		}
		for key := range readState.storage {
			r[addr].storage[key] = struct{}{}
		}
	}
}

func (s *ReadChain) merge(readChain ReadChain) {
	*s |= readChain
}

func (w WriteStates) merge(writeStates WriteStates) {
	for addr, writeState := range writeStates {
		if s, ok := w[addr]; ok {
			s.BaseState |= writeState.BaseState
		} else {
			w[addr] = &WriteState{
				BaseState: writeState.BaseState,
				storage:   make(map[common.Hash]struct{}, len(writeState.storage)),
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

func (r *RWRecord) Copy() *RWRecord {
	return &RWRecord{
		readStates:  r.readStates.Copy(),
		ReadChain:   r.ReadChain.Copy(),
		writeStates: r.writeStates.Copy(),
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
			if readState.BaseState&writeState.BaseState > 0 {
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
	validMu sync.RWMutex
	valid   bool

	// Constant information during preplay, access without lock
	parent      *types.Block
	txnCount    int
	chainFactor int
	orderCount  *big.Int
	startList   []int
	subpoolList []TransactionPool
	fullPreplay bool
	addrNotCopy map[common.Address]struct{}

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
	roundIDCh               chan uint64
	closeRoundIDChOnInvalid bool
}

func (g *TxnGroup) setInvalid() {
	g.validMu.Lock()
	defer g.validMu.Unlock()

	g.valid = false
}

func (g *TxnGroup) setValid() {
	g.validMu.Lock()
	defer g.validMu.Unlock()

	g.valid = true
}

func (g *TxnGroup) isValid() bool {
	g.validMu.RLock()
	defer g.validMu.RUnlock()

	return g.valid
}

func (g *TxnGroup) defaultInit(parent *types.Block, fullPreplay bool) {
	g.setValid()
	g.parent = parent
	g.txnCount = g.txnPool.size()
	g.chainFactor = 1
	g.orderCount = new(big.Int).SetInt64(1)
	g.fullPreplay = fullPreplay
	g.addrNotCopy = make(map[common.Address]struct{})
	g.preplayFinish = make(map[common.Hash]int)
	g.preplayFail = make(map[common.Hash][]string)
	g.nextOrderAndHeader = make(chan OrderAndHeader)
	g.roundIDCh = make(chan uint64, 1)
}

// Assume that this TxnGroup will not be accessed by other routines
func (g *TxnGroup) replaceTxn(replacingTxns types.Transactions, signer types.Signer) (newGroup *TxnGroup, replaceMapping map[*types.Transaction]*types.Transaction) {
	var replace = make(map[*types.Transaction]*types.Transaction)
	for _, replacingTxn := range replacingTxns {
		sender, _ := types.Sender(signer, replacingTxn)
		txns, ok := g.txnPool[sender]
		if !ok || len(txns) == 0 {
			continue
		}
		replacedIndex := replacingTxn.Nonce() - txns[0].Nonce()
		if replacedIndex < 0 || int(replacedIndex) >= len(txns) {
			continue
		}
		replacedTxn := txns[replacedIndex]
		if replacedTxn.Nonce() != replacingTxn.Nonce() {
			continue
		}
		replace[replacedTxn] = replacingTxn
	}

	if len(replace) == 0 {
		return nil, nil
	}

	txnPool := g.txnPool.replaceCopy(replace)
	txnList := make([][]byte, 0, len(g.txnList))
	for _, txns := range txnPool {
		for _, txn := range txns {
			txnList = append(txnList, txn.Hash().Bytes())
		}
	}
	sort.Slice(txnList, func(i, j int) bool {
		return bytes.Compare(txnList[i], txnList[j]) == -1
	})
	hash := crypto.Keccak256Hash(txnList...)

	newGroup = &TxnGroup{
		hash:               hash,
		txnPool:            txnPool,
		txnList:            txnList,
		RWRecord:           g.RWRecord.Copy(),
		valid:              g.valid,
		parent:             g.parent,
		txnCount:           g.txnCount,
		chainFactor:        g.chainFactor,
		orderCount:         new(big.Int).Set(g.orderCount),
		startList:          make([]int, len(g.startList)),
		subpoolList:        make([]TransactionPool, len(g.subpoolList)),
		fullPreplay:        g.fullPreplay,
		addrNotCopy:        make(map[common.Address]struct{}, len(g.addrNotCopy)),
		preplayFinish:      make(map[common.Hash]int, g.txnCount),
		preplayFail:        make(map[common.Hash][]string, g.txnCount),
		nextInList:         make([]map[common.Address]int, len(g.nextInList)),
		nextOrderAndHeader: make(chan OrderAndHeader),
	}
	copy(newGroup.startList, g.startList)
	for index, subpool := range g.subpoolList {
		newGroup.subpoolList[index] = subpool.replaceCopy(replace)
	}
	for addr := range g.addrNotCopy {
		newGroup.addrNotCopy[addr] = struct{}{}
	}
	for index, nextIn := range g.nextInList {
		newGroup.nextInList[index] = make(map[common.Address]int, len(nextIn))
		for addr, next := range nextIn {
			newGroup.nextInList[index][addr] = next
		}
	}

	return newGroup, replace
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

	deadlineLog    map[uint64]struct{}
	groupLog       map[common.Hash]*TxnGroup
	groupExecCnt   map[common.Hash]int
	groupEnd       map[common.Hash]struct{}
	txnLog         map[common.Hash]struct{}
	remainGroupLog map[common.Hash]*TxnGroup
	remainTxnLog   map[common.Hash]struct{}

	wobjectCopyCnt    uint64
	wobjectNotCopyCnt uint64
	wobjectAddr       map[common.Address]struct{}

	txnExecCnt         uint64
	remainGroupExecCnt uint64
	remainTxnExecCnt   uint64
}

func NewPreplayLog() *PreplayLog {
	return &PreplayLog{
		lastUpdate:     time.Now(),
		deadlineLog:    make(map[uint64]struct{}),
		groupLog:       make(map[common.Hash]*TxnGroup),
		groupExecCnt:   make(map[common.Hash]int),
		groupEnd:       make(map[common.Hash]struct{}),
		txnLog:         make(map[common.Hash]struct{}),
		remainGroupLog: make(map[common.Hash]*TxnGroup),
		remainTxnLog:   make(map[common.Hash]struct{}),
		wobjectAddr:    map[common.Address]struct{}{},
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

func (l *PreplayLog) reportRemainNewGroup(group *TxnGroup) (originGroup *TxnGroup, exist bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if originGroup, exist = l.remainGroupLog[group.hash]; !exist {
		l.remainGroupLog[group.hash] = group
		for _, txns := range group.txnPool {
			for _, txn := range txns {
				l.remainTxnLog[txn.Hash()] = struct{}{}
			}
		}
	}
	return
}

func (l *PreplayLog) getTotalGroupAndTxCount() (groupCount int, txCount int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.groupLog), len(l.txnLog)
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

func (l *PreplayLog) reportWobjectCopy(copy, noCopy uint64, addrList []common.Address) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.wobjectCopyCnt += copy
	l.wobjectNotCopyCnt += noCopy
	for _, addr := range addrList {
		l.wobjectAddr[addr] = struct{}{}
	}
}

func (l *PreplayLog) reportGroupPreplay(group *TxnGroup) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, existed := l.groupLog[group.hash]; existed {
		l.groupExecCnt[group.hash]++
		l.txnExecCnt += uint64(group.txnCount)
	} else {
		_, existed := l.remainGroupLog[group.hash]
		if existed {
			l.remainGroupExecCnt++
			l.remainTxnExecCnt += uint64(group.txnCount)
		}
	}
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

	for _, group := range l.remainGroupLog {
		group.setInvalid()
	}
}

func (l *PreplayLog) getGroupDistribution() (sizeDistribution, orderDistribution, execDistribution, chainDepDistribution string) {
	orderCounts := make([]int, 7)
	sizeCounts := make([]int, 7)
	execCounts := make([]int, 7)
	chainDepCount := 0
	timeStampDepCount := 0
	coinbaseDepCount := 0
	for _, g := range l.groupLog {
		if !g.orderCount.IsUint64() {
			orderCounts[6]++
		} else {
			orderCount := g.orderCount.Uint64()
			if orderCount <= 1 {
				orderCounts[0]++
			} else if orderCount <= 2 {
				orderCounts[1]++
			} else if orderCount <= 6 {
				orderCounts[2]++
			} else if orderCount <= 24 {
				orderCounts[3]++
			} else if orderCount <= 120 {
				orderCounts[4]++
			} else if orderCount <= 720 {
				orderCounts[5]++
			} else {
				orderCounts[6]++
			}
		}

		txnCount := g.txnCount
		cmptypes.MyAssert(txnCount > 0)
		if txnCount == 1 {
			sizeCounts[0]++
		} else if txnCount == 2 {
			sizeCounts[1]++
		} else if txnCount <= 4 {
			sizeCounts[2]++
		} else if txnCount <= 8 {
			sizeCounts[3]++
		} else if txnCount <= 16 {
			sizeCounts[4]++
		} else if txnCount <= 32 {
			sizeCounts[5]++
		} else {
			sizeCounts[6]++
		}

		execCount := l.groupExecCnt[g.hash]
		if execCount == 0 {
			execCounts[0]++
		} else if execCount == 1 {
			execCounts[1]++
		} else if execCount <= 2 {
			execCounts[2]++
		} else if execCount <= 4 {
			execCounts[3]++
		} else if execCount <= 8 {
			execCounts[4]++
		} else if execCount <= 16 {
			execCounts[5]++
		} else {
			execCounts[6]++
		}

		if g.isChainDep() {
			chainDepCount++
		}
		if g.isCoinbaseDep() {
			coinbaseDepCount++
		}
		if g.isTimestampDep() {
			timeStampDepCount++
		}

	}

	sizeDistribution = fmt.Sprintf("[1|2|4|8|16|36|>36]-[%d:%d:%d:%d:%d:%d:%d]", sizeCounts[0],
		sizeCounts[1], sizeCounts[2], sizeCounts[3], sizeCounts[4], sizeCounts[5], sizeCounts[6])

	orderDistribution = fmt.Sprintf("[1|2|6|24|120|720|>720]-[%d:%d:%d:%d:%d:%d:%d]", orderCounts[0],
		orderCounts[1], orderCounts[2], orderCounts[3], orderCounts[4], orderCounts[5], orderCounts[6])

	execDistribution = fmt.Sprintf("[0|1|2|4|8|16|>16]-[%d:%d:%d:%d:%d:%d:%d]", execCounts[0],
		execCounts[1], execCounts[2], execCounts[3], execCounts[4], execCounts[5], execCounts[6])

	chainDepDistribution = fmt.Sprintf("%d[cb|ts]-[%d:%d]", chainDepCount, coinbaseDepCount, timeStampDepCount)

	return
}

func (l *PreplayLog) printAndClearLog(block uint64, remain int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	context := []interface{}{"number", block, "package", fmt.Sprintf("%d(%d)", l.packageCnt, l.simplePackageCnt)}
	if l.packageCnt > 0 {
		context = append(context, "avgPackage", common.PrettyDuration(int64(time.Since(l.lastUpdate))/int64(l.packageCnt)))
	}
	sizeDistr, orderDistr, execDistr, chainDistr := l.getGroupDistribution()
	totalGroupExecCnt := 0
	for _, cnt := range l.groupExecCnt {
		totalGroupExecCnt += cnt
	}
	context = append(
		context, "build", l.taskBuildCnt, "deadline", len(l.deadlineLog),
		"group", fmt.Sprintf("%d(%d)-%d", len(l.groupLog), len(l.groupEnd), totalGroupExecCnt),
		"transaction", fmt.Sprintf("%d-%d", len(l.txnLog), l.txnExecCnt),
		"object", fmt.Sprintf("%d(%d)-%d", l.wobjectCopyCnt, len(l.wobjectAddr), l.wobjectNotCopyCnt),
		"unfinishedGroup", remain, "duration", common.PrettyDuration(time.Since(l.lastUpdate)),
		"gSizeDist", sizeDistr, "gOrderDist", orderDistr, "gExecDist", execDistr, "gChainDistr", chainDistr,
		"rGroup", fmt.Sprintf("%d(%d)", len(l.remainGroupLog), l.remainGroupExecCnt),
		"rTxn", fmt.Sprintf("%d-%d", len(l.remainTxnLog), l.remainTxnExecCnt),
	)
	log.Info("In last block", context...)

	l.lastUpdate = time.Now()
	l.packageCnt = 0
	l.simplePackageCnt = 0
	l.taskBuildCnt = 0
	l.deadlineLog = make(map[uint64]struct{})
	l.groupLog = make(map[common.Hash]*TxnGroup)
	l.groupExecCnt = make(map[common.Hash]int)
	l.groupEnd = make(map[common.Hash]struct{})
	l.txnLog = make(map[common.Hash]struct{})
	l.remainGroupLog = make(map[common.Hash]*TxnGroup)
	l.remainTxnLog = make(map[common.Hash]struct{})
	l.wobjectCopyCnt = 0
	l.wobjectNotCopyCnt = 0
	l.wobjectAddr = make(map[common.Address]struct{})
	l.groupExecCnt = make(map[common.Hash]int)
	l.txnExecCnt = 0
	l.remainGroupExecCnt = 0
	l.remainTxnExecCnt = 0
}
