package optipreplayer

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/params"
	"math"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// leftwardsStep is the step size of deadline move leftwards.
	leftwardsStep = 10
)

type TaskBuilder struct {
	config *params.ChainConfig
	engine consensus.Engine
	eth    Backend
	chain  *core.BlockChain

	startCh      chan struct{}
	finishOnceCh chan struct{}
	exitCh       chan struct{}

	extra []byte

	// The lock used to protect the fields updated when new block enters
	mu *sync.RWMutex

	// Cache
	globalCache *cache.GlobalCache

	// Listener
	listener *Listener

	// Package
	followLatest bool
	packagePool  TransactionPool
	addedTxn     TransactionPool
	removedTxn   bool
	signer       types.Signer
	minerList    *MinerList
	reachLast    *int32
	nowStart     uint64 // only valid for followLatest is false
	lastBaseline uint64
	txnBaseline  uint64
	txnDeadline  uint64

	// Transaction distributor
	trigger    *Trigger
	nowHeader  *Header
	nowGroups  map[common.Hash]*TxnGroup
	parent     *types.Block
	rwrecord   map[common.Hash]*RWRecord
	rwrecordMu sync.RWMutex

	// Task queue
	taskQueue *TaskQueue

	// Log groups and transactions
	preplayLog *PreplayLog
}

func NewTaskBuilder(config *params.ChainConfig, engine consensus.Engine, eth Backend, mu *sync.RWMutex, listener *Listener,
	followLatest bool, minerList *MinerList, nowHeader *Header, taskQueue *TaskQueue,
	preplayLog *PreplayLog) *TaskBuilder {
	builder := &TaskBuilder{
		config:       config,
		engine:       engine,
		eth:          eth,
		chain:        eth.BlockChain(),
		startCh:      make(chan struct{}, 1),
		finishOnceCh: make(chan struct{}),
		exitCh:       make(chan struct{}),
		mu:           mu,
		followLatest: followLatest,
		packagePool:  make(TransactionPool),
		addedTxn:     make(TransactionPool),
		signer:       types.NewEIP155Signer(config.ChainID),
		minerList:    minerList,
		reachLast:    new(int32),
		lastBaseline: uint64(time.Now().Unix()),
		trigger:      NewTrigger("TxsBlock1P1", 1),
		nowHeader:    nowHeader,
		nowGroups:    make(map[common.Hash]*TxnGroup),
		rwrecord:     make(map[common.Hash]*RWRecord),
		taskQueue:    taskQueue,
		preplayLog:   preplayLog,
	}
	if followLatest {
		builder.listener = listener
	}
	return builder
}

func (b *TaskBuilder) mainLoop() {
	for {
		select {
		case <-b.startCh:
			b.mu.RLock()
			rawPending, _ := b.eth.TxPool().Pending()
			currentBlock := b.chain.CurrentBlock()
			if b.parent != nil {
				if currentBlock.Root() == b.parent.Root() {
					b.resetPackagePool(rawPending)
					b.preplayLog.reportNewPackage(!b.removedTxn)
					if b.removedTxn || len(b.addedTxn) > 0 {
						b.commitNewWork()
						b.updateTxnGroup()
						b.preplayLog.reportNewTaskBuild()
					}
				} else {
					log.Info("Diff between chain head and parent!!!", "followLatest", b.followLatest,
						"chain head", fmt.Sprintf("%d-%s-%s",
							currentBlock.NumberU64(), currentBlock.Hash().TerminalString(), currentBlock.Root().TerminalString()),
						"parent", fmt.Sprintf("%d-%s-%s",
							b.parent.NumberU64(), b.parent.Hash().TerminalString(), b.parent.Root().TerminalString()),
					)
				}
			}
			b.mu.RUnlock()
			b.finishOnceCh <- struct{}{}
		case <-b.exitCh:
			return
		}
	}
}

func (b *TaskBuilder) resetPackagePool(rawPending TransactionPool) {
	originPool := b.packagePool
	b.packagePool = make(TransactionPool)

	// inWhiteList the whitelist txns in advance
	for from := range b.minerList.whiteList {
		if txns, ok := rawPending[from]; ok {
			b.packagePool[from] = txns
			delete(rawPending, from)
		}
	}

	isTxTooLate := func(txn *types.Transaction) bool {
		if txnListen := b.globalCache.GetTxListen(txn.Hash()); txnListen != nil {
			return txnListen.ListenTime > b.txnDeadline
		}
		return true
	}

	var (
		currentPrice                   = new(big.Int)
		minPrice                       *big.Int
		currentPriceStartingCumGasUsed uint64
		cumGasUsed                     uint64
		currentPriceCumGasUsedBySender = make(map[common.Address]uint64)
		pending                        = types.NewTransactionsByPriceAndNonce(b.signer, rawPending, nil, true)
		poppedSenders                  = make(map[common.Address]struct{})
	)

	for pending.Peek() != nil {
		txn := pending.Peek()

		if isTxTooLate(txn) {
			pending.Pop()
			continue
		}

		gasPrice := txn.GasPrice()
		if gasPrice.Cmp(currentPrice) != 0 {
			if cumGasUsed+params.TxGas > b.nowHeader.gasLimit {
				break
			}

			currentPrice = gasPrice
			currentPriceStartingCumGasUsed = cumGasUsed
			currentPriceCumGasUsedBySender = make(map[common.Address]uint64)
		}

		sender, _ := types.Sender(b.signer, txn)
		gasUsed := b.globalCache.GetGasUsedCache(sender, txn)

		if _, senderPopped := poppedSenders[sender]; !senderPopped {
			if cumGasUsed+txn.Gas() <= b.nowHeader.gasLimit {
				cumGasUsed += gasUsed
			} else {
				poppedSenders[sender] = struct{}{}
			}
		}
		pending.Shift()

		senderCumGasUsed, ok := currentPriceCumGasUsedBySender[sender]
		if !ok {
			senderCumGasUsed = currentPriceStartingCumGasUsed
		}
		if senderCumGasUsed+txn.Gas() <= b.nowHeader.gasLimit {
			if b.packagePool.addTxn(sender, txn) {
				currentPriceCumGasUsedBySender[sender] = senderCumGasUsed + gasUsed
				if minPrice == nil {
					minPrice = gasPrice
				} else {
					if minPrice.Cmp(gasPrice) >= 0 {
						minPrice = gasPrice
					}
				}
			}
		}
	}

	b.addedTxn = b.packagePool.txnsPoolDiff(originPool)
	b.removedTxn = originPool.isTxnsPoolLarge(b.packagePool)

	b.preplayLog.reportNewDeadline(b.txnDeadline)
	b.txnDeadline = b.nextTxnDeadline(b.txnDeadline)
	if b.followLatest {
		b.listener.setMinPrice(minPrice)
	}

	nowTime := time.Now()
	for _, txn := range b.packagePool {
		for _, tx := range txn {
			b.globalCache.CommitTxPackage(tx.Hash(), uint64(nowTime.Unix()))
		}
	}
}

func (b *TaskBuilder) commitNewWork() {
	parentHash := b.parent.Hash()
	parentNumber := b.parent.Number()
	parentNumberU64 := b.parent.NumberU64()

	gasLimit := uint64(0)
	pendingTxn := make(TransactionPool)
	for from, txns := range b.addedTxn {
		pendingTxn[from] = b.packagePool[from]
		for _, txn := range txns {
			gasLimit += txn.Gas()
		}
	}

	totalDifficulty := b.chain.GetTd(parentHash, parentNumberU64)
	if totalDifficulty == nil {
		totalDifficulty = new(big.Int)
	}
	currentState := &cache.CurrentState{
		PreplayName:       b.trigger.Name,
		Number:            parentNumberU64,
		Hash:              parentHash.Hex(),
		RawHash:           parentHash,
		Txs:               b.parent.Transactions(),
		TotalDifficulty:   totalDifficulty.String(),
		SnapshotTimestamp: time.Now().UnixNano() / 1000000,
	}

	header := &types.Header{
		ParentHash: parentHash,
		Number:     parentNumber.Add(parentNumber, common.Big1),
		GasLimit:   gasLimit,
		Coinbase:   b.nowHeader.coinbase,
		Extra:      b.extra,
		Time:       b.nowHeader.time,
	}

	executor := NewExecutor("0", b.config, b.engine, b.chain, b.eth.ChainDb(), nil, pendingTxn, currentState,
		b.trigger, nil, false, true, false)

	executor.RoundID = b.globalCache.NewRoundID()

	// Execute, use pending for preplay
	executor.commit(header.Coinbase, b.parent, header, pendingTxn)

	b.updateDependency(executor.RoundID, pendingTxn)

	b.chain.Warmuper.AddWarmupTask(executor.RoundID, executor.executionOrder, b.parent.Root())
}

func (b *TaskBuilder) updateTxnGroup() {
	if b.removedTxn {
		b.nowGroups = make(map[common.Hash]*TxnGroup)
		b.groupTxns(b.packagePool)
	} else {
		b.groupTxns(b.addedTxn)
	}

	for groupHash, group := range b.nowGroups {
		if originGroup, exist := b.preplayLog.reportNewGroup(group); exist {
			b.nowGroups[groupHash] = originGroup
			continue
		}

		for _, txns := range group.txns {
			sort.Sort(types.TxByNonce(txns))
		}
		group.setValid()
		group.preplayCountMap = make(map[common.Hash]int, group.txns.size())
		group.parent = b.parent
		group.txnCount = group.txns.size()
		group.chainFactor = 1
		if group.isCoinbaseDep() {
			group.chainFactor *= topActiveCount
		}
		if group.isTimestampDep() {
			group.chainFactor *= len(timeShift)
		}
		group.orderCount, group.startList, group.subpoolList, group.nextInList = divideTransactionPool(group.txns, group.chainFactor)
		group.nextOrderAndHeader = make(chan OrderAndHeader)

		// start walk for new group
		go func(group *TxnGroup) {
			lastIndex := len(group.subpoolList) - 1
			walkTxnsPool(lastIndex, group.startList[lastIndex], group, make(TxnOrder, group.txnCount), b)
			close(group.nextOrderAndHeader)
			b.preplayLog.reportGroupEnd(group)
		}(group)

		b.taskQueue.pushTask(group)

		nowTime := time.Now()
		for _, txns := range group.txns {
			for _, txn := range txns {
				b.globalCache.CommitTxEnqueue(txn.Hash(), uint64(nowTime.Unix()))
			}
		}
	}
}

func (b *TaskBuilder) chainHeadUpdate(block *types.Block) {
	if b.followLatest {
		b.listener.setMinPrice(new(big.Int))
	}
	b.lastBaseline = b.txnBaseline
	if blockPre := b.globalCache.PeekBlockPre(block.Hash()); blockPre != nil {
		b.txnBaseline = blockPre.ListenTime
	} else {
		b.txnBaseline = block.Time()
	}
	if b.followLatest {
		b.txnDeadline = b.txnBaseline
	} else {
		b.nowStart = b.txnBaseline - leftwardsStep
		if b.nowStart <= b.lastBaseline {
			b.nowStart = b.lastBaseline + 1
		}
		b.txnDeadline = b.nowStart
	}
	b.parent = block
	b.clearRWRecord()
}

var getFactorial = func() func(n int64) *big.Int {
	var factorCacheMu sync.Mutex
	factorCache := map[int64]*big.Int{
		0: new(big.Int).SetUint64(1),
		1: new(big.Int).SetUint64(1),
		2: new(big.Int).SetUint64(2),
		3: new(big.Int).SetUint64(6),
	}
	return func(n int64) *big.Int {
		factorCacheMu.Lock()
		defer factorCacheMu.Unlock()

		if factor, ok := factorCache[n]; ok {
			return new(big.Int).Set(factor)
		}
		var factor = new(big.Int).SetInt64(1)
		for mul := n; mul >= 1; mul-- {
			if cache, ok := factorCache[mul]; ok {
				factor.Mul(factor, cache)
				factorCache[n] = new(big.Int).Set(factor)
				return factor
			}
			factor.Mul(factor, new(big.Int).SetInt64(mul))
		}
		return factor
	}
}()

func divideTransactionPool(txns TransactionPool, chainFactor int) (orderCount *big.Int, startList []int,
	subpoolList []TransactionPool, nextInList []map[common.Address]int) {
	txnCount := txns.size()
	orderCount = new(big.Int).SetUint64(uint64(chainFactor))
	var (
		inPoolList int
		poolCpy    = txns.copy()
	)
	for inPoolList < txnCount {
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
		orderCount.Mul(orderCount, factor)
		startList = append(startList, inPoolList)
		subpoolList = append(subpoolList, subPool)
		nextIn := make(map[common.Address]int, len(subPool))
		for from := range subPool {
			nextIn[from] = 0
		}
		nextInList = append(nextInList, nextIn)

		inPoolList += subPool.size()
	}
	return
}

// timeShift denotes 0, -1, 1, -2, 2
var timeShift = []uint64{0, math.MaxUint64, 1, math.MaxUint64 - 1, 2}

func walkTxnsPool(subpoolLoc int, txnLoc int, group *TxnGroup, order TxnOrder, b *TaskBuilder) {
	// boundaries of recursion
	if !group.isValid() {
		return
	}
	if group.isSubInOrder(subpoolLoc) {
		if subpoolLoc == 0 {
			orderAndHeader := OrderAndHeader{order: make(TxnOrder, len(order))}
			copy(orderAndHeader.order, order)
			if group.isChainDep() {
				var (
					coinbaseTryCount  = 1
					timestampTryCount = 1
				)
				if group.isCoinbaseDep() {
					coinbaseTryCount = topActiveCount
				}
				if group.isTimestampDep() {
					timestampTryCount = len(timeShift)
				}
				for i := 0; i < coinbaseTryCount; i++ {
					for j := 0; j < timestampTryCount; j++ {
						if group.isValid() {
							orderAndHeader.header.coinbase = b.minerList.topActive[i]
							orderAndHeader.header.time = b.nowHeader.time + timeShift[j]
							orderAndHeader.header.gasLimit = b.nowHeader.gasLimit
							group.nextOrderAndHeader <- orderAndHeader
						}
					}
				}
			} else {
				orderAndHeader.header.coinbase = b.nowHeader.coinbase
				orderAndHeader.header.time = b.nowHeader.time
				orderAndHeader.header.gasLimit = b.nowHeader.gasLimit
				group.nextOrderAndHeader <- orderAndHeader
			}
			return
		} else {
			subpoolLoc--
			txnLoc = group.startList[subpoolLoc]
		}
	}

	subpool := group.subpoolList[subpoolLoc]
	nextIn := group.nextInList[subpoolLoc]
	var (
		maxPriceFrom = make([]common.Address, 0)
		maxPrice     = new(big.Int)
	)
	for from, txns := range subpool {
		nextIndex := nextIn[from]
		if nextIndex >= len(txns) {
			continue
		}
		headTxn := txns[nextIndex]
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
	sort.Slice(maxPriceFrom, func(i, j int) bool {
		return bytes.Compare(maxPriceFrom[i].Bytes(), maxPriceFrom[j].Bytes()) < 0
	})
	for _, from := range maxPriceFrom {
		txn := subpool[from][nextIn[from]]
		order[txnLoc] = txn.Hash()
		nextIn[from]++
		walkTxnsPool(subpoolLoc, txnLoc+1, group, order, b)
		nextIn[from]--
	}
}

func (b *TaskBuilder) nextTxnDeadline(nowValue uint64) uint64 {
	if b.followLatest {
		if nowValue+1 <= uint64(time.Now().Unix()) {
			return nowValue + 1
		} else {
			return nowValue
		}
	} else {
		if nowValue+1-b.nowStart >= leftwardsStep {
			if b.nowStart <= b.lastBaseline+1 {
				atomic.StoreInt32(b.reachLast, 1)
			} else {
				b.nowStart -= leftwardsStep
				if b.nowStart <= b.lastBaseline {
					b.nowStart = b.lastBaseline + 1
				}
				nowValue = b.nowStart
			}
			return nowValue
		} else {
			return nowValue + 1
		}
	}
}

func (b *TaskBuilder) updateDependency(roundID uint64, pool TransactionPool) {
	for _, txns := range pool {
		for _, txn := range txns {
			txnHash := txn.Hash()
			txPreplay := b.globalCache.GetTxPreplay(txnHash)
			if txPreplay != nil {
				if round, ok := txPreplay.PeekRound(roundID); ok {
					b.insertRWRecord(txnHash, NewRWRecord(round.RWrecord))
					continue
				}
			}
		}
	}
}

func (b *TaskBuilder) groupTxns(pool TransactionPool) {
	for from, txns := range pool {
		relativeGroup := make(map[common.Hash]struct{})
		for _, txn := range txns {
			for groupHash, group := range b.nowGroups {
				if _, ok := relativeGroup[groupHash]; ok {
					continue
				}
				if b.isSameGroup(from, txn.Hash(), group) {
					relativeGroup[groupHash] = struct{}{}
				}
			}
		}

		newGroup := make(TransactionPool)
		rwrecord := NewRWRecord(nil)
		var res [][]byte
		for groupHash := range relativeGroup {
			addGroup := b.nowGroups[groupHash]
			rwrecord.merge(addGroup.RWRecord)
			for addr, txns := range addGroup.txns {
				newGroup[addr] = append(newGroup[addr], txns...)
				for _, txn := range txns {
					res = append(res, txn.Hash().Bytes())
				}
			}
			delete(b.nowGroups, groupHash)
		}
		newGroup[from] = append(newGroup[from], txns...)
		for _, txn := range txns {
			rwrecord.merge(b.selectRWRecord(txn.Hash()))
			res = append(res, txn.Hash().Bytes())
		}
		sort.Slice(res, func(i, j int) bool {
			return bytes.Compare(res[i], res[j]) == -1
		})
		groupHash := crypto.Keccak256Hash(res...)
		b.nowGroups[groupHash] = &TxnGroup{
			hash:     groupHash,
			txns:     newGroup,
			RWRecord: rwrecord,
		}
	}
}

func (b *TaskBuilder) isSameGroup(sender common.Address, txn common.Hash, group *TxnGroup) bool {
	if _, ok := group.txns[sender]; ok {
		return true
	}
	rwrecord := b.selectRWRecord(txn)
	return rwrecord.isRWOverlap(group.RWRecord) || group.isRWOverlap(rwrecord)
}

// setExtra sets the content used to initialize the block extra field.
func (b *TaskBuilder) setExtra(extra []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.extra = extra
}

func (b *TaskBuilder) setGlobalCache(globalCache *cache.GlobalCache) {
	b.globalCache = globalCache
}

func (b *TaskBuilder) insertRWRecord(txnHash common.Hash, rwrecord *RWRecord) {
	b.rwrecordMu.Lock()
	defer b.rwrecordMu.Unlock()

	b.rwrecord[txnHash] = rwrecord
}

func (b *TaskBuilder) clearRWRecord() {
	b.rwrecordMu.Lock()
	defer b.rwrecordMu.Unlock()

	b.rwrecord = make(map[common.Hash]*RWRecord)
}

func (b *TaskBuilder) selectRWRecord(txn common.Hash) *RWRecord {
	b.rwrecordMu.RLock()
	defer b.rwrecordMu.RUnlock()

	if rwrecord, ok := b.rwrecord[txn]; ok {
		return rwrecord
	} else {
		return NewRWRecord(nil)
	}
}

func (b *TaskBuilder) close() {
	close(b.exitCh)
}
