package optipreplayer

import (
	"bytes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/params"
	"math"
	"math/big"
	"sort"
	"sync"
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

	// Package
	packageType  PackageType
	originPool   TransactionPool
	packagePool  TransactionPool
	removedTxn   bool
	preplayPool  TransactionPool
	minerList    *MinerList
	signer       types.Signer
	lastBaseline uint64
	txnBaseline  uint64
	txnDeadline  uint64
	setMinPrice  func(price *big.Int)

	// Transaction distributor
	trigger    *Trigger
	nowHeader  *Header
	nowRoundID uint64
	nowGroups  map[common.Hash]*TxnGroup
	parent     *types.Block
	rwrecord   map[common.Hash]*RWRecord
	rwrecordMu sync.RWMutex

	// Task queue
	taskQueue *TaskQueue

	// Log groups and transactions
	preplayLog *PreplayLog

	preplayer *Preplayer
}

func NewTaskBuilder(config *params.ChainConfig, engine consensus.Engine, eth Backend, mu *sync.RWMutex, packageType PackageType) *TaskBuilder {
	builder := &TaskBuilder{
		config:       config,
		engine:       engine,
		eth:          eth,
		chain:        eth.BlockChain(),
		startCh:      make(chan struct{}, 1),
		finishOnceCh: make(chan struct{}),
		exitCh:       make(chan struct{}),
		mu:           mu,
		packageType:  packageType,
		originPool:   make(TransactionPool),
		packagePool:  make(TransactionPool),
		preplayPool:  make(TransactionPool),
		signer:       types.NewEIP155Signer(config.ChainID),
		lastBaseline: uint64(time.Now().Unix()),
		setMinPrice:  func(price *big.Int) {},
		trigger:      NewTrigger("TxsBlock1P1", 1),
		nowGroups:    make(map[common.Hash]*TxnGroup),
		rwrecord:     make(map[common.Hash]*RWRecord),
	}
	return builder
}

func (b *TaskBuilder) setPreplayer(preplayer *Preplayer) {
	b.preplayer = preplayer

	b.minerList = preplayer.minerList
	b.nowHeader = preplayer.nowHeader
	b.taskQueue = preplayer.taskQueue
	b.preplayLog = preplayer.preplayLog
}

func (b *TaskBuilder) mainLoop0() {
	for {
		select {
		case <-b.startCh:
			b.mu.RLock()
			rawPending, _ := b.eth.TxPool().Pending()
			currentBlock := b.chain.CurrentBlock()
			if b.parent != nil && currentBlock.Root() == b.parent.Root() {
				b.resetPackagePool(rawPending)
				b.resetPreplayPool()
				if b.removedTxn || len(b.preplayPool) > 0 {
					b.commitNewWork(false)
					b.updateDependency(b.nowRoundID, b.preplayPool)
					b.updateTxnGroup()
				}
			}
			b.mu.RUnlock()
			b.finishOnceCh <- struct{}{}
		case <-b.exitCh:
			return
		}
	}
}

func (b *TaskBuilder) mainLoop1() {
	for {
		select {
		case <-b.startCh:
			b.mu.RLock()
			rawPending, _ := b.eth.TxPool().Pending()
			currentBlock := b.chain.CurrentBlock()
			if b.parent != nil && currentBlock.Root() == b.parent.Root() {
				b.resetPackagePool(rawPending)
				b.resetPreplayPool()
				if len(b.preplayPool) > 0 {
					b.commitNewWork(true)
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
	b.originPool, b.packagePool = b.packagePool, make(TransactionPool)

	if b.packageType == TYPE0 {
		// inWhiteList the whitelist txns in advance
		for from := range b.minerList.whiteList {
			if txns, ok := rawPending[from]; ok {
				b.packagePool[from] = txns
				delete(rawPending, from)
			}
		}

		var (
			txnWithMinPrice *types.Transaction // Denote min price for avoiding call types.Transaction.GasPrice()
			pending         = types.NewTransactionsByPriceAndNonce(b.signer, rawPending, nil, true)
			poppedSenders   = make(map[common.Address]struct{})
		)

		for range b.packageType.packageRatio() {
			var (
				cumGasUsed                     uint64
				currentPriceStartingCumGasUsed uint64
				currentPriceCumGasUsedBySender = make(map[common.Address]uint64)
			)

			if pending.Peek() == nil {
				break
			}
			// Denote current price for avoiding call types.Transaction.GasPrice()
			for txnWithCurrentPrice := pending.Peek(); pending.Peek() != nil; {
				txn := pending.Peek()

				if b.isTxTooLate(txn) {
					pending.Pop()
					continue
				}

				if txn.CmpGasPriceWithTxn(txnWithCurrentPrice) != 0 {
					txnWithCurrentPrice = txn
					currentPriceStartingCumGasUsed = cumGasUsed
					currentPriceCumGasUsedBySender = make(map[common.Address]uint64)

					if cumGasUsed+params.TxGas > b.nowHeader.gasLimit {
						break
					}
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
						if txnWithMinPrice == nil || txnWithMinPrice.CmpGasPriceWithTxn(txn) >= 0 {
							txnWithMinPrice = txn
						}
					}
				}
			}
		}

		if txnWithMinPrice != nil {
			b.setMinPrice(txnWithMinPrice.GasPrice())
		}
	} else {
		for from, txns := range rawPending {
			for _, txn := range txns {
				if b.isTxTooLate(txn) {
					break
				} else {
					b.packagePool.addTxn(from, txn)
				}
			}
		}
	}

	b.removedTxn = b.originPool.isTxnsPoolLarge(b.packagePool)
	b.preplayLog.reportNewPackage(!b.removedTxn)
	b.preplayLog.reportNewDeadline(b.txnDeadline)
	b.txnDeadline = b.nextTxnDeadline(b.txnDeadline)

	nowTime := uint64(time.Now().UnixNano())
	for _, txn := range b.packagePool {
		for _, tx := range txn {
			b.globalCache.CommitTxPackage(tx.Hash(), nowTime)
		}
	}
}

func (b *TaskBuilder) resetPreplayPool() {
	if b.packageType == TYPE0 {
		b.preplayPool = b.packagePool.filter(func(sender common.Address, txn *types.Transaction) bool {
			return !b.originPool.isTxnIn(sender, txn)
		})
	} else {
		b.preplayPool = b.packagePool.filter(func(sender common.Address, txn *types.Transaction) bool {
			return b.globalCache.GetTxPreplay(txn.Hash()) == nil
		})
		nowTime := uint64(time.Now().UnixNano())
		for _, txns := range b.preplayPool {
			for _, txn := range txns {
				b.globalCache.CommitTxEnqueue(txn.Hash(), nowTime)
			}
		}
	}
}

func (b *TaskBuilder) commitNewWork(basicPreplay bool) {
	parentHash := b.parent.Hash()
	parentNumber := b.parent.Number()
	parentNumberU64 := b.parent.NumberU64()

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
		GasLimit:   b.preplayPool.gas(),
		Coinbase:   b.nowHeader.coinbase,
		Extra:      b.extra,
		Time:       b.nowHeader.time,
	}

	executor := NewExecutor("0", b.config, b.engine, b.chain, b.eth.ChainDb(), nil, b.preplayPool, currentState,
		b.trigger, nil, false, true, basicPreplay)

	//executor.EnableReuseTracer = true

	executor.RoundID = b.globalCache.NewRoundID()

	// Execute, use pending for preplay
	executor.commit(header.Coinbase, b.parent, header, b.preplayPool)

	b.nowRoundID = executor.RoundID
	b.chain.Warmuper.AddWarmupTask(executor.RoundID, executor.executionOrder, b.parent.Root())
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

func (b *TaskBuilder) updateTxnGroup() {
	if b.removedTxn {
		b.nowGroups = make(map[common.Hash]*TxnGroup)
		b.groupTxns(b.packagePool)
	} else {
		b.groupTxns(b.preplayPool)
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
		group.finishPreplay = make(map[common.Hash]int, group.txns.size())
		group.failPreplay = make(map[common.Hash][]string, group.txns.size())
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

		nowTime := uint64(time.Now().UnixNano())
		for _, txns := range group.txns {
			for _, txn := range txns {
				b.globalCache.CommitTxEnqueue(txn.Hash(), nowTime)
			}
		}
	}

	b.preplayLog.reportNewTaskBuild()
}

func (b *TaskBuilder) chainHeadUpdate(block *types.Block) {
	b.originPool = make(TransactionPool)
	b.packagePool = make(TransactionPool)
	b.preplayPool = make(TransactionPool)
	b.setMinPrice(common.Big0)
	b.lastBaseline = b.txnBaseline
	if blockPre := b.globalCache.PeekBlockPre(block.Hash()); blockPre != nil {
		b.txnBaseline = blockPre.ListenTime
	} else {
		b.txnBaseline = block.Time()
	}
	if b.packageType == TYPE2 {
		b.txnDeadline = b.txnBaseline - leftwardsStep
	} else {
		b.txnDeadline = b.txnBaseline
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
			maxPriceFrom    = make([]common.Address, 0)
			txnWithMaxPrice *types.Transaction
		)
		for from, txns := range poolCpy {
			if len(txns) == 0 {
				continue
			}
			headTxn := txns[0]
			if txnWithMaxPrice == nil {
				txnWithMaxPrice = headTxn
			}
			cmp := headTxn.CmpGasPriceWithTxn(txnWithMaxPrice)
			switch {
			case cmp > 0:
				txnWithMaxPrice = headTxn
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
				cmp := txns[index].CmpGasPriceWithTxn(txnWithMaxPrice)
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
		maxPriceFrom    = make([]common.Address, 0)
		txnWithMaxPrice *types.Transaction
	)
	for from, txns := range subpool {
		nextIndex := nextIn[from]
		if nextIndex >= len(txns) {
			continue
		}
		headTxn := txns[nextIndex]
		if txnWithMaxPrice == nil {
			txnWithMaxPrice = headTxn
		}
		cmp := headTxn.CmpGasPriceWithTxn(txnWithMaxPrice)
		switch {
		case cmp > 0:
			txnWithMaxPrice = headTxn
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

func (b *TaskBuilder) isTxTooLate(txn *types.Transaction) bool {
	if txnListen := b.globalCache.GetTxListen(txn.Hash()); txnListen != nil {
		return txnListen.ListenTime > b.txnDeadline
	}
	return true
}

func (b *TaskBuilder) nextTxnDeadline(nowValue uint64) uint64 {
	if b.packageType == TYPE1 {
		return nowValue
	} else {
		if nowValue+1 <= uint64(time.Now().Unix()) {
			return nowValue + 1
		} else {
			return nowValue
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
