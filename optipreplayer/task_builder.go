package optipreplayer

import (
	"bytes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/params"
	"math/big"
	"sort"
	"sync"
	"time"
)

const (
	// chainHeadChanSize is the size of channel listening to ChainHeadEvent.
	chainHeadChanSize = 10
	// deadlineStart is the start of deadline's shift.
	deadlineStart = -10
)

type TaskBuilder struct {
	config *params.ChainConfig
	engine consensus.Engine
	eth    Backend
	chain  *core.BlockChain

	gasFloor uint64
	gasCeil  uint64
	gasLimit uint64

	// Subscriptions
	chainHeadCh  chan core.ChainHeadEvent
	chainHeadSub event.Subscription

	startCh chan struct{}
	exitCh  chan struct{}

	mu    sync.RWMutex // The lock used to protect the extra fields
	extra []byte

	trigger *Trigger

	// Cache
	globalCache *cache.GlobalCache

	// Listener
	listener *Listener

	// Package
	packagePool TransactionPool
	addedTxn    TransactionPool
	removedTxn  bool
	signer      types.Signer
	minerList   MinerList
	whiteList   map[common.Address]struct{}
	txnBaseline uint64
	txnDeadline uint64

	// Transaction distributor
	nowHeader    Header
	nowGroups    map[common.Hash]*TxnGroup
	pastGroups   map[common.Hash]*TxnGroup
	pastGroupsMu sync.RWMutex
	parent       *types.Block
	rwrecord     map[common.Hash]*RWRecord

	// Task queue
	taskQueue *TaskQueue

	// Log groups and transactions
	preplayLog *PreplayLog
}

func NewTaskBuilder(config *params.ChainConfig, engine consensus.Engine, eth Backend, gasFloor, gasCeil uint64, listener *Listener) *TaskBuilder {
	chain := eth.BlockChain()
	chainHeadCh := make(chan core.ChainHeadEvent, chainHeadChanSize)
	trigger := &Trigger{
		Name:        "TxsBlock1P1",
		ExecutorNum: 1,
	}
	builder := &TaskBuilder{
		config:       config,
		engine:       engine,
		eth:          eth,
		chain:        chain,
		gasFloor:     gasFloor,
		gasCeil:      gasCeil,
		chainHeadCh:  chainHeadCh,
		chainHeadSub: chain.SubscribeChainHeadEvent(chainHeadCh),
		startCh:      make(chan struct{}, 1),
		exitCh:       make(chan struct{}),
		trigger:      trigger,
		listener:     listener,
		packagePool:  make(TransactionPool),
		addedTxn:     make(TransactionPool),
		signer:       types.NewEIP155Signer(config.ChainID),
		minerList:    NewMinerList(chain),
		whiteList:    make(map[common.Address]struct{}),
		nowGroups:    make(map[common.Hash]*TxnGroup),
		pastGroups:   make(map[common.Hash]*TxnGroup),
		rwrecord:     make(map[common.Hash]*RWRecord),
		taskQueue:    NewTaskQueue(),
		preplayLog:   NewPreplayLog(),
	}
	return builder
}

func (b *TaskBuilder) mainLoop() {
	defer b.chainHeadSub.Unsubscribe()

	waitNewTxCh := make(chan struct{})
	go func() {
		for {
			b.listener.waitForNewTx()
			b.startCh <- struct{}{}
			<-waitNewTxCh
		}
	}()

	for {
		select {
		case <-b.startCh:
			//b.listener.waitForNewTx()
			b.resetPackagePool()
			b.preplayLog.reportNewPackage()
			if b.removedTxn || len(b.addedTxn) > 0 {
				b.commitNewWork()
				b.updateTxnGroup()
				b.preplayLog.reportNewBuild()
			}
			waitNewTxCh <- struct{}{}
			//b.startCh <- struct{}{}
		case chainHeadEvent := <-b.chainHeadCh:
			currentBlock := chainHeadEvent.Block

			b.chainHeadUpdate(currentBlock)
			b.preplayLog.printAndClearLog(currentBlock.NumberU64(), b.taskQueue.countTask())
		case <-b.exitCh:
			return
		}
	}
}

func (b *TaskBuilder) resetPackagePool() {
	originPool := b.packagePool
	b.packagePool = make(TransactionPool)
	var rawPending TransactionPool
	rawPending, _ = b.eth.TxPool().Pending()

	// inWhiteList the whitelist txns in advance
	for from := range b.whiteList {
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
			if cumGasUsed+params.TxGas > b.gasLimit {
				break
			}

			currentPrice = gasPrice
			currentPriceStartingCumGasUsed = cumGasUsed
			currentPriceCumGasUsedBySender = make(map[common.Address]uint64)
		}

		sender, _ := types.Sender(b.signer, txn)
		gasUsed := b.globalCache.GetGasUsedCache(sender, txn)

		if _, senderPopped := poppedSenders[sender]; !senderPopped {
			if cumGasUsed+txn.Gas() <= b.gasLimit {
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
		if senderCumGasUsed+txn.Gas() <= b.gasLimit {
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

	var isShift bool
	b.txnDeadline, isShift = b.nextTxnDeadline(b.txnDeadline)
	if isShift {
		b.preplayLog.reportNewDeadline()
	}
	b.listener.setMinPrice(minPrice)

	nowTime := time.Now()
	for _, txn := range b.packagePool {
		for _, tx := range txn {
			b.globalCache.CommitTxPackage(tx.Hash(), uint64(nowTime.Unix()))
		}
	}
}

func (b *TaskBuilder) commitNewWork() {
	b.mu.RLock()
	defer b.mu.RUnlock()

	parent := b.parent
	parentHash := parent.Hash()
	parentNumber := parent.Number()

	gasLimit := uint64(0)
	pendingTxn := make(TransactionPool)
	pendingList := types.Transactions{}
	for from, txns := range b.addedTxn {
		pendingTxn[from] = b.packagePool[from]
		pendingList = append(pendingList, txns...)
		for _, txn := range txns {
			gasLimit += txn.Gas()
			if b.globalCache.GetTxPreplay(txn.Hash()) == nil {
				b.globalCache.CommitTxPreplay(cache.NewTxPreplay(txn))
			}
		}
	}

	totalDifficulty := b.chain.GetTd(parentHash, parent.NumberU64())
	if totalDifficulty == nil {
		totalDifficulty = new(big.Int)
	}
	currentState := &cache.CurrentState{
		PreplayName:       b.trigger.Name,
		Number:            parent.NumberU64(),
		Hash:              parentHash.Hex(),
		RawHash:           parentHash,
		Txs:               parent.Transactions(),
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

	executor := NewExecutor("0", b.config, b.engine, b.chain, b.eth.ChainDb(), nil,
		pendingTxn, pendingList, currentState, b.trigger, nil, false)

	executor.RoundID = b.globalCache.NewRoundID()

	// Execute, use pending for preplay
	executor.commit(header.Coinbase, parent, header, pendingTxn)

	if b.removedTxn {
		b.updateDependency(executor.RoundID, pendingTxn)
	} else {
		b.updateDependency(executor.RoundID, b.addedTxn)
	}

	b.chain.Warmuper.AddWarmupTask(executor.RoundID, executor.executionOrder, parent.Root())
}

func (b *TaskBuilder) updateTxnGroup() {
	if b.removedTxn {
		b.nowGroups = make(map[common.Hash]*TxnGroup)
		b.groupTxns(b.packagePool)
	} else {
		b.groupTxns(b.addedTxn)
	}

	for groupHash, group := range b.nowGroups {
		b.pastGroupsMu.RLock()
		if _, ok := b.pastGroups[groupHash]; ok {
			b.pastGroupsMu.RUnlock()
			continue
		}
		b.pastGroupsMu.RUnlock()
		b.pastGroupsMu.Lock()
		b.pastGroups[groupHash] = group
		b.pastGroupsMu.Unlock()
		b.preplayLog.reportNewGroup(group)
		group.setValid()
		group.parent = b.parent
		group.header = b.nowHeader
		group.chainFactor = 1
		if group.isCoinbaseDep() {
			group.chainFactor *= len(b.minerList.top5Active)
		}
		if group.isTimestampDep() {
			group.chainFactor *= len(timeShift)
		}
		group.nextOrder = make(chan TxnOrder)

		for _, txns := range group.txns {
			sort.Sort(types.TxByNonce(txns))
		}

		group.divideTransactionPool()

		// start walk for new group
		go func(group *TxnGroup) {
			lastIndex := len(group.subpoolList) - 1
			walkTxnsPool(lastIndex, group.startList[lastIndex], group, make(TxnOrder, group.txnCount), b.minerList.top5Active)
			close(group.nextOrder)
			b.preplayLog.reportGroupEnd(group)
		}(group)

		b.taskQueue.pushTask(group)
	}
}

func (b *TaskBuilder) chainHeadUpdate(block *types.Block) {
	b.listener.setMinPrice(new(big.Int))
	b.gasLimit = core.CalcGasLimit(block, b.gasFloor, b.gasCeil)
	b.minerList.addMiner(block.Coinbase())
	b.whiteList = b.minerList.getWhiteList()
	if blockPre := b.globalCache.PeekBlockPre(block.Hash()); blockPre != nil {
		b.txnBaseline = blockPre.ListenTime
	} else {
		b.txnBaseline = block.Time()
	}
	b.txnDeadline = uint64(int64(b.txnBaseline) + deadlineStart)
	for _, txn := range block.Transactions() {
		delete(b.rwrecord, txn.Hash())
	}
	b.nowHeader = Header{
		coinbase: b.minerList.top5Active[0],
		time:     uint64(b.globalCache.GetTimeStamp()),
		gasLimit: b.gasLimit,
	}
	b.nowGroups = make(map[common.Hash]*TxnGroup)
	b.pastGroupsMu.RLock()
	for _, group := range b.pastGroups {
		group.setInvalid()
	}
	b.pastGroupsMu.RUnlock()
	b.pastGroupsMu.Lock()
	b.pastGroups = make(map[common.Hash]*TxnGroup)
	b.pastGroupsMu.Unlock()
	b.parent = block
}

var timeShift = []int{0, -1, 1}

func walkTxnsPool(subpoolLoc int, txnLoc int, group *TxnGroup, order TxnOrder, activeMiner []common.Address) {
	// boundaries of recursion
	if !group.isValid() {
		return
	}
	if group.isSubInOrder(subpoolLoc) {
		if subpoolLoc == 0 {
			var finalOrder = make(TxnOrder, len(order))
			copy(finalOrder, order)
			if group.isChainDep() {
				var (
					coinbaseTryCount  = 1
					timestampTryCount = 1
				)
				if group.isCoinbaseDep() {
					coinbaseTryCount = len(activeMiner)
				}
				if group.isTimestampDep() {
					timestampTryCount = len(timeShift)
				}
				for i := 0; i < coinbaseTryCount; i++ {
					for j := 0; j < timestampTryCount; j++ {
						if group.isValid() {
							group.header.coinbase = activeMiner[i]
							group.header.time = uint64(int(group.header.time) + timeShift[j])
							group.nextOrder <- finalOrder
						}
					}
				}
			} else {
				group.nextOrder <- finalOrder
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
		walkTxnsPool(subpoolLoc, txnLoc+1, group, order, activeMiner)
		nextIn[from]--
	}
}

func (b *TaskBuilder) nextTxnDeadline(nowValue uint64) (uint64, bool) {
	if nowValue+1 <= uint64(time.Now().Unix()) {
		return nowValue + 1, true
	} else {
		return nowValue, false
	}
}

func (b *TaskBuilder) updateDependency(roundID uint64, pool TransactionPool) {
	for _, txns := range pool {
		for _, txn := range txns {
			txnHash := txn.Hash()
			txPreplay := b.globalCache.GetTxPreplay(txnHash)
			if txPreplay != nil {
				if round, ok := txPreplay.PeekRound(roundID); ok {
					b.rwrecord[txnHash] = NewRWRecord(round.RWrecord)
					continue
				}
			}
			b.rwrecord[txnHash] = NewRWRecord(nil)
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
			rwrecord.merge(b.getRWRecord(txn.Hash()))
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

func (b TaskBuilder) isSameGroup(sender common.Address, txn common.Hash, group *TxnGroup) bool {
	if _, ok := group.txns[sender]; ok {
		return true
	}
	rwrecord := b.getRWRecord(txn)
	return rwrecord.isRWOverlap(group.RWRecord) || group.isRWOverlap(rwrecord)
}

func (b TaskBuilder) getRWRecord(txn common.Hash) *RWRecord {
	if rwrecord, ok := b.rwrecord[txn]; ok {
		return rwrecord
	} else {
		b.rwrecord[txn] = NewRWRecord(nil)
		return b.rwrecord[txn]
	}
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

func (b *TaskBuilder) close() {
	close(b.exitCh)
}
