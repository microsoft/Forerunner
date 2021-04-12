package optipreplayer

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/params"
	"math"
	"math/big"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// preplayLimitForRemain is the upper limit of preplay count for transactions in remain pool.
	preplayLimitForRemain = 2
	// remainTxnLimit is the limit of remain txn preplay count.
	remainTxnLimit = 40
	// remainTxnTotalLimit is the total limit of remain txn preplay count between two blocks.
	remainTxnTotalLimit = 8000
)

type TaskBuilder struct {
	txPool *core.TxPool
	chain  *core.BlockChain

	startCh      chan struct{}
	finishOnceCh chan struct{}
	exitCh       chan struct{}

	// The lock used to protect the fields updated when new block enters
	mu *sync.RWMutex

	// Cache
	globalCache *cache.GlobalCache

	// Package
	packageType   PackageType
	originPool    TransactionPool
	packagePool   TransactionPool
	dupTxns       map[common.Hash]types.Transactions
	removedTxn    bool
	preplayPool   TransactionPool
	preplayRemain *int32
	remainCount   map[common.Hash]int
	minerList     *MinerList
	signer        types.Signer
	lastBaseline  uint64
	txnBaseline   uint64
	txnDeadline   uint64
	setMinPrice   func(price *big.Int)

	// Transaction distributor
	trigger    *Trigger
	nowHeader  *Header
	nowGroups  map[common.Hash]*TxnGroup
	parent     *types.Block
	rwrecord   map[common.Hash]*RWRecord
	rwrecordMu sync.RWMutex

	// Task queue
	groupTaskQueue   *TaskQueue
	preplayTaskQueue *TaskQueue

	// Log groups and transactions
	preplayLog *PreplayLog

	groupedReplacedTxns     map[common.Hash]struct{}
	groupedReplaceTxnsMutex sync.Mutex

	preplayer *Preplayer

	taskBuilderLogFile *os.File

	cfg *vm.MSRAVMConfig
}

func NewTaskBuilder(config *params.ChainConfig, eth Backend, mu *sync.RWMutex, packageType PackageType, cfg *vm.MSRAVMConfig) *TaskBuilder {
	builder := &TaskBuilder{
		txPool:              eth.TxPool(),
		chain:               eth.BlockChain(),
		startCh:             make(chan struct{}, 1),
		finishOnceCh:        make(chan struct{}),
		exitCh:              make(chan struct{}),
		mu:                  mu,
		packageType:         packageType,
		originPool:          make(TransactionPool),
		packagePool:         make(TransactionPool),
		dupTxns:             make(map[common.Hash]types.Transactions),
		preplayPool:         make(TransactionPool),
		preplayRemain:       new(int32),
		remainCount:         make(map[common.Hash]int),
		signer:              types.NewEIP155Signer(config.ChainID),
		lastBaseline:        uint64(time.Now().Unix()),
		setMinPrice:         func(price *big.Int) {},
		trigger:             NewTrigger("TxsBlock1P1", 1),
		nowGroups:           make(map[common.Hash]*TxnGroup),
		rwrecord:            make(map[common.Hash]*RWRecord),
		groupedReplacedTxns: make(map[common.Hash]struct{}),
		cfg:                 cfg,
	}

	if cfg.TaskBuilderChecking {
		builder.taskBuilderLogFile, _ = os.OpenFile("/tmp/TaskBuilderLog.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
	}

	return builder
}

func (b *TaskBuilder) setPreplayer(preplayer *Preplayer) {
	b.preplayer = preplayer

	b.minerList = preplayer.minerList
	b.nowHeader = preplayer.nowHeader
	b.groupTaskQueue = preplayer.groupTaskQueue
	b.preplayTaskQueue = preplayer.preplayTaskQueue
	b.preplayLog = preplayer.preplayLog
}

func (b *TaskBuilder) debugLog(fmtStr string, objects ...interface{}) {
	content := fmt.Sprintf(fmtStr, objects...) + "\n"
	b.taskBuilderLogFile.WriteString(content)
}

func (b *TaskBuilder) debugLogWithTimestamp(fmtStr string, objects ...interface{}) {
	now := time.Now()
	content := "[" + now.Format("01-02|15:04:05.000") + "] " + fmt.Sprintf(fmtStr, objects...) + "\n"
	b.taskBuilderLogFile.WriteString(content)
}

func (b *TaskBuilder) TaskBuilderLoop() {
	for {
		select {
		case <-b.startCh:
			startTime := time.Now()
			b.mu.RLock()
			checkDuration(&startTime, 1, "taskBuilder TaskBuilderLoop TOO SLOW", 100)
			currentBlock := b.chain.CurrentBlock()
			if b.parent != nil && currentBlock.Root() == b.parent.Root() {
				rawPending, _ := b.txPool.Pending()
				checkDuration(&startTime, 2, "taskBuilder TaskBuilderLoop TOO SLOW", 200)
				if b.cfg.TaskBuilderChecking {
					b.debugLogWithTimestamp("mainLoopIter blockNum %v blockHash %v deadline %v currentTime %v",
						currentBlock.NumberU64(), currentBlock.Hash().String(), b.txnDeadline, time.Now().Unix())
				}
				b.resetPackagePool(rawPending)
				checkDuration(&startTime, 3, "taskBuilder TaskBuilderLoop TOO SLOW", 200)
				b.resetPreplayPool()
				checkDuration(&startTime, 4, "taskBuilder TaskBuilderLoop TOO SLOW", 100)
				if len(b.preplayPool) > 0 {
					b.commitNewWork()
					checkDuration(&startTime, 5, "taskBuilder TaskBuilderLoop TOO SLOW", 500)
					b.updateTxnGroup()
					checkDuration(&startTime, 6, "taskBuilder TaskBuilderLoop TOO SLOW", 300)
				}
			}
			b.mu.RUnlock()
			checkDuration(&startTime, 7, "taskBuilder TaskBuilderLoop TOO SLOW", 100)
			b.finishOnceCh <- struct{}{}
			checkDuration(&startTime, 8, "taskBuilder TaskBuilderLoop TOO SLOW", 100)
		case <-b.exitCh:
			return
		}
	}
}

func (b *TaskBuilder) resetPackagePool(rawPending TransactionPool) {
	b.originPool, b.packagePool, b.dupTxns = b.packagePool, make(TransactionPool), make(map[common.Hash]types.Transactions)

	if b.cfg.TaskBuilderChecking {
		b.debugLogWithTimestamp(" resetPackagePool lastPackagePoolLen %v cunrretPoolLen %v whiteListLen %v",
			b.originPool.size(), rawPending.size(), len(b.minerList.whiteList))
	}

	if b.packageType == TYPE0 {
		var rawPendingCopy = make(TransactionPool)
		if len(b.remainCount) < remainTxnTotalLimit && atomic.LoadInt32(b.preplayRemain) == 0 {
			rawPendingCopy = rawPending.copy()
		}

		// inWhiteList the whitelist txns in advance
		for from := range b.minerList.whiteList {
			if txns, ok := rawPending[from]; ok {
				b.packagePool[from] = txns
				delete(rawPending, from)
			}
		}

		if b.cfg.TaskBuilderChecking {
			b.debugLogWithTimestamp("  after_add_miner_whitelist PackagePoolLen %v curretPoolLen %v",
				b.packagePool.size(), rawPending.size())
		}

		for _, from := range b.globalCache.GetWhiteList() {
			if txns, ok := rawPending[from]; ok {
				b.packagePool[from] = txns
				delete(rawPending, from)
			}
		}

		if b.cfg.TaskBuilderChecking {
			b.debugLogWithTimestamp("  after_add_global_whitelist PackagePoolLen %v curretPoolLen %v",
				b.packagePool.size(), rawPending.size())
		}

		var (
			txnWithMinPrice       *types.Transaction // Denote min price for avoiding call types.Transaction.GasPrice()
			pending               = types.NewTransactionsByPriceAndNonce(b.signer, rawPending, nil, true)
			poppedSenders         = make(map[common.Address]struct{})
			txnRemovedForGasLimit = make(map[*types.Transaction]struct{})
		)

		for range [1]int{} {
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

				if b.isTxnTooLate(txn) {
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
				} else {
					txnRemovedForGasLimit[txn] = struct{}{}
				}
			}
		}

		txPrice := "NoTxn"
		if txnWithMinPrice != nil {
			b.setMinPrice(txnWithMinPrice.GasPrice())
			txPrice = txnWithMinPrice.GasPrice().String()
		}

		if b.cfg.TaskBuilderChecking {
			b.debugLogWithTimestamp("  after_next_block_prediction PackagePoolLen %v curretPoolLen %v txnWithMinPrice %v",
				b.packagePool.size(), rawPending.size(), txPrice)
		}
		if len(b.remainCount) < remainTxnTotalLimit && atomic.CompareAndSwapInt32(b.preplayRemain, 0, 1) {
			if b.cfg.TaskBuilderChecking {
				b.debugLogWithTimestamp("  start_handle_remaining rawPendingCopy %v txnRemovedForGasLimit %v",
					rawPending.size(), len(txnRemovedForGasLimit))
			}
			go func() {
				b.handleRemainPreplayPool(pending, txnRemovedForGasLimit, rawPendingCopy)
				atomic.StoreInt32(b.preplayRemain, 0)
			}()
		}
	}

	b.packagePool.addPool(b.originPool)

	if b.cfg.TaskBuilderChecking {
		b.debugLogWithTimestamp("  after_add_originPool PackagePoolLen %v",
			b.packagePool.size())
	}

	b.removedTxn = b.originPool.isTxnsPoolLarge(b.packagePool)
	b.preplayLog.reportNewPackage(!b.removedTxn)
	b.preplayLog.reportNewDeadline(b.txnDeadline)
	b.txnDeadline = b.nextTxnDeadline(b.txnDeadline)

	nowTime := uint64(time.Now().UnixNano())
	for sender, txns := range b.packagePool {
		for _, txn := range txns {
			b.globalCache.CommitTxPackage(txn.Hash(), nowTime)
		}
		minNonce := txns[0].Nonce()
		if reduplicatedNonceTxn, ok := b.globalCache.SearchReduplicatedNonceTxn(sender, minNonce, txns[len(txns)-1].Nonce()); ok {
			for _, dupTxn := range reduplicatedNonceTxn {
				replacedTxn := txns[dupTxn.Nonce()-minNonce]
				if !b.isAlreadyGroupedReplacedTx(dupTxn.Hash()) {
					if replacedTxn.Hash() != dupTxn.Hash() {
						b.dupTxns[replacedTxn.Hash()] = append(b.dupTxns[replacedTxn.Hash()], dupTxn)
						b.globalCache.CommitTxPackage(dupTxn.Hash(), nowTime)
					}
				}
			}
		}
	}
}

func (b *TaskBuilder) resetPreplayPool() {
	b.preplayPool = make(TransactionPool)

	if b.packageType == TYPE0 {
		b.preplayPool = b.packagePool.filter(func(sender common.Address, txn *types.Transaction) bool {
			return !b.originPool.isTxnIn(sender, txn)
		})
	}

	if b.cfg.TaskBuilderChecking {
		b.debugLogWithTimestamp(" resetPreplayPool preplayPoolLen %v preplayPoolSenderCount %v", b.preplayPool.size(), len(b.preplayPool))
	}
}

func (b *TaskBuilder) commitNewWork() {
	var wg sync.WaitGroup

	orderAndHeader := OrderAndHeader{order: TxnOrder{}}
	orderAndHeader.header.coinbase = b.nowHeader.coinbase
	orderAndHeader.header.time = b.nowHeader.time
	orderAndHeader.header.gasLimit = b.preplayPool.gas()

	if b.cfg.TaskBuilderChecking {
		b.debugLogWithTimestamp(" commitNewWork")
	}

	for from, txns := range b.preplayPool {
		var txnList [][]byte
		for _, txn := range txns {
			txnList = append(txnList, txn.Hash().Bytes())
		}
		sort.Slice(txnList, func(i, j int) bool {
			return bytes.Compare(txnList[i], txnList[j]) == -1
		})

		group := &TxnGroup{
			hash:    crypto.Keccak256Hash(txnList...),
			txnPool: TransactionPool{from: txns},
			txnList: txnList,
		}

		group.defaultInit(b.parent, false)
		wg.Add(1)
		go func(group *TxnGroup, txns types.Transactions) {
			b.groupTaskQueue.pushTask(group)

			nowTime := uint64(time.Now().UnixNano())
			for _, txn := range txns {
				b.globalCache.CommitTxEnqueue(txn.Hash(), nowTime)
			}

			group.nextOrderAndHeader <- orderAndHeader
			close(group.nextOrderAndHeader)

			b.updateDependency(<-group.roundIDCh, group.txnPool)
			wg.Done()
		}(group, txns)
	}

	if b.cfg.TaskBuilderChecking {
		b.debugLogWithTimestamp("  group Dispatched")
	}

	wg.Wait()

	if b.cfg.TaskBuilderChecking {
		b.debugLogWithTimestamp("  wait Done")
	}
}

func isSamePriceTooMuch(pool TransactionPool) bool {
	var priceTotalMap = make(map[uint64]int)
	for _, txns := range pool {
		var priceMap = make(map[uint64]struct{})
		for _, txn := range txns {
			priceMap[txn.GasPriceU64()] = struct{}{}
		}
		for price := range priceMap {
			priceTotalMap[price]++
		}
	}
	for _, count := range priceTotalMap {
		if count >= 2 {
			return true
		}
	}
	return false
}

func (b *TaskBuilder) updateTxnGroup() {

	totalGroupCount, totalTxnCount := b.preplayLog.getTotalGroupAndTxCount()
	if b.cfg.TaskBuilderChecking {
		b.debugLogWithTimestamp(" updateTxnGroup removedTxn %v nowGroupCount %v totalGroupCount %v totalTxCount %v", b.removedTxn, len(b.nowGroups), totalGroupCount, totalTxnCount)
	}
	if b.removedTxn {
		b.nowGroups = make(map[common.Hash]*TxnGroup)
		b.groupTxns(b.packagePool)
	} else {
		b.groupTxns(b.preplayPool)
	}

	if b.cfg.TaskBuilderChecking {
		b.debugLogWithTimestamp("   after groupTxns nowGroupCount %v", len(b.nowGroups))
	}

	var accountDepOnGroup = make(map[common.Address][]*TxnGroup)
	var accountDepOnTxn = make(map[common.Address]TransactionPool)
	for _, group := range b.nowGroups {
		for addr := range group.RWRecord.writeStates {
			accountDepOnGroup[addr] = append(accountDepOnGroup[addr], group)
		}
	}
	for from, txns := range b.packagePool {
		for _, txn := range txns {
			for addr := range b.selectRWRecord(txn.Hash()).writeStates {
				if accountDepOnTxn[addr] == nil {
					accountDepOnTxn[addr] = make(TransactionPool)
				}
				accountDepOnTxn[addr][from] = append(accountDepOnTxn[addr][from], txn)
			}
		}
	}

	var addrNotCopyMap = make(map[*TxnGroup]map[common.Address]struct{})
	for addr, relativeTxn := range accountDepOnTxn {
		relativeGroup := accountDepOnGroup[addr]
		if len(relativeGroup) >= 2 || isSamePriceTooMuch(relativeTxn) {
			for _, group := range relativeGroup {
				if _, ok := addrNotCopyMap[group]; !ok {
					addrNotCopyMap[group] = make(map[common.Address]struct{})
				}
				addrNotCopyMap[group][addr] = struct{}{}
			}
		}
	}

	var replacingTxnsList []types.Transactions
	var dupTxnCount int
	for i := 0; ; i++ {
		var replacingTxns types.Transactions
		for _, txns := range b.dupTxns {
			if i == 0 {
				dupTxnCount += len(txns)
			}
			if i < len(txns) {
				replacingTxns = append(replacingTxns, txns[i])
			}
		}
		if len(replacingTxns) == 0 {
			break
		} else {
			replacingTxnsList = append(replacingTxnsList, replacingTxns)
		}
	}

	if b.cfg.TaskBuilderChecking {
		b.debugLogWithTimestamp("   after dupLoop dupTxnLen %v dupTxnCount %v replacingTxnLen %v", len(b.dupTxns), dupTxnCount, len(replacingTxnsList))
	}
	var newGroupCount, newExtraGroupCount int

	for groupHash, group := range b.nowGroups {
		if originGroup, exist := b.preplayLog.reportNewGroup(group); exist {
			b.nowGroups[groupHash] = originGroup
			continue
		}
		newGroupCount++

		for _, txns := range group.txnPool {
			sort.Sort(types.TxByNonce(txns))
		}

		group.setValid()

		group.parent = b.parent
		group.txnCount = group.txnPool.size()
		group.chainFactor = 1
		if group.isCoinbaseDep() {
			group.chainFactor *= topActiveCount
		}
		if group.isTimestampDep() {
			group.chainFactor *= len(timeShift)
		}
		group.orderCount, group.startList, group.subpoolList = divideTransactionPool(group.txnPool, group.txnCount, group.chainFactor)
		group.fullPreplay = true
		if addrNotCopy, ok := addrNotCopyMap[group]; ok {
			group.addrNotCopy = addrNotCopy
		} else {
			group.addrNotCopy = make(map[common.Address]struct{})
		}

		group.preplayFinish = make(map[common.Hash]int, group.txnCount)
		group.preplayFail = make(map[common.Hash][]string, group.txnCount)

		group.nextInList = make([]map[common.Address]int, len(group.subpoolList))
		for index, subPool := range group.subpoolList {
			nextIn := make(map[common.Address]int, len(subPool))
			for from := range subPool {
				nextIn[from] = 0
			}
			group.nextInList[index] = nextIn
		}
		group.nextOrderAndHeader = make(chan OrderAndHeader)

		var extraGroups []*TxnGroup
		for _, replacingTxns := range replacingTxnsList {
			if newGroup, replaceMapping := group.replaceTxn(replacingTxns, b.signer); newGroup != nil {
				if _, exist := b.preplayLog.reportNewGroup(newGroup); !exist {
					extraGroups = append(extraGroups, newGroup)
					newGroupCount++
					newExtraGroupCount++
					b.recordGroupedReplacedTx(replaceMapping)
				}
			}
		}

		// start walk for new group
		walk := func(group *TxnGroup) {
			//lastIndex := len(group.subpoolList) - 1
			//walkTxnsPool(lastIndex, group.startList[lastIndex], group, make(TxnOrder, group.txnCount), b)
			walkTxnsPool_2(group, b)
			close(group.nextOrderAndHeader)
			b.preplayLog.reportGroupEnd(group)
		}

		go walk(group)
		for _, extraGroup := range extraGroups {
			go walk(extraGroup)
		}

		b.preplayTaskQueue.pushTask(group)

		nowTime := uint64(time.Now().UnixNano())
		for _, txns := range group.txnPool {
			for _, txn := range txns {
				b.globalCache.CommitTxEnqueue(txn.Hash(), nowTime)
			}
		}

		for _, extraGroup := range extraGroups {
			nowTime := uint64(time.Now().UnixNano())
			b.preplayTaskQueue.pushTask(extraGroup)
			for _, txns := range extraGroup.txnPool {
				for _, txn := range txns {
					b.globalCache.CommitTxEnqueue(txn.Hash(), nowTime)
				}
			}
		}

		//for _, txns := range b.dupTxns {
		//	for _, txn := range txns {
		//		b.globalCache.CommitTxEnqueue(txn.Hash(), nowTime)
		//	}
		//}
	}

	totalGroupCount, totalTxnCount = b.preplayLog.getTotalGroupAndTxCount()
	if b.cfg.TaskBuilderChecking {
		b.debugLogWithTimestamp("   after newGroupLoop newGroupCount %v newExtraGroupCount %v totalGroupCount %v totalTxCount %v", newGroupCount, newExtraGroupCount,
			totalGroupCount, totalTxnCount)
	}
	b.preplayLog.reportNewTaskBuild()
}

func (b *TaskBuilder) chainHeadUpdate(block *types.Block) {
	b.originPool = make(TransactionPool)
	b.packagePool = make(TransactionPool)
	b.dupTxns = make(map[common.Hash]types.Transactions)
	b.preplayPool = make(TransactionPool)
	b.remainCount = make(map[common.Hash]int)
	b.lastBaseline = b.txnBaseline
	if blockPre := b.globalCache.PeekBlockPre(block.Hash()); blockPre != nil {
		b.txnBaseline = blockPre.ListenTimeNano / 1E9
	} else {
		b.txnBaseline = block.Time()
	}
	b.txnDeadline = b.txnBaseline
	b.setMinPrice(common.Big0)
	b.nowGroups = make(map[common.Hash]*TxnGroup)
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

func divideTransactionPool(txns TransactionPool, txnCount, chainFactor int) (orderCount *big.Int, startList []int, subpoolList []TransactionPool) {
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

		inPoolList += subPool.size()
	}
	return
}

// timeShift denotes 0, -1, 1, -2, 2
var timeShift = []uint64{0, math.MaxUint64, 1, math.MaxUint64 - 1, 2}

// TODO: based on statistic: replace 2, -2

const MaxRetryCount = 10

func walkTxnsPool_2(group *TxnGroup, b *TaskBuilder) {

	if !group.isValid() {
		return
	}

	var walkheaders []Header
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
		walkheaders = make([]Header, coinbaseTryCount*timestampTryCount)
		index := 0
		for indexGap := 0; indexGap < timestampTryCount; indexGap++ {
			for i := 0; i < coinbaseTryCount; i++ {
				timeIndex := (i + indexGap) % timestampTryCount
				header := Header{coinbase: b.minerList.topActive[i], time: b.nowHeader.time + timeShift[timeIndex],
					gasLimit: b.nowHeader.gasLimit}
				walkheaders[index] = header
				index++
			}
		}
		rand.Shuffle(len(walkheaders)-1, func(i, j int) {
			tmp := walkheaders[i+1]
			walkheaders[i+1] = walkheaders[j+1]
			walkheaders[j+1] = tmp
		})
	} else {
		walkheaders = []Header{*b.nowHeader}
	}

	maxOrderCount := new(big.Int).Div(group.orderCount, big.NewInt(int64(group.chainFactor)))
	headerAllWorked := make([]bool, len(walkheaders))
	allWalkedHeaderCount := 0
	headerPriorities := make([]map[common.Hash]struct{}, len(walkheaders))
	headerOrders := make([]map[common.Hash]struct{}, len(walkheaders))

	for i := range walkheaders {
		headerOrders[i] = make(map[common.Hash]struct{})
		headerPriorities[i] = make(map[common.Hash]struct{})
	}
	//walkedPriorities := make(map[common.Hash]bool, 10)
	//walkedOrder := make(map[common.Hash]struct{}, 10)

	if len(group.txnPool) > 1 {
		retryCount := 0
		headerIndex := 0
		startTime := time.Now()

		for ; allWalkedHeaderCount < len(walkheaders); headerIndex++ {

			if !group.isValid() {
				break
			}

			headerIndex = headerIndex % len(walkheaders)

			if headerAllWorked[headerIndex] {
				continue
			}

			var shuffleIndex []int
			var isRepeated bool

			shuffleIndex = rand.Perm(group.txnCount)

			sliceHash := getIntSliceHash(shuffleIndex)
			curHeaderPriorities := headerPriorities[headerIndex]

			_, ok := curHeaderPriorities[sliceHash]
			if ok {
				// retry
				if retryCount < MaxRetryCount {
					retryCount++
					continue
				}
				isRepeated = true
			} else {
				curHeaderPriorities[sliceHash] = struct{}{}
			}
			//}

			newPriorities := make(map[common.Hash]int, group.txnCount)
			for index := 0; index < group.txnCount; index++ {
				newPriorities[common.BytesToHash(group.txnList[index])] = shuffleIndex[index]
			}

			newOrder := make(TxnOrder, group.txnCount)

			rawTxs := group.txnPool.copy()
			txs := types.NewTransactionsByPriceAndNonce(b.signer, rawTxs, newPriorities, false)
			for i := 0; i < group.txnCount; i++ {
				newOrder[i] = txs.Peek().Hash()
				txs.Shift()
			}
			curHeaderOrder := headerOrders[headerIndex]

			newOrderHash := newOrder.Hash()
			if _, ok := curHeaderOrder[newOrderHash]; ok {
				// repeated order
				// retry
				if retryCount < MaxRetryCount {
					retryCount++
					continue
				}
				isRepeated = true
			} else {
				curHeaderOrder[newOrderHash] = struct{}{}
			}

			if maxOrderCount.IsInt64() && int64(len(curHeaderOrder)) >= maxOrderCount.Int64() {
				headerAllWorked[headerIndex] = true
				allWalkedHeaderCount++
			}

			dur := time.Since(startTime)
			if dur > 100*time.Millisecond {
				log.Warn("TOO SLOW walk loop", "duration", dur.String(), "groupSize", group.txnCount,
					"subpoolSize", len(group.subpoolList), "curOrderCount", len(curHeaderOrder), "maxOrderCount", maxOrderCount,
					"retryCount", retryCount, "isRepeated", isRepeated)
			}

			orderAndHeader := OrderAndHeader{header: walkheaders[headerIndex], order: newOrder, isRepeated: isRepeated}
			group.nextOrderAndHeader <- orderAndHeader
			retryCount = 0
			startTime = time.Now()

		}
	} else {
		for hi := 0; hi < len(walkheaders); hi++ {
			if !group.isValid() {
				break
			}
			orderAndHeader := OrderAndHeader{header: walkheaders[hi], order: make(TxnOrder, 0)}
			group.nextOrderAndHeader <- orderAndHeader
		}
	}

}

func getIntSliceHash(nums []int) common.Hash {
	data := make([]byte, 4*len(nums))
	for i, num := range nums {
		data[i*4] = byte(num)
		data[i*4+1] = byte(num >> 8)
		data[i*4+2] = byte(num >> 16)
		data[i*4+3] = byte(num >> 24)
	}
	return sha256.Sum256(data)
}

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
				orderAndHeader.header = *b.nowHeader
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
		maxPriceTxn     = make(map[common.Address]*types.Transaction)
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
			maxPriceTxn = map[common.Address]*types.Transaction{from: headTxn}
		case cmp == 0:
			maxPriceFrom = append(maxPriceFrom, from)
			maxPriceTxn[from] = headTxn
		}
	}
	sort.Slice(maxPriceFrom, func(i, j int) bool {
		return bytes.Compare(maxPriceFrom[i].Bytes(), maxPriceFrom[j].Bytes()) < 0
	})
	for _, from := range maxPriceFrom {
		order[txnLoc] = maxPriceTxn[from].Hash()
		nextIn[from]++
		walkTxnsPool(subpoolLoc, txnLoc+1, group, order, b)
		nextIn[from]--
	}
}

func (b *TaskBuilder) isTxnTooLate(txn *types.Transaction) bool {
	txnListen := b.globalCache.GetTxListen(txn.Hash())
	return txnListen == nil || txnListen.ListenTime > b.txnDeadline
}

func (b *TaskBuilder) handleRemainPreplayPool(remainPending *types.TransactionsByPriceAndNonce, txnRemovedForGasLimit map[*types.Transaction]struct{}, rawPendingCopy TransactionPool) {
	var pickTxn = make(map[common.Hash]*types.Transaction)
	for txn := range txnRemovedForGasLimit {
		if b.globalCache.PeekTxPreplayInNonProcess(txn.Hash()) == nil && b.remainCount[txn.Hash()] < preplayLimitForRemain {
			pickTxn[txn.Hash()] = txn
		}
	}
	needSize := remainTxnLimit - len(pickTxn)
	for needSize > 0 {
		txn := remainPending.Peek()
		if txn == nil {
			break
		}
		if b.globalCache.PeekTxPreplayInNonProcess(txn.Hash()) == nil && b.remainCount[txn.Hash()] < preplayLimitForRemain {
			pickTxn[txn.Hash()] = txn
			needSize--
		}
		remainPending.Shift()
	}

	rawPreplayPool := rawPendingCopy.filter(func(sender common.Address, tx *types.Transaction) bool {
		_, ok := pickTxn[tx.Hash()]
		return ok
	})

	var preplayPool = make(TransactionPool)
	for from, txns := range rawPreplayPool {
		for _, txn := range txns {
			preplayPool.addTxn(from, txn)
		}
	}

	if len(preplayPool) == 0 {
		return
	}

	nowTime := uint64(time.Now().UnixNano())
	for _, txns := range preplayPool {
		for _, txn := range txns {
			b.globalCache.CommitTxPackage(txn.Hash(), nowTime)
		}
	}

	var dupTxns = make(map[common.Hash]types.Transactions)
	for _, txn := range pickTxn {
		sender, _ := types.Sender(b.signer, txn)
		if reduplicatedNonceTxn, ok := b.globalCache.SearchReduplicatedNonceTxn(sender, txn.Nonce(), txn.Nonce()); ok {
			for _, dupTxn := range reduplicatedNonceTxn {
				if !b.isAlreadyGroupedReplacedTx(dupTxn.Hash()) {
					if txn.Hash() != dupTxn.Hash() {
						dupTxns[txn.Hash()] = append(dupTxns[txn.Hash()], dupTxn)
						b.globalCache.CommitTxPackage(dupTxn.Hash(), nowTime)
					}
				}
			}
		}
	}

	var txnList [][]byte
	for _, txns := range preplayPool {
		for _, txn := range txns {
			txnList = append(txnList, txn.Hash().Bytes())
		}
	}
	sort.Slice(txnList, func(i, j int) bool {
		return bytes.Compare(txnList[i], txnList[j]) == -1
	})

	group := &TxnGroup{
		hash:    crypto.Keccak256Hash(txnList...),
		txnPool: preplayPool,
		txnList: txnList,
	}

	//if b.preplayLog.isGroupExist(group) {
	//	return
	//}
	if _, existed := b.preplayLog.reportRemainNewGroup(group); existed {
		return
	}

	var replacingTxnsList []types.Transactions
	for i := 0; ; i++ {
		var replacingTxns types.Transactions
		for _, txns := range dupTxns {
			if i < len(txns) {
				replacingTxns = append(replacingTxns, txns[i])
			}
		}
		if len(replacingTxns) == 0 {
			break
		} else {
			replacingTxnsList = append(replacingTxnsList, replacingTxns)
		}
	}

	group.RWRecord = NewRWRecord(nil)
	group.defaultInit(b.parent, true)
	group.isPriorityConst = true
	group.closeRoundIDChOnInvalid = true

	for _, txns := range group.txnPool {
		for _, txn := range txns {
			b.remainCount[txn.Hash()]++
		}
	}

	var extraGroups []*TxnGroup
	for _, replacingTxns := range replacingTxnsList {
		if newGroup, replaceMapping := group.replaceTxn(replacingTxns, b.signer); newGroup != nil {
			//if !b.preplayLog.isGroupExist(newGroup) {
			if _, existed := b.preplayLog.reportRemainNewGroup(newGroup); !existed {
				newGroup.roundIDCh = make(chan uint64, 1)
				newGroup.closeRoundIDChOnInvalid = true
				extraGroups = append(extraGroups, newGroup)
				b.recordGroupedReplacedTx(replaceMapping)
				if len(extraGroups) == 2 {
					break
				}
			}
		}
	}

	orderAndHeader := OrderAndHeader{
		order:  TxnOrder{},
		header: *b.nowHeader,
	}

	// start walk for new group
	walk := func(group *TxnGroup) {
		group.nextOrderAndHeader <- orderAndHeader
		close(group.nextOrderAndHeader)
	}

	go walk(group)
	for _, extraGroup := range extraGroups {
		go walk(extraGroup)
	}

	b.preplayTaskQueue.pushTask(group)
	nowTime = uint64(time.Now().UnixNano())
	for _, txns := range group.txnPool {
		for _, txn := range txns {
			b.globalCache.CommitTxEnqueue(txn.Hash(), nowTime)
		}
	}

	<-group.roundIDCh

	for _, extraGroup := range extraGroups {
		nowTime = uint64(time.Now().UnixNano())
		b.preplayTaskQueue.pushTask(extraGroup)
		for _, txns := range extraGroup.txnPool {
			for _, txn := range txns {
				b.globalCache.CommitTxEnqueue(txn.Hash(), nowTime)
			}
		}
		<-extraGroup.roundIDCh
	}

	//for _, txns := range dupTxns {
	//	for _, txn := range txns {
	//		b.globalCache.CommitTxEnqueue(txn.Hash(), nowTime)
	//	}
	//}
	//
	//
	//for _, extraGroup := range extraGroups {
	//	<-extraGroup.roundIDCh
	//}
}

func (b *TaskBuilder) recordGroupedReplacedTx(replaceMapping map[*types.Transaction]*types.Transaction) {
	b.groupedReplaceTxnsMutex.Lock()
	defer b.groupedReplaceTxnsMutex.Unlock()
	for _, txn := range replaceMapping {
		b.groupedReplacedTxns[txn.Hash()] = struct{}{}
	}
}

func (b *TaskBuilder) isAlreadyGroupedReplacedTx(txHash common.Hash) (exist bool) {
	b.groupedReplaceTxnsMutex.Lock()
	defer b.groupedReplaceTxnsMutex.Unlock()
	_, exist = b.groupedReplacedTxns[txHash]
	return
}

func (b *TaskBuilder) nextTxnDeadline(nowValue uint64) uint64 {
	if nowValue+1 <= uint64(time.Now().Unix()) {
		return nowValue + 1
	} else {
		return nowValue
	}
}

func (b *TaskBuilder) updateDependency(roundID uint64, pool TransactionPool) {
	for _, txns := range pool {
		for _, txn := range txns {
			txnHash := txn.Hash()
			if txPreplay := b.globalCache.PeekTxPreplayInNonProcess(txnHash); txPreplay != nil {
				txPreplay.RLockRound()
				if round, ok := txPreplay.PeekRound(roundID); ok {
					b.insertRWRecord(txnHash, NewRWRecord(round.RWrecord))
				}
				txPreplay.RUnlockRound()
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

		txnPool := make(TransactionPool)
		txnList := make([][]byte, 0)
		rwrecord := NewRWRecord(nil)
		for groupHash := range relativeGroup {
			addGroup := b.nowGroups[groupHash]
			delete(b.nowGroups, groupHash)
			for addr, txns := range addGroup.txnPool {
				txnPool[addr] = append(txnPool[addr], txns...)
			}
			txnList = append(txnList, addGroup.txnList...)
			rwrecord.merge(addGroup.RWRecord)
		}
		txnPool[from] = append(txnPool[from], txns...)
		for _, txn := range txns {
			txnList = append(txnList, txn.Hash().Bytes())
			rwrecord.merge(b.selectRWRecord(txn.Hash()))
		}
		sort.Slice(txnList, func(i, j int) bool {
			return bytes.Compare(txnList[i], txnList[j]) == -1
		})
		groupHash := crypto.Keccak256Hash(txnList...)
		b.nowGroups[groupHash] = &TxnGroup{
			hash:     groupHash,
			txnPool:  txnPool,
			txnList:  txnList,
			RWRecord: rwrecord,
		}
	}
}

func (b *TaskBuilder) isSameGroup(sender common.Address, txn common.Hash, group *TxnGroup) bool {
	if _, ok := group.txnPool[sender]; ok {
		return true
	}
	rwrecord := b.selectRWRecord(txn)
	return rwrecord.isRWOverlap(group.RWRecord) || group.isRWOverlap(rwrecord)
}

func (b *TaskBuilder) setGlobalCache(globalCache *cache.GlobalCache) {
	b.mu.Lock()
	defer b.mu.Unlock()
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
