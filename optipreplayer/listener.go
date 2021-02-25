package optipreplayer

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/optipreplayer/config"
	"math/big"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Listener struct {
	blockMap map[uint64][]*types.Block

	chain  *core.BlockChain
	txPool *core.TxPool

	// Cache
	globalCache *cache.GlobalCache
}

func NewListener(eth Backend) *Listener {
	listener := &Listener{
		blockMap: make(map[uint64][]*types.Block),
		chain:    eth.BlockChain(),
		txPool:   eth.TxPool(),
	}

	go listener.cacheEvictionLoop2()
	go listener.dropReduplicatedNonceTxnLoop()

	go listener.listenCommitLoop()
	go listener.enpoolCommitLoop()
	go listener.enpendingCommitLoop()

	return listener
}

// eviction is triggered by the heap info
func (l *Listener) cacheEvictionLoop2() {
	chainHeadCh := make(chan core.ChainHeadEvent, chainHeadChanSize)
	chainHeadSub := l.chain.SubscribeChainHeadEvent(chainHeadCh)
	defer chainHeadSub.Unsubscribe()

	m := new(runtime.MemStats)
	runtime.ReadMemStats(m)
	lastNumGC := m.NumGC
	for chainHeadEvent := range chainHeadCh {
		currentBlock := chainHeadEvent.Block
		l.blockMap[currentBlock.NumberU64()] = append(l.blockMap[currentBlock.NumberU64()], currentBlock)

		sizes := l.globalCache.GetTrieAndWObjectSizes()
		detail := &sizes.ExternalTransferDetails
		log.Info("ET trie size in cache",
			"totalTxCnt", sizes.TotalTxCount,
			"etTxCnt", sizes.ExternalTransferTxCount,
			"traceTxCnt", detail.TxWithTraceTrieCount,
			"mixTxCnt", detail.TxWithMixTrieCount,
			"rwTxCnt", detail.TxWithRWTrieCount,
			"deltaTxCnt", detail.TxWithDeltaTrieCount,
			"rwNodeCnt", detail.TotalRWTrieNodeCount,
			"deltaNodeCnt", detail.TotalDeltaTrieNodeCount,
			"mixNodeCnt", detail.TotalMixTrieNodeCount,
			"mixRoundCnt", detail.TotalMixTrieRoundCount,
			"mixRWCnt", detail.TotalMixTrieRWRecordCount,
			"traceNodeCnt", detail.TotalTraceTrieNodeCount,
			"traceRoundCnt", detail.TotalTraceTrieRoundCount,
			"tracePathCnt", detail.TotalTraceTriePathCount,
			"multiPathCnt", detail.TotalPathCountOfMultiPathTx,
			"multiPathTxCnt", detail.TxWithMultiPathCount,
			"pathDistr", detail.TraceTriePathCountDistributionStr,
			"wobjectCnt", detail.TotalWObjectCount,
			"wobjectItemCnt", detail.TotalWObjectStorageSize,
			)
		totalNodeCount := detail.TotalMixTrieNodeCount + detail.TotalTraceTrieNodeCount + detail.TotalRWTrieNodeCount + detail.TotalDeltaTrieNodeCount
		totalWObjectSize := detail.TotalWObjectCount*config.WOBJECT_BASE_SIZE+ detail.TotalWObjectStorageSize

        detail = &sizes.SmartContractDetails
		log.Info("SC trie size in cache",
			"totalTxCnt", sizes.TotalTxCount,
			"scTxCnt", sizes.TotalTxCount - sizes.ExternalTransferTxCount,
			"traceTxCnt", detail.TxWithTraceTrieCount,
			"mixTxCnt", detail.TxWithMixTrieCount,
			"rwTxCnt", detail.TxWithRWTrieCount,
			"deltaTxCnt", detail.TxWithDeltaTrieCount,
			"rwNodeCnt", detail.TotalRWTrieNodeCount,
			"deltaNodeCnt", detail.TotalDeltaTrieNodeCount,
			"mixNodeCnt", detail.TotalMixTrieNodeCount,
			"mixRoundCnt", detail.TotalMixTrieRoundCount,
			"mixRWCnt", detail.TotalMixTrieRWRecordCount,
			"traceNodeCnt", detail.TotalTraceTrieNodeCount,
			"traceRoundCnt", detail.TotalTraceTrieRoundCount,
			"tracePathCnt", detail.TotalTraceTriePathCount,
			"multiPathCnt", detail.TotalPathCountOfMultiPathTx,
			"multiPathTxCnt", detail.TxWithMultiPathCount,
			"pathDistr", detail.TraceTriePathCountDistributionStr,
			"wobjectCnt", detail.TotalWObjectCount,
			"wobjectItemCnt", detail.TotalWObjectStorageSize,
		)
        totalNodeCount += detail.TotalMixTrieNodeCount + detail.TotalTraceTrieNodeCount + detail.TotalRWTrieNodeCount + detail.TotalDeltaTrieNodeCount
		totalWObjectSize += detail.TotalWObjectCount*config.WOBJECT_BASE_SIZE+ detail.TotalWObjectStorageSize


		beforeSize := l.globalCache.LenOfTxPreplay()

		removed12 := 0
		removed6 := 0
		removed2 := 0
		removedHalf := 0

		m := new(runtime.MemStats)
		runtime.ReadMemStats(m)
		if m.NumGC > lastNumGC {
			// remove txs with 12 confirmations
			before12 := l.globalCache.LenOfTxPreplay()
			l.removeBefore(currentBlock.NumberU64() - 12)
			removed12 = before12 - l.globalCache.LenOfTxPreplay()

			if m.HeapAlloc > config.CACHE_START_EVICTION_SIZE_LIMIT {

				if m.HeapAlloc > config.CACHE_LIGHT_EVICTION_SIZE_LIMIT {
					// remove txs with 6 confirmations
					before6 := l.globalCache.LenOfTxPreplay()
					l.removeBefore(currentBlock.NumberU64() - 6)
					removed6 = before6 - l.globalCache.LenOfTxPreplay()
					//before6 := l.globalCache.LenOfTxPreplay()
					//l.removeBefore(currentBlock.NumberU64() - 6)
					//removed6 = before6 - l.globalCache.LenOfTxPreplay()
					// remove txs with 2 confirmations
				}
				if m.HeapAlloc > config.CACHE_SOFT_EVICTION_SIZE_LIMIT {
					// remove txs with 2 confirmations
					before2 := l.globalCache.LenOfTxPreplay()
					l.removeBefore(currentBlock.NumberU64() - 2)
					removed2 = before2 - l.globalCache.LenOfTxPreplay()
				}
				if m.HeapAlloc > config.CACHE_HARD_EVICTION_SIZE_LIMIT {
					currentCacheSize := l.globalCache.LenOfTxPreplay()
					targetCacheSize := currentCacheSize / 2
					l.globalCache.ResizeTxPreplay(targetCacheSize)
					removedHalf = currentCacheSize - l.globalCache.LenOfTxPreplay()
				}
			}
		}

		l.globalCache.GCWObjects()


		afterSize := l.globalCache.LenOfTxPreplay()
		removed := beforeSize - afterSize

		log.Info("TxPreplay cache eviction", "number", currentBlock.NumberU64(),
			"heapAlloc", m.HeapAlloc/1024/1024/1024,
			"txRemoved", fmt.Sprintf("%d(12:%d,6:%d,2:%d,half:%d)", removed, removed12, removed6, removed2, removedHalf),
			"txBefore", beforeSize, "txAfter", afterSize,
			"nodeCntBefore", totalNodeCount, "wobjectSizeBefore", totalWObjectSize,
			"numGC", m.NumGC, "lastNumGC", lastNumGC,
		)

		lastNumGC = m.NumGC
	}
}

func (l *Listener) dropReduplicatedNonceTxnLoop() {
	chainHeadCh := make(chan core.ChainHeadEvent, chainHeadChanSize)
	chainHeadSub := l.chain.SubscribeChainHeadEvent(chainHeadCh)
	defer chainHeadSub.Unsubscribe()

	signer := types.NewEIP155Signer(l.chain.Config().ChainID)

	for chainHeadEvent := range chainHeadCh {
		maxNonceMap := make(map[common.Address]uint64)
		for _, txn := range chainHeadEvent.Block.Transactions() {
			sender, _ := types.Sender(signer, txn)
			maxNonceMap[sender] = txn.Nonce()
		}
		l.globalCache.DropReduplicatedNonceTxn(func(addr common.Address, txns types.TxByNonce) types.TxByNonce {
			if maxNonce, ok := maxNonceMap[addr]; ok {
				index := sort.Search(len(txns), func(i int) bool {
					return txns[i].Nonce() > maxNonce
				})
				return txns[index:]
			} else {
				return txns
			}
		})
	}
}

func (l *Listener) removeBefore(remove uint64) {
	for n, blocks := range l.blockMap {
		if n <= remove {
			delete(l.blockMap, n)
			for _, block := range blocks {
				for _, txn := range block.Transactions() {
					l.globalCache.RemoveTxPreplay(txn.Hash())
				}
			}
		}
	}
}

func (l *Listener) listenCommitLoop() {
	listenTxsCh := make(chan core.ListenTxsEvent, chanSize)
	listenTxsSub := l.txPool.SubscribeListenTxsEvent(listenTxsCh)
	defer listenTxsSub.Unsubscribe()

	for txsEvent := range listenTxsCh {

		if len(txsEvent.Txs) == 0 {
			continue
		}

		nowTime := time.Now()

		for _, tx := range txsEvent.Txs {
			l.globalCache.CommitTxListen(&cache.TxListen{
				Tx:             tx,
				ListenTime:     uint64(nowTime.Unix()),
				ListenTimeNano: uint64(nowTime.UnixNano()),
			})
		}
	}
}

func (l *Listener) enpoolCommitLoop() {
	enpoolTxsCh := make(chan core.EnpoolTxsEvent, chanSize)
	enpoolTxsSub := l.txPool.SubscribeEnpoolTxsEvent(enpoolTxsCh)
	defer enpoolTxsSub.Unsubscribe()

	for txsEvent := range enpoolTxsCh {

		if len(txsEvent.Txs) == 0 {
			continue
		}

		nowTime := uint64(time.Now().UnixNano())

		for _, tx := range txsEvent.Txs {
			l.globalCache.CommitTxEnpool(tx.Hash(), nowTime)
		}
	}
}

func (l *Listener) enpendingCommitLoop() {
	enpendingTxsCh := make(chan core.EnpendingTxsEvent, chanSize)
	enpendingTxsSub := l.txPool.SubscribeEnpendingTxsEvent(enpendingTxsCh)
	defer enpendingTxsSub.Unsubscribe()

	for txsEvent := range enpendingTxsCh {

		if len(txsEvent.Txs) == 0 {
			continue
		}

		nowTime := uint64(time.Now().UnixNano())

		for _, tx := range txsEvent.Txs {
			l.globalCache.CommitTxEnpending(tx.Hash(), nowTime)
		}
	}
}

func (l *Listener) register() (func(*big.Int), func()) {
	var (
		newTx      int32
		minPrice   = new(big.Int)
		minPriceMu sync.RWMutex
	)

	go func() {
		newTxsCh := make(chan core.NewTxsEvent, chanSize)
		newTxsSub := l.txPool.SubscribeNewTxsEvent(newTxsCh)
		defer newTxsSub.Unsubscribe()

		for txsEvent := range newTxsCh {
			txs := txsEvent.Txs
			if len(txs) == 0 {
				continue
			}

			minPriceMu.RLock()
			for _, tx := range txs {
				if tx.CmpGasPrice(minPrice) >= 0 {
					atomic.StoreInt32(&newTx, 1)
					continue
				}
			}
			minPriceMu.RUnlock()
		}
	}()

	setMinPrice := func(price *big.Int) {
		if price == nil {
			return
		}

		minPriceMu.Lock()
		defer minPriceMu.Unlock()

		minPrice.Set(price)
	}

	waitForNewTx := func() {
		for atomic.LoadInt32(&newTx) == 0 {
			time.Sleep(10 * time.Millisecond)
		}
		atomic.StoreInt32(&newTx, 0)
	}

	return setMinPrice, waitForNewTx
}

func (l *Listener) setGlobalCache(globalCache *cache.GlobalCache) {
	l.globalCache = globalCache
}
