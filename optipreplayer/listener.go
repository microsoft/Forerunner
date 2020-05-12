package optipreplayer

import (
	"fmt"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"math/big"
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

	go listener.blockLoop()
	go listener.commitLoop()

	return listener
}

func (l *Listener) blockLoop() {
	chainHeadCh := make(chan core.ChainHeadEvent, chainHeadChanSize)
	chainHeadSub := l.chain.SubscribeChainHeadEvent(chainHeadCh)
	defer chainHeadSub.Unsubscribe()

	var lastRemove, lastCheapRemove, lastSize int
	for {
		currentBlock := (<-chainHeadCh).Block
		l.blockMap[currentBlock.NumberU64()] = append(l.blockMap[currentBlock.NumberU64()], currentBlock)

		nowSize := l.globalCache.GetTxPreplayLen()
		log.Info("TxPreplay cache size", "number", currentBlock.NumberU64(),
			"last remove", fmt.Sprintf("%d(%d)", lastRemove, lastCheapRemove), "last size", lastSize, "add", nowSize-lastSize, "now", nowSize)

		l.removeBefore(currentBlock.NumberU64() - 6)
		if l.globalCache.GetTxPreplayLen() > l.globalCache.PreplayCacheSize/2 {
			l.removeBefore(currentBlock.NumberU64() - 2)
		}
		saveSize := l.globalCache.GetTxPreplayLen()
		l.globalCache.ResizeTxPreplay(l.globalCache.PreplayCacheSize)
		l.globalCache.ResizeTxPreplay(l.globalCache.PreplayCacheSize * 2)

		lastSize = l.globalCache.GetTxPreplayLen()
		lastCheapRemove = saveSize - lastSize
		lastRemove = nowSize - lastSize
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

func (l *Listener) commitLoop() {
	newTxsCh := make(chan core.NewTxsEvent, chanSize)
	newTxsSub := l.txPool.SubscribeNewTxsEvent(newTxsCh)
	defer newTxsSub.Unsubscribe()

	for {
		l.commitNewTxs((<-newTxsCh).Txs)
	}
}

func (l *Listener) commitNewTxs(txs types.Transactions) bool {
	if len(txs) == 0 {
		return false
	}

	nowTime := time.Now()
	for _, tx := range txs {
		l.globalCache.CommitTxListen(&cache.TxListen{
			Tx:              tx,
			ListenTime:      uint64(nowTime.Unix()),
			ListenTimeNano:  uint64(nowTime.UnixNano()),
			ConfirmTime:     0,
			ConfirmBlockNum: 0,
		})
	}
	return true
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

		for {
			txs := (<-newTxsCh).Txs
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
