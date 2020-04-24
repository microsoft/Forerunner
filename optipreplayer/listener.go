package optipreplayer

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"math/big"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Listener struct {
	// Flag new transaction arrive
	newTx      int32
	minPrice   *big.Int
	minPriceMu sync.RWMutex

	lastTxPreplaySize int

	chain  *core.BlockChain
	txPool *core.TxPool

	// Cache
	globalCache *cache.GlobalCache
}

func NewListener(eth Backend) *Listener {
	listener := &Listener{
		minPrice: new(big.Int),
		chain:    eth.BlockChain(),
		txPool:   eth.TxPool(),
	}

	go listener.blockLoop()

	go listener.reportLoop()
	go listener.commitLoop()

	return listener
}

func (l *Listener) blockLoop() {
	chainHeadCh := make(chan core.ChainHeadEvent, chainHeadChanSize)
	chainHeadSub := l.chain.SubscribeChainHeadEvent(chainHeadCh)
	defer chainHeadSub.Unsubscribe()

	for {
		currentBlock := (<-chainHeadCh).Block

		confirmBlock := l.chain.GetBlockByNumber(currentBlock.NumberU64() - 6)
		for _, txn := range confirmBlock.Transactions() {
			l.globalCache.RemoveTxPreplay(txn.Hash())
		}

		m := new(runtime.MemStats)
		runtime.ReadMemStats(m)
		if m.GCCPUFraction > 0.015 {
			saveSize := 2000
			nowTxPreplaySize := l.globalCache.GetTxPreplayLen()
			if newTxPreplaySize := nowTxPreplaySize - l.lastTxPreplaySize; newTxPreplaySize > saveSize {
				saveSize = newTxPreplaySize
			}
			if removeSize := (nowTxPreplaySize - saveSize) / 4; removeSize > 0 {
				for _, rawKey := range l.globalCache.GetTxPreplayKeys()[:removeSize] {
					l.globalCache.RemoveTxPreplay(rawKey.(common.Hash))
				}
			}
		}

		l.lastTxPreplaySize = l.globalCache.GetTxPreplayLen()
	}
}

func (l *Listener) reportLoop() {
	newTxsCh := make(chan core.NewTxsEvent, chanSize)
	newTxsSub := l.txPool.SubscribeNewTxsEvent(newTxsCh)
	defer newTxsSub.Unsubscribe()

	for {
		l.reportNewTx((<-newTxsCh).Txs)
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

func (l *Listener) reportNewTx(txs types.Transactions) {
	if len(txs) == 0 {
		return
	}

	l.minPriceMu.RLock()
	defer l.minPriceMu.RUnlock()

	for _, tx := range txs {
		if tx.GasPrice().Cmp(l.minPrice) >= 0 {
			atomic.StoreInt32(&l.newTx, 1)
			return
		}
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

func (l *Listener) setMinPrice(price *big.Int) {
	if price == nil {
		return
	}

	l.minPriceMu.Lock()
	defer l.minPriceMu.Unlock()

	l.minPrice = price
}

func (l *Listener) waitForNewTx() {
	for atomic.LoadInt32(&l.newTx) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	atomic.StoreInt32(&l.newTx, 0)
}

func (l *Listener) setGlobalCache(globalCache *cache.GlobalCache) {
	l.globalCache = globalCache
}
