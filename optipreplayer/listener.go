package optipreplayer

import (
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)

type Listener struct {
	// Flag new transaction arrive
	newTx      int32
	minPrice   *big.Int
	minPriceMu sync.RWMutex

	// NewTxs Subscription
	newTxsCh  chan core.NewTxsEvent
	newTxsSub event.Subscription

	// Cache
	globalCache *cache.GlobalCache
}

func NewListener(eth Backend) *Listener {
	listener := &Listener{
		minPrice: new(big.Int),
		newTxsCh: make(chan core.NewTxsEvent, chanSize),
	}
	listener.newTxsSub = eth.TxPool().SubscribeNewTxsEvent(listener.newTxsCh)

	go listener.txLoop()

	return listener
}

func (l *Listener) txLoop() {
	defer l.newTxsSub.Unsubscribe()

	for {
		select {
		case req := <-l.newTxsCh:
			l.reportNewTx(req.Txs)
			l.commitNewTxs(req.Txs)
		}
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
