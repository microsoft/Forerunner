package optipreplayer

import (
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"time"
)

type Listener struct {
	// NewTxs Subscription
	newTxsCh  chan core.NewTxsEvent
	newTxsSub event.Subscription

	// Cache
	globalCache *cache.GlobalCache
}

func NewListener(eth Backend) *Listener {
	listener := &Listener{
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
			l.commitNewTxs(req.Txs)
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

func (l *Listener) setGlobalCache(globalCache *cache.GlobalCache) {
	l.globalCache = globalCache
}
