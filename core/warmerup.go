package core

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/event"
	lru "github.com/hashicorp/golang-lru"
	"sync"
)

const (
	coldQueueSize = 100
	dbCacheSize   = 10
	dbListSize    = 2
	minerListSize = 100
)

type statedbList [dbListSize]*state.StateDB

type Warmuper struct {
	chain *BlockChain
	cfg   vm.Config

	coldQueue chan map[common.Address]map[common.Hash]struct{}
	requestCh  chan common.Hash
	responseCh chan *state.StateDB

	// Subscriptions
	chainHeadCh  chan ChainHeadEvent
	chainHeadSub event.Subscription
	exitCh       chan struct{}

	valid        bool
	root         common.Hash
	statedbCache *lru.Cache
	Processed    map[common.Address]map[common.Hash]struct{}
	minerList    *lru.Cache

	ProcessedMu sync.Mutex
}

func NewWarmuper(chain *BlockChain, cfg vm.Config) *Warmuper {
	chainHeadCh := make(chan ChainHeadEvent, chainHeadChanSize)
	warmuper := &Warmuper{
		chain: chain,
		cfg:   cfg,
		coldQueue: make(chan map[common.Address]map[common.Hash]struct{}, coldQueueSize),
		requestCh:    make(chan common.Hash),
		responseCh:   make(chan *state.StateDB),
		chainHeadCh:  chainHeadCh,
		chainHeadSub: chain.SubscribeChainHeadEvent(chainHeadCh),
		exitCh:       make(chan struct{}),
	}
	warmuper.statedbCache, _ = lru.New(dbCacheSize)
	go warmuper.mainLoop()

	return warmuper
}

func (w *Warmuper) mainLoop() {
	for {
		select {
		case addrList := <-w.coldQueue:
			w.chain.MSRACache.PauseForProcess()
			w.commitNewWork(addrList)
		case root := <-w.requestCh:
			statedbs := w.getStatedbList(root)
			if statedbs == nil {
				w.responseCh <- nil
				continue
			}
			switch {
			case (*statedbs)[0] != nil:
				w.responseCh <- (*statedbs)[0]
				(*statedbs)[0] = nil
			case (*statedbs)[1] != nil:
				w.responseCh <- (*statedbs)[1]
				(*statedbs)[1] = nil
			default:
				w.responseCh <- nil
			}
		case <-w.chainHeadCh:
			currentBlock := w.chain.CurrentBlock()

			w.valid = true
			w.root = currentBlock.Root()
			statedb, _ := w.chain.StateAt(w.root)
			statedbs := statedbList{statedb, statedb.Copy()}
			if w.cfg.MSRAVMSettings.CmpReuse {
				for _, statedb := range statedbs {
					statedb.ShareCopy()
				}
			}
			w.commitStatedbList(w.root, &statedbs)
			w.ProcessedMu.Lock()
			w.Processed = make(map[common.Address]map[common.Hash]struct{})
			w.ProcessedMu.Unlock()

			if w.minerList == nil {
				w.minerList, _ = lru.New(minerListSize)
				nextBlk := w.chain.CurrentBlock().NumberU64() + 1
				start := nextBlk - minerListSize
				for index := start; index < nextBlk; index++ {
					coinbase := w.chain.GetBlockByNumber(index).Coinbase()
					w.minerList.Add(coinbase, struct{}{})
				}
			} else {
				w.minerList.Add(currentBlock.Coinbase(), struct{}{})
			}
			w.warmupMiner()
		case <-w.exitCh:
			return
		}
	}
}

func (w *Warmuper) exit() {
	w.exitCh <- struct{}{}
}

func (w *Warmuper) IsRunning() bool {
	return w.root != common.Hash{}
}

func (w *Warmuper) AddWarmupTask(addrMap map[common.Address]map[common.Hash]struct{}) {
	if !w.valid || w.statedbCache.Len() == 0 || len(addrMap) == 0 {
		return
	}
	go func() {
		w.coldQueue <- addrMap
	}()
}

func (w *Warmuper) GetStateDB(root common.Hash) *state.StateDB {
	w.requestCh <- root
	return <-w.responseCh
}

func (w *Warmuper) IsObjectWarm(addr common.Address) bool {
	w.ProcessedMu.Lock()
	defer w.ProcessedMu.Unlock()

	_, ok := w.Processed[addr]
	return ok
}

func (w *Warmuper) IsKeyWarm(addr common.Address, key common.Hash) bool {
	addrOk := w.IsObjectWarm(addr)
	if !addrOk {
		return addrOk
	}

	w.ProcessedMu.Lock()
	defer w.ProcessedMu.Unlock()

	_, ok := w.Processed[addr][key]
	return ok
}

func (w *Warmuper) warmupMiner() {
	addrMap := make(map[common.Address]map[common.Hash]struct{})
	minerList := w.minerList.Keys()
	for _, rawMiner := range minerList {
		miner := rawMiner.(common.Address)
		addrMap[miner] = make(map[common.Hash]struct{})
	}
	w.AddWarmupTask(addrMap)
}

func (w *Warmuper) commitNewWork(addrMap map[common.Address]map[common.Hash]struct{}) {
	statedbs := w.getStatedbList(w.root)
	if statedbs == nil {
		return
	}

	newAddrMap := make(map[common.Address]map[common.Hash]struct{})
	w.ProcessedMu.Lock()
	for addr, keyMap := range addrMap {
		if _, ok := w.Processed[addr]; !ok {
			newAddrMap[addr] = make(map[common.Hash]struct{})
			w.Processed[addr] = make(map[common.Hash]struct{})
		}
		for key := range keyMap {
			if _, ok := w.Processed[addr][key]; !ok {
				if _, ok := newAddrMap[addr]; !ok {
					newAddrMap[addr] = make(map[common.Hash]struct{})
				}
				newAddrMap[addr][key] = struct{}{}
				w.Processed[addr][key] = struct{}{}
			}
		}
	}
	w.ProcessedMu.Unlock()

	if len(newAddrMap) == 0 {
		return
	}

	for _, statedb := range *statedbs {
		if statedb == nil {
			continue
		}
		for addr, keyMap := range addrMap {
			statedb.GetBalance(addr)
			statedb.GetCode(addr)
			for key := range keyMap {
				statedb.GetCommittedState(addr, key)
			}
		}
		if pairdb := statedb.GetPair(); pairdb != nil {
			for addr, keyMap := range addrMap {
				pairdb.GetBalance(addr)
				pairdb.GetCode(addr)
				for key := range keyMap {
					pairdb.GetCommittedState(addr, key)
				}
			}
		}
	}
}

func (w *Warmuper) getStatedbList(root common.Hash) *statedbList {
	rawdbs, ok := w.statedbCache.Get(root)
	if !ok {
		return nil
	}
	statedbs, ok := rawdbs.(*statedbList)
	if !ok {
		return nil
	}
	return statedbs
}

func (w *Warmuper) commitStatedbList(root common.Hash, statedbs *statedbList) {
	if statedbs == nil {
		return
	}
	w.statedbCache.Add(root, statedbs)
}
