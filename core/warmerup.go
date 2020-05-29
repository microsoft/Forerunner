package core

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/hashicorp/golang-lru"
	"sync"
	"sync/atomic"
	"time"
)

const (
	dbListSize           = 2
	objSubgroupSize byte = 4
	coldQueueSize        = 5000
	dbCacheSize          = 10
	minerListSize        = 100
	warmupQueueSize      = 1000
)

type ColdTask struct {
	root             common.Hash
	contentForDb     map[common.Address]map[common.Hash]struct{}
	contentForObject cache.WObjectWeakRefListMap
}

type ObjWarmupTask struct {
	root          common.Hash
	objectHolders state.ObjectHolderList
	storage       map[common.Hash]struct{}
}

type StatedbBox struct {
	usableDb       int
	statedbList    [dbListSize]*state.StateDB
	processedForDb map[common.Address]map[common.Hash]struct{}
	dbWarmupCh     chan *ColdTask

	wobjectRefPool  cache.WObjectWeakRefPool
	prepareForObj   map[common.Address]map[common.Hash]struct{}
	processedForObj map[common.Address]map[common.Hash]struct{}

	valid   bool
	validMu sync.RWMutex

	exitCh chan struct{}
	wg     sync.WaitGroup
}

func (b *StatedbBox) warmupDbLoop() {
	b.wg.Add(1)
	for {
		select {
		case task := <-b.dbWarmupCh:
			usableDbList := b.statedbList[b.usableDb:]
			for _, statedb := range usableDbList {
				for addr, keyMap := range task.contentForDb {
					if exist := statedb.Exist(addr); exist {
						statedb.GetCode(addr)
						if _, ok := b.processedForDb[addr]; !ok {
							b.processedForDb[addr] = make(map[common.Hash]struct{})
						}
						for key := range keyMap {
							statedb.GetCommittedState(addr, key)
							b.processedForDb[addr][key] = struct{}{}
						}
					}
				}
				if pairdb := statedb.GetPair(); pairdb != nil {
					for addr, keyMap := range task.contentForDb {
						if exist := pairdb.Exist(addr); exist {
							pairdb.GetCode(addr)
							for key := range keyMap {
								pairdb.GetCommittedState(addr, key)
							}
						}
					}
				}
			}
		case <-b.exitCh:
			b.wg.Done()
			return
		}
	}
}

func (b *StatedbBox) exit() {
	b.validMu.Lock()
	if !b.valid {
		b.validMu.Unlock()
		return
	}
	b.valid = false
	b.validMu.Unlock()

	b.exitCh <- struct{}{}
}

type Warmuper struct {
	chain *BlockChain
	cfg   vm.Config

	coldQueue chan *ColdTask

	// Subscriptions
	chainHeadCh  chan ChainHeadEvent
	chainHeadSub event.Subscription
	exitCh       chan struct{}

	// Pause and continue
	pause      int32
	pauseCh    chan struct{}
	pauseWg    sync.WaitGroup
	continueWg sync.WaitGroup

	// Cache
	GlobalCache *cache.GlobalCache

	valid           bool
	root            common.Hash
	statedbBoxCache *lru.Cache
	minerList       *lru.Cache

	objWarmupChList [objSubgroupSize]chan *ObjWarmupTask
}

func NewWarmuper(chain *BlockChain, cfg vm.Config) *Warmuper {
	chainHeadCh := make(chan ChainHeadEvent, chainHeadChanSize)
	warmuper := &Warmuper{
		chain:           chain,
		cfg:             cfg,
		coldQueue:       make(chan *ColdTask, coldQueueSize),
		chainHeadCh:     chainHeadCh,
		chainHeadSub:    chain.SubscribeChainHeadEvent(chainHeadCh),
		exitCh:          make(chan struct{}, 1+objSubgroupSize),
		pauseCh:         make(chan struct{}, objSubgroupSize),
		objWarmupChList: [objSubgroupSize]chan *ObjWarmupTask{},
	}
	warmuper.statedbBoxCache, _ = lru.New(dbCacheSize)
	go warmuper.mainLoop()
	for index := range warmuper.objWarmupChList {
		warmuper.objWarmupChList[index] = make(chan *ObjWarmupTask, warmupQueueSize)
		go warmuper.warmupObjLoop(warmuper.objWarmupChList[index])
	}

	return warmuper
}

func (w *Warmuper) mainLoop() {
	for {
		select {
		case task := <-w.coldQueue:
			w.commitNewWork(task)
		case <-w.chainHeadCh:
			currentBlock := w.chain.CurrentBlock()

			w.valid = true
			w.root = currentBlock.Root()
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
			w.warmupMiner(currentBlock.Root())
		case <-w.exitCh:
			w.valid = false
			return
		}
	}
}

func (w *Warmuper) warmupObjLoop(objWarmupCh <-chan *ObjWarmupTask) {
	for {
		select {
		case task := <-objWarmupCh:
			if w.root != task.root {
				continue
			}
			for _, holder := range task.objectHolders {
				wobject := holder.Obj
				db := wobject.GetDatabase()
				for key := range task.storage {
					wobject.GetCommittedState(db, key)
				}
			}
		case <-w.pauseCh:
			w.pauseWg.Done()
			for atomic.LoadInt32(&w.pause) == 1 {
				time.Sleep(2 * time.Millisecond)
			}
			w.continueWg.Done()
		case <-w.exitCh:
			return
		}
	}
}

func (w *Warmuper) Pause() {
	w.pauseWg.Add(int(objSubgroupSize))
	atomic.StoreInt32(&w.pause, 1)
	for i := byte(0); i < objSubgroupSize; i++ {
		w.pauseCh <- struct{}{}
	}
	w.pauseWg.Wait()
}

func (w *Warmuper) Continue() {
	w.continueWg.Add(int(objSubgroupSize))
	atomic.StoreInt32(&w.pause, 0)
	w.continueWg.Wait()
}

func (w *Warmuper) exit() {
	for i := byte(0); i < 1+objSubgroupSize; i++ {
		w.exitCh <- struct{}{}
	}
}

func (w *Warmuper) AddWarmupTask(rounds []*cache.PreplayResult, root common.Hash) {
	if !w.valid {
		return
	}
	addrMap := make(map[common.Address]map[common.Hash]struct{})
	objectRefListMap := make(cache.WObjectWeakRefListMap)
	for _, round := range rounds {
		if round.RWrecord != nil {
			for addr, readStates := range round.RWrecord.RState {
				if _, ok := addrMap[addr]; !ok {
					addrMap[addr] = make(map[common.Hash]struct{})
				}
				for key := range readStates.Storage {
					addrMap[addr][key] = struct{}{}
				}
				for key := range readStates.CommittedStorage {
					addrMap[addr][key] = struct{}{}
				}
			}
		}
		for address, object := range round.WObjectWeakRefs {
			//if object == nil {
			//	continue
			//}
			objectRefListMap[address] = append(objectRefListMap[address], object)
		}
	}
	if len(addrMap) == 0 && len(objectRefListMap) == 0 {
		return
	}
	go func() {
		task := &ColdTask{
			root:             root,
			contentForDb:     addrMap,
			contentForObject: objectRefListMap,
		}
		w.coldQueue <- task
	}()
}

func (w *Warmuper) GetStateDB(root common.Hash) (retDb *state.StateDB) {
	if box := w.getStatedbBox(root); box != nil && box.usableDb < dbListSize {
		box.exit()
		box.wg.Wait()

		retDb = box.statedbList[box.usableDb]
		box.usableDb++

		retDb.AccountReads = 0
		retDb.StorageReads = 0

		if pair := retDb.GetPair(); pair != nil {
			pair.AccountReads = 0
			pair.StorageReads = 0
		}
	}
	return
}

func (w *Warmuper) GetProcessed(root common.Hash) (map[common.Address]map[common.Hash]struct{}, map[common.Address]map[common.Hash]struct{}) {
	if box := w.getStatedbBox(root); box != nil {
		return box.processedForDb, box.processedForObj
	}
	return nil, nil
}

func (w *Warmuper) warmupMiner(root common.Hash) {
	addrMap := make(map[common.Address]map[common.Hash]struct{})
	minerList := w.minerList.Keys()
	for _, rawMiner := range minerList {
		miner := rawMiner.(common.Address)
		addrMap[miner] = make(map[common.Hash]struct{})
	}
	go func() {
		task := &ColdTask{
			root:         root,
			contentForDb: addrMap,
		}
		w.coldQueue <- task
	}()
}

func (w *Warmuper) getObjectHolder(wref *cache.WObjectWeakReference) *state.ObjectHolder {
	txPreplay := w.GlobalCache.PeekTxPreplay(wref.TxHash)
	if txPreplay != nil && txPreplay.Timestamp == wref.Timestamp {
		if holder, hok := txPreplay.PreplayResults.GetHolder(wref); hok {
			return holder
		}
	}
	return nil
}

func (w *Warmuper) getObjectHolderList(wrefList cache.WObjectWeakRefList) state.ObjectHolderList {
	holderList := make(state.ObjectHolderList, 0, len(wrefList))
	for _, wref := range wrefList {
		holder := w.getObjectHolder(wref)
		if holder != nil {
			holderList = append(holderList, holder)
		}
	}
	return holderList
}

func (w *Warmuper) commitNewWork(task *ColdTask) {
	box := w.getOrNewStatedbBox(task.root)
	if box == nil {
		return
	}
	box.validMu.Lock()
	defer box.validMu.Unlock()

	if !box.valid {
		return
	}

	box.dbWarmupCh <- task

	for address, keyMap := range task.contentForDb {
		if _, ok := task.contentForObject[address]; !ok {
			if baseStorage, ok2 := box.processedForObj[address]; ok2 {
				deltaStorage := make(map[common.Hash]struct{})
				for key := range keyMap {
					if _, ok := baseStorage[key]; !ok {
						deltaStorage[key] = struct{}{}
						baseStorage[key] = struct{}{}
					}
				}

				groupId := address[0] % objSubgroupSize
				w.objWarmupChList[groupId] <- &ObjWarmupTask{
					root:          task.root,
					objectHolders: w.getObjectHolderList(box.wobjectRefPool.GetWObjectWeakRefList(address)),
					storage:       deltaStorage,
				}
			} else {
				if _, ok := box.prepareForObj[address]; !ok {
					box.prepareForObj[address] = make(map[common.Hash]struct{})
				}
				for key := range keyMap {
					box.prepareForObj[address][key] = struct{}{}
				}
			}
		}
	}
	for address, objectRefList := range task.contentForObject {
		newObjectRefList := make(cache.WObjectWeakRefList, 0, len(objectRefList))
		newObjectHolderList := make(state.ObjectHolderList, 0, len(objectRefList))
		for _, objRef := range objectRefList {
			if !box.wobjectRefPool.IsObjectRefInPool(address, objRef) {
				newObjectRefList = append(newObjectRefList, objRef)
				holder := w.getObjectHolder(objRef)
				if holder != nil {
					newObjectHolderList = append(newObjectHolderList, holder)
				}
			}
		}
		if _, ok := box.processedForObj[address]; !ok {
			if keyMap, ok := box.prepareForObj[address]; ok {
				box.processedForObj[address] = keyMap
			} else {
				box.processedForObj[address] = make(map[common.Hash]struct{})
			}
		}
		baseStorage := box.processedForObj[address]
		deltaStorage := make(map[common.Hash]struct{})
		if deltaKeyMap, ok := task.contentForDb[address]; ok {
			for key := range deltaKeyMap {
				if _, ok := baseStorage[key]; !ok {
					deltaStorage[key] = struct{}{}
					baseStorage[key] = struct{}{}
				}
			}
		} else {
			for _, holder := range newObjectHolderList {
				for key := range holder.Obj.GetOriginStorage() {
					if _, ok := baseStorage[key]; !ok {
						deltaStorage[key] = struct{}{}
						baseStorage[key] = struct{}{}
					}
				}
			}
		}

		groupId := address[0] % objSubgroupSize
		baseStorageCpy := make(map[common.Hash]struct{}, len(baseStorage))
		for key := range baseStorage {
			baseStorageCpy[key] = struct{}{}
		}

		w.objWarmupChList[groupId] <- &ObjWarmupTask{
			root:          task.root,
			objectHolders: w.getObjectHolderList(box.wobjectRefPool.GetWObjectWeakRefList(address)),
			storage:       deltaStorage,
		}
		w.objWarmupChList[groupId] <- &ObjWarmupTask{
			root:          task.root,
			objectHolders: newObjectHolderList,
			storage:       baseStorageCpy,
		}

		box.wobjectRefPool.AddWObjectWeakRefList(address, newObjectRefList)
	}
}

func (w *Warmuper) getStatedbBox(root common.Hash) *StatedbBox {
	rawdbs, ok := w.statedbBoxCache.Get(root)
	if !ok {
		return nil
	}
	box, ok := rawdbs.(*StatedbBox)
	if !ok {
		return nil
	}
	return box
}

func (w *Warmuper) getOrNewStatedbBox(root common.Hash) *StatedbBox {
	rawdbs, ok := w.statedbBoxCache.Get(root)
	if !ok {
		statedb, _ := w.chain.StateAt(root)
		box := &StatedbBox{
			statedbList:     [dbListSize]*state.StateDB{statedb, statedb.Copy()},
			processedForDb:  make(map[common.Address]map[common.Hash]struct{}),
			dbWarmupCh:      make(chan *ColdTask, warmupQueueSize),
			wobjectRefPool:  cache.NewWObjectWeakRefPool(),
			prepareForObj:   make(map[common.Address]map[common.Hash]struct{}),
			processedForObj: make(map[common.Address]map[common.Hash]struct{}),
			valid:           true,
			exitCh:          make(chan struct{}, 1),
		}

		go box.warmupDbLoop()

		if w.cfg.MSRAVMSettings.CmpReuse {
			for _, statedb := range box.statedbList {
				statedb.ShareCopy()
				statedb.PreAllocateObjects()
				statedb.GetPair().PreAllocateObjects()
			}
		}
		var value interface{}
		var ok bool
		if keys := w.statedbBoxCache.Keys(); len(keys) > 0 {
			value, ok = w.statedbBoxCache.Peek(keys[0])
		}
		if eviected := w.statedbBoxCache.Add(root, box); eviected && ok {
			oldestBox := value.(*StatedbBox)
			oldestBox.exit()
		}
		return box
	}
	box, ok := rawdbs.(*StatedbBox)
	if !ok {
		return nil
	}
	return box
}
