package core

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/event"
	lru "github.com/hashicorp/golang-lru"
	"sync"
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
	contentForObject state.ObjectListMap
}

type ObjWarmupTask struct {
	objects state.ObjectList
	storage map[common.Hash]struct{}
}

type StatedbBox struct {
	sync.Mutex
	warmuper *Warmuper

	usableDb       int
	statedbList    [dbListSize]*state.StateDB
	processedForDb map[common.Address]map[common.Hash]struct{}
	dbWarmupCh     chan *ColdTask

	wobjectMap      state.ObjectListMap
	prepareForObj   map[common.Address]map[common.Hash]struct{}
	processedForObj map[common.Address]map[common.Hash]struct{}
	objWarmupChList [objSubgroupSize]chan *ObjWarmupTask

	valid           bool
	dbWarmupExitCh  chan struct{}
	objWarmupExitCh chan struct{}
	wg              sync.WaitGroup
}

func (b *StatedbBox) warmupDbLoop() {
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
		case <-b.dbWarmupExitCh:
			b.wg.Done()
			return
		}
	}
}

func (b *StatedbBox) warmupObjLoop(objWarmupCh <-chan *ObjWarmupTask) {
	for {
		select {
		case task := <-objWarmupCh:
			for _, wobject := range task.objects {
				db := wobject.GetDatabase()
				for key := range task.storage {
					wobject.GetCommittedState(db, key)
				}
			}
		case <-b.objWarmupExitCh:
			b.wg.Done()
			return
		}
	}
}

func (b *StatedbBox) exit() {
	if !b.valid {
		return
	}
	b.valid = false

	b.Lock()
	b.dbWarmupExitCh <- struct{}{}
	for i := byte(0); i < objSubgroupSize; i++ {
		b.objWarmupExitCh <- struct{}{}
	}
	b.Unlock()
}

type Warmuper struct {
	chain *BlockChain
	cfg   vm.Config

	coldQueue chan *ColdTask

	// Subscriptions
	chainHeadCh  chan ChainHeadEvent
	chainHeadSub event.Subscription
	exitCh       chan struct{}

	valid           bool
	statedbBoxCache *lru.Cache
	minerList       *lru.Cache
}

func NewWarmuper(chain *BlockChain, cfg vm.Config) *Warmuper {
	chainHeadCh := make(chan ChainHeadEvent, chainHeadChanSize)
	warmuper := &Warmuper{
		chain:        chain,
		cfg:          cfg,
		coldQueue:    make(chan *ColdTask, coldQueueSize),
		chainHeadCh:  chainHeadCh,
		chainHeadSub: chain.SubscribeChainHeadEvent(chainHeadCh),
		exitCh:       make(chan struct{}),
	}
	warmuper.statedbBoxCache, _ = lru.New(dbCacheSize)
	go warmuper.mainLoop()

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

func (w *Warmuper) exit() {
	w.exitCh <- struct{}{}
}

func (w *Warmuper) AddWarmupTask(RoundID uint64, executionOrder []*types.Transaction, root common.Hash) {
	if !w.valid {
		return
	}
	addrMap := make(map[common.Address]map[common.Hash]struct{})
	objectListMap := make(state.ObjectListMap)
	for _, tx := range executionOrder {
		txPreplay := w.chain.MSRACache.GetTxPreplay(tx.Hash())
		if txPreplay == nil {
			continue
		}
		round, _ := txPreplay.PeekRound(RoundID)
		if round == nil {
			continue
		}
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
		for address, object := range round.WObjects {
			if object == nil {
				continue
			}
			object.RoundId = round.RoundID
			objectListMap[address] = append(objectListMap[address], object)
		}
	}
	if len(addrMap) == 0 && len(objectListMap) == 0 {
		return
	}
	go func() {
		task := &ColdTask{
			root:             root,
			contentForDb:     addrMap,
			contentForObject: objectListMap,
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
	}
	return
}

func (w *Warmuper) warmupMiner(root common.Hash) {
	addrMap := make(map[common.Address]map[common.Hash]struct{})
	minerList := w.minerList.Keys()
	for _, rawMiner := range minerList {
		miner := rawMiner.(common.Address)
		addrMap[miner] = make(map[common.Hash]struct{})
	}
	task := &ColdTask{
		root:         root,
		contentForDb: addrMap,
	}
	w.coldQueue <- task
}

func (w *Warmuper) commitNewWork(task *ColdTask) {
	box := w.getOrNewStatedbBox(task.root)
	if box == nil || !box.valid {
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
				box.Lock()
				if box.valid {
					box.objWarmupChList[groupId] <- &ObjWarmupTask{
						objects: box.wobjectMap[address][:],
						storage: deltaStorage,
					}
				}
				box.Unlock()
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
	for address, objectList := range task.contentForObject {
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
			for _, wobject := range objectList {
				for key := range wobject.GetOriginStorage() {
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

		box.Lock()
		if box.valid {
			box.objWarmupChList[groupId] <- &ObjWarmupTask{
				objects: box.wobjectMap[address][:],
				storage: deltaStorage,
			}
			box.objWarmupChList[groupId] <- &ObjWarmupTask{
				objects: objectList,
				storage: baseStorageCpy,
			}
		}
		box.Unlock()

		box.wobjectMap[address] = append(box.wobjectMap[address], objectList...)
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
			warmuper:        w,
			statedbList:     [dbListSize]*state.StateDB{statedb, statedb.Copy()},
			processedForDb:  make(map[common.Address]map[common.Hash]struct{}),
			dbWarmupCh:      make(chan *ColdTask, warmupQueueSize),
			wobjectMap:      make(state.ObjectListMap),
			prepareForObj:   make(map[common.Address]map[common.Hash]struct{}),
			processedForObj: make(map[common.Address]map[common.Hash]struct{}),
			objWarmupChList: [objSubgroupSize]chan *ObjWarmupTask{},
			valid:           true,
			dbWarmupExitCh:  make(chan struct{}, 1),
			objWarmupExitCh: make(chan struct{}, objSubgroupSize),
		}

		go box.warmupDbLoop()
		for index := range box.objWarmupChList {
			box.objWarmupChList[index] = make(chan *ObjWarmupTask, warmupQueueSize)
			go box.warmupObjLoop(box.objWarmupChList[index])
		}
		box.wg.Add(int(objSubgroupSize) + 1)

		if w.cfg.MSRAVMSettings.CmpReuse {
			for _, statedb := range box.statedbList {
				statedb.ShareCopy()
				statedb.PreAllocateObjects()
				statedb.GetPair().PreAllocateObjects()
			}
		}
		w.statedbBoxCache.Add(root, box)
		return box
	}
	box, ok := rawdbs.(*StatedbBox)
	if !ok {
		return nil
	}
	return box
}
