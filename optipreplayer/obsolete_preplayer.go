// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

//// Copyright 2015 The go-ethereum Authors
//// This file is part of the go-ethereum library.
////
//// The go-ethereum library is free software: you can redistribute it and/or modify
//// it under the terms of the GNU Lesser General Public License as published by
//// the Free Software Foundation, either version 3 of the License, or
//// (at your option) any later version.
////
//// The go-ethereum library is distributed in the hope that it will be useful,
//// but WITHOUT ANY WARRANTY; without even the implied warranty of
//// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//// GNU Lesser General Public License for more details.
////
//// You should have received a copy of the GNU Lesser General Public License
//// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.
//
package optipreplayer
//
//import (
//	"fmt"
//	"math/big"
//	"os"
//	"strconv"
//	"sync"
//	"sync/atomic"
//	"time"
//
//	"github.com/ethereum/go-ethereum/core/types"
//	"github.com/ethereum/go-ethereum/optipreplayer/cache"
//	"github.com/ethereum/go-ethereum/optipreplayer/writer"
//
//	"github.com/ethereum/go-ethereum/common"
//	"github.com/ethereum/go-ethereum/consensus"
//	"github.com/ethereum/go-ethereum/core"
//	"github.com/ethereum/go-ethereum/event"
//	"github.com/ethereum/go-ethereum/log"
//	"github.com/ethereum/go-ethereum/params"
//)
//
//const (
//	// chainHeadChanSize is the size of channel listening to ChainHeadEvent.
//	chainHeadChanSize = 10
//
//	// miningLogAtDepth is the number of confirmations before logging successful mining.
//	miningLogAtDepth = 7
//
//	// minRecommitInterval is the minimal time interval to recreate the mining block with
//	// any newly arrived transactions.
//	minRecommitInterval = 1 * time.Second
//
//	// staleThreshold is the maximum depth of the acceptable stale block.
//	staleThreshold = 7
//)
//
//// task contains all information for consensus engine sealing and result submitting.
//// type task struct {
//// 	receipts  []*types.Receipt
//// 	state     *state.StateDB
//// 	block     *types.Block
//// 	createdAt time.Time
//// }
//
//// newWorkReq represents a request for new sealing work submitting with relative interrupt notifier.
//type newWorkReq struct {
//	interrupt *int32
//	timestamp int64
//}
//
//type Trigger struct {
//	Name        string
//	ExecutorNum int
//
//	IsTxsInDetector bool
//
//	IsChainHeadDrive bool
//
//	IsTxsNumDrive        bool
//	PreplayedTxsNumLimit uint64
//
//	IsTxsRatioDrive        bool
//	PreplayedTxsRatioLimit float64
//
//	IsBlockCntDrive           bool
//	PreplayedBlockNumberLimit uint64
//
//	IsPriceRankDrive        bool
//	PreplayedPriceRankLimit uint64
//}
//
//// Preplayer is the main object which takes care of submitting new work to consensus engine
//// and gathering the sealing result.
//type Preplayer struct {
//	nodeID string
//	config *params.ChainConfig
//	engine consensus.Engine
//	eth    Backend
//	chain  *core.BlockChain
//
//	gasFloor uint64
//	gasCeil  uint64
//
//	// Subscriptions
//	chainHeadCh  chan core.ChainHeadEvent
//	chainHeadSub event.Subscription
//
//	// Channels
//	newWorkCh chan *newWorkReq
//	startCh   chan struct{}
//	exitCh    chan struct{}
//
//	mu       sync.RWMutex // The lock used to protect the coinbase and extra fields
//	coinbase common.Address
//	extra    []byte
//
//	running  int32 // The indicator whether the consensus engine is running or not.
//	filePath string
//	// aggregator *Aggregator
//	trigger *Trigger
//
//	// Result
//	resultMu sync.RWMutex
//	txResult map[common.Hash]*cache.ExtraResult
//
//	// Cache
//	globalCache *cache.GlobalCache
//}
//
//func NewPreplayer(config *params.ChainConfig, engine consensus.Engine, eth Backend, gasFloor, gasCeil uint64, trigger *Trigger) *Preplayer {
//	preplayer := &Preplayer{
//		config:      config,
//		engine:      engine,
//		eth:         eth,
//		chain:       eth.BlockChain(),
//		gasFloor:    gasFloor,
//		gasCeil:     gasCeil,
//		chainHeadCh: make(chan core.ChainHeadEvent, chainHeadChanSize),
//		newWorkCh:   make(chan *newWorkReq),
//		startCh:     make(chan struct{}, 2),
//		exitCh:      make(chan struct{}),
//		trigger:     trigger,
//	}
//
//	preplayer.filePath = fmt.Sprintf("/datadrive/preplayers")
//	os.MkdirAll(preplayer.filePath, os.ModePerm)
//	if preplayer.trigger.IsTxsInDetector {
//		preplayer.chainHeadSub = eth.BlockChain().SubscribeChainHeadEvent(preplayer.chainHeadCh)
//	}
//	go preplayer.GroupSchedulerLoop()
//	go preplayer.newWorkLoop()
//
//	// Submit preplaying work to initialize pending state.
//	preplayer.startCh <- struct{}{}
//
//	return preplayer
//}
//
//// setExtra sets the content used to initialize the block extra field.
//func (p *Preplayer) setExtra(extra []byte) {
//	p.mu.Lock()
//	defer p.mu.Unlock()
//	p.extra = extra
//}
//
//// setEtherbase sets the etherbase used to initialize the block coinbase field.
//func (p *Preplayer) setEtherbase(addr common.Address) {
//	p.mu.Lock()
//	defer p.mu.Unlock()
//	p.coinbase = addr
//}
//
//func (p *Preplayer) setNodeID(nodeID string) {
//	p.mu.Lock()
//	defer p.mu.Unlock()
//	p.nodeID = nodeID
//}
//
//func (p *Preplayer) setGlobalCache(globalCache *cache.GlobalCache) {
//	p.mu.Lock()
//	defer p.mu.Unlock()
//	p.globalCache = globalCache
//}
//
//func (p *Preplayer) GetResult(txHash common.Hash) *cache.ExtraResult {
//	p.resultMu.RLock()
//	defer p.resultMu.RUnlock()
//
//	if _, ok := p.txResult[txHash]; ok {
//		return p.txResult[txHash]
//	}
//
//	return nil
//}
//
//// start sets the running Status as 1 and triggers new work submitting.
//func (p *Preplayer) start() {
//	atomic.StoreInt32(&p.running, 1)
//	p.startCh <- struct{}{}
//}
//
//// stop sets the running Status as 0.
//func (p *Preplayer) stop() {
//	atomic.StoreInt32(&p.running, 0)
//}
//
//// isRunning returns an indicator whether preplayers is running or not.
//func (p *Preplayer) isRunning() bool {
//	return atomic.LoadInt32(&p.running) == 1
//}
//
//// close terminates all background threads maintained by the preplayers.
//// Note the preplayers does not support being closed multiple times.
//func (p *Preplayer) close() {
//	close(p.exitCh)
//}
//
//// newWorkLoop is a standalone goroutine to submit new mining work upon received events.
//func (p *Preplayer) newWorkLoop() {
//	var (
//		// interrupt *int32
//		timestamp int64 // timestamp for each executorNum of mining.
//	)
//
//	execute := func() {
//		timestamp = time.Now().Unix()
//		p.newWorkCh <- &newWorkReq{timestamp: timestamp}
//
//	}
//
//	for {
//		select {
//		// Didn't catch things out
//		case <-p.startCh:
//			execute()
//			if !p.trigger.IsChainHeadDrive {
//				p.startCh <- struct{}{}
//			}
//
//		case <-p.chainHeadCh:
//			// log.Info("new Block", fmt.Sprintf("%s-%s", ev.Block.Number(), ev.Block.Header().Time),
//			// 	len([]int{}))
//			if p.trigger.IsChainHeadDrive {
//				log.Debug("TxsInDetector hear!")
//				execute()
//				log.Debug("TxsInDetector inserted!")
//			}
//
//		case <-p.exitCh:
//			// atomic.StoreInt32(interrupt, commitInterruptExit)
//			return
//		}
//	}
//
//	// For test only
//	//execute()
//}
//
//// GroupSchedulerLoop is a standalone goroutine to regenerate the sealing task based on the received event.
//func (p *Preplayer) GroupSchedulerLoop() {
//	if p.trigger.IsTxsInDetector {
//		defer p.chainHeadSub.Unsubscribe()
//	}
//	for {
//		select {
//		case req := <-p.newWorkCh:
//			p.commitNewWork(req.timestamp)
//		case <-p.exitCh: //0xa76b13c02b47aebef5ca26fd5e4f33bf1c1ce19d1ab0b7186afb48ccf4599161
//			return
//		}
//	}
//}
//
//// commit generates several new sealing tasks based on the parent block.
//func (p *Preplayer) commitNewWork(timestamp int64) {
//
//	//defer func() {
//	//	if p.trigger.IsTxsInDetector {
//	//		log.Debug("Preplay gc")
//	//		runtime.GC()
//	//	}
//	//}()
//
//	// time.Sleep(100 * time.Millisecond)
//	// log.Info("preplay enter",
//	// 	"currentState", fmt.Sprintf("%s", p.trigger.Name))
//
//	var (
//		coinbase common.Address
//		extra    []byte
//	)
//
//	if p.nodeID == "" || p.globalCache == nil {
//		return
//	}
//
//	// stop ?
//	// if !p.isRunning() {
//	// 	return
//	// }
//
//	p.mu.RLock()
//	defer p.mu.RUnlock()
//
//	if p.isRunning() {
//		if p.coinbase == (common.Address{}) {
//			log.Info("Preplay coinbase null")
//			return
//		}
//		coinbase = p.coinbase
//	}
//	extra = p.extra
//
//	// log.Info("preplay enter get block",
//	// 	"currentState", fmt.Sprintf("%s", p.trigger.Name))
//	parent := p.chain.CurrentBlock()
//
//	// log.Info("preplay enter get td",
//	// 	"currentState", fmt.Sprintf("%s", p.trigger.Name))
//	totalDifficulty := p.chain.GetTd(parent.Hash(), parent.Number().Uint64())
//	if totalDifficulty == nil {
//		totalDifficulty = &big.Int{}
//	}
//
//	// Fill the block with all available local transactions.
//	// We use local for testing only. Txs commit can be others later
//
//	currentState := &cache.CurrentState{
//		PreplayID:         p.nodeID,
//		PreplayName:       p.trigger.Name,
//		Number:            parent.Number().Uint64(),
//		Hash:              parent.Hash().String(),
//		RawHash:           parent.Hash(),
//		Txs:               parent.Transactions(),
//		TotalDifficulty:   totalDifficulty.String(),
//		SnapshotTimestamp: time.Now().UnixNano() / 1000000,
//	}
//
//	var (
//		rawPending map[common.Address]types.Transactions
//		// rawLocal   map[common.Address]types.Transactions
//		err error
//	)
//
//	// log.Info("preplay in (get pool)",
//	// 	"currentState", fmt.Sprintf("%s-%s-%d-%d", p.trigger.Name, currentState.PreplayID, currentState.Number, currentState.SnapshotTimestamp),
//	// 	"txs num", len(rawPending), " ", len(rawLocal))
//
//	// log.Info("preplay enter get pending",
//	// 	"currentState", fmt.Sprintf("%s", p.trigger.Name))
//	if !p.trigger.IsTxsInDetector {
//		// rawPending, rawLocal, err = p.eth.TxPool().Pending()
//		// if err != nil {
//		// 	return
//		// }
//
//		rawPending, err = p.eth.TxPool().Pending()
//		if err != nil {
//			return
//		}
//
//	} else {
//		// in detector
//		return
//	}
//
//	// When to return.. need discuss
//	if len(rawPending) == 0 {
//		return
//	}
//
//	p.globalCache.PauseForProcess()
//
//	pendingList := types.Transactions{}
//	for _, trx := range rawPending {
//		for _, tx := range trx {
//			pendingList = append(pendingList, tx)
//		}
//	}
//
//	// localList := types.Transactions{}
//	// for _, trx := range rawLocal {
//	// 	for _, tx := range trx {
//	// 		localList = append(localList, tx)
//	// 	}
//	// }
//
//	// if parent.Time().Cmp(new(big.Int).SetInt64(timestamp)) >= 0 {
//	// 	timestamp = parent.Time().Int64() + 1
//	// }
//	// // this will ensure we're not going off too far in the future
//	// if now := time.Now().Unix(); timestamp > now+1 {
//	// 	wait := time.Duration(timestamp-now) * time.Second
//	// 	log.Info("Mining too far in the future", "wait", common.PrettyDuration(wait))
//	// 	time.Sleep(wait)
//	// }
//
//	timestamp = p.globalCache.NewTimeStamp()
//	// log.Info("preplay enter ready to start",
//	// "currentState", fmt.Sprintf("%s", p.trigger.Name))
//	num := parent.Number()
//	header := &types.Header{
//		ParentHash: parent.Hash(),
//		Number:     num.Add(num, common.Big1),
//		GasLimit:   core.CalcGasLimit(parent, p.gasFloor, p.gasCeil),
//		Coinbase:   coinbase,
//		Extra:      extra,
//		Time:       uint64(timestamp),
//	}
//
//	//funny := 1
//	log.Debug("Preplay start",
//		"currentState", fmt.Sprintf("%s-%s-%d-%d", p.trigger.Name, currentState.PreplayID, currentState.Number, currentState.SnapshotTimestamp),
//		"pending", fmt.Sprintf("%d(%d)", len(rawPending), len(pendingList)))
//
//	// p.aggregator = NewAggregator(p.trigger.Name, p.trigger.ExecutorNum, pendingList, currentState, p.filePath)
//
//	chain := &core.BlockChain{}
//	Copy(chain, p.chain)
//
//	for i := 0; i < p.trigger.ExecutorNum; i++ {
//		// go func(i int) {
//		exeHeader := &types.Header{}
//		exeParent := &types.Block{}
//		exeChain := &core.BlockChain{}
//		Copy(exeHeader, header)
//		Copy(exeParent, parent)
//		Copy(exeChain, chain)
//
//		// log.Info("preplay new executor",
//		// 	"currentState", fmt.Sprintf("%s", p.trigger.Name))
//
//		executor := NewExecutor(strconv.Itoa(i), p.config, p.engine, p.chain, p.eth.ChainDb(),
//			nil, rawPending, pendingList, currentState, p.trigger, nil, false)
//
//		txCommit := rawPending
//		executor.RoundID = p.globalCache.NewRoundID()
//
//		// Pre setting
//		for _, txs := range txCommit {
//			for _, tx := range txs {
//				if p.globalCache.GetTxPreplay(tx.Hash()) == nil {
//					p.globalCache.AddTxPreplay(cache.NewTxPreplay(tx))
//				}
//			}
//		}
//
//		currentState.StartTimeString = time.Now().Format("2006-01-02 15:04:05")
//
//		// Execute, use pending for preplay
//		executor.commit(coinbase, extra, exeParent, exeHeader, txCommit)
//
//		executor.submitResult(p, txCommit)
//
//		// Update Cache, need initialize
//		p.globalCache.CommitTxResult(executor.RoundID, currentState, p.txResult)
//
//		currentState.EndTimeString = time.Now().Format("2006-01-02 15:04:05")
//
//		settings := p.chain.GetVMConfig().MSRAVMSettings
//		if settings.PreplayRecord && !settings.Silent {
//			p.globalCache.PreplayPrint(executor.RoundID, executor.executionOrder, currentState)
//		}
//		// p.aggregator.setResultChFinish(i)
//		// }(i)
//	}
//
//	// p.aggregator.mergeLoop()
//
//	log.Debug("Preplay end",
//		"currentState", fmt.Sprintf("%s-%s-%d-%d", p.trigger.Name, currentState.PreplayID, currentState.Number, currentState.SnapshotTimestamp),
//		"pending", fmt.Sprintf("%d(%d)", len(rawPending), len(pendingList)))
//
//}
//
//type Preplayers []*Preplayer
//
//func NewPreplayers(eth Backend, config *params.ChainConfig, engine consensus.Engine, gasFloor, gasCeil uint64) Preplayers {
//	var triggers []*Trigger
//	filePath := writer.GetParentDirectory(writer.CurrentFile())
//	writer.Load(filePath+"/config/config.json", &triggers)
//	log.Info("Resolve preplayers", "filepath", filePath+"/config/config.json")
//	preplayers := Preplayers{}
//	for _, trigger := range triggers {
//		preplayers = append(preplayers, NewPreplayer(config, engine, eth, gasFloor, gasCeil, trigger))
//	}
//	return preplayers
//}
//
//// setExtra sets the content used to initialize the block extra field.
//func (preplayers Preplayers) setExtra(extra []byte) {
//	for _, preplayer := range preplayers {
//		preplayer.setExtra(extra)
//	}
//}
//
//// setEtherbase sets the etherbase used to initialize the block coinbase field.
//func (preplayers Preplayers) setEtherbase(addr common.Address) {
//	for _, preplayer := range preplayers {
//		preplayer.setEtherbase(addr)
//	}
//}
//
//func (preplayers Preplayers) setNodeID(nodeID string) {
//	for _, preplayer := range preplayers {
//		preplayer.setNodeID(nodeID)
//	}
//}
//
//func (preplayers Preplayers) setGlobalCache(globalCache *cache.GlobalCache) {
//	for _, preplayer := range preplayers {
//		preplayer.setGlobalCache(globalCache)
//	}
//}
//
//// start sets the running Status as 1 and triggers new work submitting.
//func (preplayers Preplayers) start() {
//	for _, preplayer := range preplayers {
//		preplayer.start()
//	}
//}
//
//// stop sets the running Status as 0.
//func (preplayers Preplayers) stop() {
//	for _, preplayer := range preplayers {
//		preplayer.stop()
//	}
//}
//
//// isRunning returns an indicator whether preplayers is running or not.
//func (preplayers Preplayers) isRunning() bool {
//	for _, preplayer := range preplayers {
//		if preplayer.isRunning() {
//			return true
//		}
//	}
//	return false
//}
//
//// close terminates all background threads maintained by the preplayers.
//// Note the preplayers does not support being closed multiple times.
//func (preplayers Preplayers) close() {
//	for _, preplayer := range preplayers {
//		preplayer.close()
//	}
//}
