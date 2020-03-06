package optipreplayer

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ivpusic/grpool"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)

type Trigger struct {
	Name        string
	ExecutorNum int

	IsTxsInDetector bool

	IsChainHeadDrive bool

	IsTxsNumDrive        bool
	PreplayedTxsNumLimit uint64

	IsTxsRatioDrive        bool
	PreplayedTxsRatioLimit float64

	IsBlockCntDrive           bool
	PreplayedBlockNumberLimit uint64

	IsPriceRankDrive        bool
	PreplayedPriceRankLimit uint64
}

type Preplayer struct {
	nodeID string
	config *params.ChainConfig
	engine consensus.Engine
	eth    Backend
	chain  *core.BlockChain

	exitCh chan struct{}

	mu    sync.RWMutex // The lock used to protect the coinbase and extra fields
	extra []byte

	running int32 // The indicator whether the consensus engine is running or not.
	trigger *Trigger

	// Cache
	globalCache *cache.GlobalCache

	taskBuilder *TaskBuilder
	routinePool *grpool.Pool
}

func NewPreplayer(config *params.ChainConfig, engine consensus.Engine, eth Backend, gasFloor, gasCeil uint64, listener *Listener) *Preplayer {
	taskBuilder := NewTaskBuilder(config, engine, eth, gasFloor, gasCeil, listener)
	trigger := &Trigger{
		Name:                      "TxsBlock1P1",
		ExecutorNum:               10,
		IsBlockCntDrive:           true,
		PreplayedBlockNumberLimit: 1,
	}
	preplayer := &Preplayer{
		config:      config,
		engine:      engine,
		eth:         eth,
		chain:       eth.BlockChain(),
		exitCh:      make(chan struct{}),
		trigger:     trigger,
		taskBuilder: taskBuilder,
		routinePool: grpool.NewPool(trigger.ExecutorNum, trigger.ExecutorNum),
	}

	go taskBuilder.mainLoop()
	taskBuilder.startCh <- struct{}{}

	for i := 0; i < trigger.ExecutorNum; i++ {
		preplayer.routinePool.JobQueue <- func() {
			preplayer.mainLoop()
		}
	}

	return preplayer
}

func (p *Preplayer) mainLoop() {
	taskQueue := p.taskBuilder.taskQueue
	for {
		if task := taskQueue.popTask(); task == nil {
			p.wait()
		} else {
			if task.isValid() {
				if txnOrder, ok := <-task.nextOrder; ok {
					p.commitNewWork(task, txnOrder)
					p.taskBuilder.preplayLog.reportGroupPreplay(task)
					task.preplayCount++
					task.priority = task.preplayCount * task.txnCount
					if task.preplayCount < 1 {
						//if task.txnCount > 1 || task.isDep() {
						taskQueue.pushTask(task)
					} else {
						task.setInvalid()
						<-task.nextOrder
					}
				}
			} else {
				<-task.nextOrder
			}
		}
	}
}

// setExtra sets the content used to initialize the block extra field.
func (p *Preplayer) setExtra(extra []byte) {
	p.taskBuilder.setExtra(extra)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.extra = extra
}

func (p *Preplayer) setNodeID(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nodeID = nodeID
}

func (p *Preplayer) setGlobalCache(globalCache *cache.GlobalCache) {
	p.taskBuilder.setGlobalCache(globalCache)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.globalCache = globalCache
}

// start sets the running Status as 1 and triggers new work submitting.
func (p *Preplayer) start() {
	atomic.StoreInt32(&p.running, 1)
}

// stop sets the running Status as 0.
func (p *Preplayer) stop() {
	atomic.StoreInt32(&p.running, 0)
}

// isRunning returns an indicator whether preplayers is running or not.
func (p *Preplayer) isRunning() bool {
	return atomic.LoadInt32(&p.running) == 1
}

// close terminates all background threads maintained by the preplayers.
// Note the preplayers does not support being closed multiple times.
func (p *Preplayer) close() {
	p.taskBuilder.close()
	p.routinePool.Release()
	close(p.exitCh)
}

func (p *Preplayer) wait() {
	time.Sleep(200 * time.Millisecond)
}

func (p *Preplayer) commitNewWork(task *TxnGroup, txnOrder TxnOrder) {
	if p.nodeID == "" || p.globalCache == nil {
		return
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	parent := task.parent
	parentHash := parent.Hash()
	parentNumber := parent.Number()

	// Pre setting
	pendingList := types.Transactions{}
	for _, txns := range task.txns {
		pendingList = append(pendingList, txns...)
		for _, txn := range txns {
			if p.globalCache.GetTxPreplay(txn.Hash()) == nil {
				p.globalCache.CommitTxPreplay(cache.NewTxPreplay(txn))
			}
		}
	}

	orderMap := make(map[common.Hash]int)
	for index, txn := range txnOrder {
		orderMap[txn] = index
	}

	totalDifficulty := p.chain.GetTd(parentHash, parent.NumberU64())
	if totalDifficulty == nil {
		totalDifficulty = new(big.Int)
	}
	currentState := &cache.CurrentState{
		PreplayID:         p.nodeID,
		PreplayName:       p.trigger.Name,
		Number:            parent.NumberU64(),
		Hash:              parentHash.Hex(),
		RawHash:           parentHash,
		Txs:               parent.Transactions(),
		TotalDifficulty:   totalDifficulty.String(),
		SnapshotTimestamp: time.Now().UnixNano() / 1000000,
	}

	header := &types.Header{
		ParentHash: parentHash,
		Number:     parentNumber.Add(parentNumber, common.Big1),
		GasLimit:   task.header.gasLimit,
		Coinbase:   task.header.coinbase,
		Extra:      p.extra,
		Time:       task.header.time,
	}

	executor := NewExecutor("0", p.config, p.engine, p.chain, p.eth.ChainDb(), orderMap,
		task.txns, pendingList, currentState, p.trigger, nil, false)

	executor.RoundID = p.globalCache.NewRoundID()

	currentState.StartTimeString = time.Now().Format("2006-01-02 15:04:05")

	// Execute, use pending for preplay
	executor.commit(task.header.coinbase, parent, header, task.txns)

	// Update Cache, need initialize
	p.globalCache.CommitTxResult(executor.RoundID, currentState, task.txns, executor.resultMap)

	currentState.EndTimeString = time.Now().Format("2006-01-02 15:04:05")

	settings := p.chain.GetVMConfig().MSRAVMSettings
	if settings.PreplayRecord && !settings.Silent {
		p.globalCache.PreplayPrint(executor.RoundID, executor.executionOrder, currentState)
	}

	p.chain.Warmuper.AddWarmupTask(executor.RoundID, executor.executionOrder, parent.Root())
}

type Preplayers []*Preplayer

func NewPreplayers(eth Backend, config *params.ChainConfig, engine consensus.Engine, gasFloor, gasCeil uint64, listener *Listener) Preplayers {
	preplayers := Preplayers{}
	for i := 0; i < 1; i++ {
		preplayers = append(preplayers, NewPreplayer(config, engine, eth, gasFloor, gasCeil, listener))
	}
	return preplayers
}

// setExtra sets the content used to initialize the block extra field.
func (preplayers Preplayers) setExtra(extra []byte) {
	for _, preplayer := range preplayers {
		preplayer.setExtra(extra)
	}
}

func (preplayers Preplayers) setNodeID(nodeID string) {
	for _, preplayer := range preplayers {
		preplayer.setNodeID(nodeID)
	}
}

func (preplayers Preplayers) setGlobalCache(globalCache *cache.GlobalCache) {
	for _, preplayer := range preplayers {
		preplayer.setGlobalCache(globalCache)
	}
}

// start sets the running Status as 1 and triggers new work submitting.
func (preplayers Preplayers) start() {
	for _, preplayer := range preplayers {
		preplayer.start()
	}
}

// stop sets the running Status as 0.
func (preplayers Preplayers) stop() {
	for _, preplayer := range preplayers {
		preplayer.stop()
	}
}

// isRunning returns an indicator whether preplayers is running or not.
func (preplayers Preplayers) isRunning() bool {
	for _, preplayer := range preplayers {
		if preplayer.isRunning() {
			return true
		}
	}
	return false
}

// close terminates all background threads maintained by the preplayers.
// Note the preplayers does not support being closed multiple times.
func (preplayers Preplayers) close() {
	for _, preplayer := range preplayers {
		preplayer.close()
	}
}
