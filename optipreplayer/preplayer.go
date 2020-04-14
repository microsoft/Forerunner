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

const (
	// chainHeadChanSize is the size of channel listening to ChainHeadEvent.
	chainHeadChanSize = 10
)

type Preplayer struct {
	nodeID string
	config *params.ChainConfig
	engine consensus.Engine
	eth    Backend
	chain  *core.BlockChain

	gasFloor uint64
	gasCeil  uint64

	builder0Finish chan struct{}
	builder1Finish chan struct{}
	exitCh         chan struct{}

	mu    sync.RWMutex // The lock used to protect the coinbase and extra fields
	extra []byte

	running int32 // The indicator whether the consensus engine is running or not.
	trigger *Trigger

	// The lock used to protect two task builders
	builderMu *sync.RWMutex

	// Cache
	globalCache *cache.GlobalCache

	// Listener
	listener *Listener

	minerList    *MinerList
	nowHeader    *Header
	taskQueue    *TaskQueue
	preplayLog   *PreplayLog
	taskBuilder0 *TaskBuilder
	taskBuilder1 *TaskBuilder
	missReporter *MissReporter
	routinePool  *grpool.Pool
}

func NewPreplayer(config *params.ChainConfig, engine consensus.Engine, eth Backend, gasFloor, gasCeil uint64, listener *Listener) *Preplayer {
	builder0Finish := make(chan struct{})
	builder1Finish := make(chan struct{})
	mu := new(sync.RWMutex)
	minerList := NewMinerList(eth.BlockChain())
	nowHeader := new(Header)
	taskQueue := NewTaskQueue()
	preplayLog := NewPreplayLog()
	taskBuilder0 := NewTaskBuilder(config, engine, eth, builder0Finish, mu, listener, true, minerList, nowHeader,
		taskQueue, preplayLog)
	taskBuilder1 := NewTaskBuilder(config, engine, eth, builder1Finish, mu, listener, false, minerList, nowHeader,
		taskQueue, preplayLog)
	trigger := NewTrigger("TxsBlock1P1", 10)
	preplayer := &Preplayer{
		config:         config,
		engine:         engine,
		eth:            eth,
		chain:          eth.BlockChain(),
		gasFloor:       gasFloor,
		gasCeil:        gasCeil,
		builder0Finish: builder0Finish,
		builder1Finish: builder1Finish,
		exitCh:         make(chan struct{}),
		trigger:        trigger,
		builderMu:      mu,
		listener:       listener,
		minerList:      minerList,
		nowHeader:      nowHeader,
		taskQueue:      taskQueue,
		preplayLog:     preplayLog,
		taskBuilder0:   taskBuilder0,
		taskBuilder1:   taskBuilder1,
		missReporter:   NewMissReporter(config),
		routinePool:    grpool.NewPool(trigger.ExecutorNum, trigger.ExecutorNum),
	}
	preplayer.missReporter.preplayer = preplayer

	go taskBuilder0.mainLoop()
	go taskBuilder1.mainLoop()

	go preplayer.listenLoop()
	go func() {
		preplayer.builder0Finish <- struct{}{}
		preplayer.builder1Finish <- struct{}{}
	}()

	for i := 0; i < trigger.ExecutorNum; i++ {
		preplayer.routinePool.JobQueue <- func() {
			preplayer.mainLoop()
		}
	}

	return preplayer
}

func (p *Preplayer) listenLoop() {
	chainHeadCh := make(chan core.ChainHeadEvent, chainHeadChanSize)
	chainHeadSub := p.chain.SubscribeChainHeadEvent(chainHeadCh)
	defer chainHeadSub.Unsubscribe()

	for {
		select {
		case <-p.builder0Finish:
			p.listener.waitForNewTx()
			p.taskBuilder0.startCh <- struct{}{}
		case <-p.builder1Finish:
			if !p.taskBuilder1.reachLast {
				p.taskBuilder1.startCh <- struct{}{}
			}
		case chainHeadEvent := <-chainHeadCh:
			p.builderMu.Lock()
			if p.taskBuilder1.reachLast {
				p.taskBuilder1.reachLast = false
				p.taskBuilder1.startCh <- struct{}{}
			}

			currentBlock := chainHeadEvent.Block
			p.minerList.addMiner(currentBlock.Coinbase())
			p.nowHeader.coinbase = p.minerList.top5Active[0]
			p.nowHeader.time = p.globalCache.GetTimeStamp()
			p.nowHeader.gasLimit = core.CalcGasLimit(currentBlock, p.gasFloor, p.gasCeil)
			p.taskBuilder0.chainHeadUpdate(currentBlock)
			p.taskBuilder1.chainHeadUpdate(currentBlock)
			p.preplayLog.printAndClearLog(currentBlock.NumberU64(), p.taskQueue.countTask())
			p.builderMu.Unlock()
		case <-p.exitCh:
			return
		}
	}
}

func (p *Preplayer) mainLoop() {
	for {
		if task := p.taskQueue.popTask(); task == nil {
			p.wait()
		} else {
			if task.isValid() {
				if orderAndHeader, ok := <-task.nextOrderAndHeader; ok {
					p.commitNewWork(task, orderAndHeader.order, orderAndHeader.header)
					p.preplayLog.reportGroupPreplay(task)
					task.priority = task.preplayCount * task.txnCount
					task.preplayHistory = append(task.preplayHistory, orderAndHeader.order)
					task.timeHistory = append(task.timeHistory, orderAndHeader.header.time)
					if task.preplayCount < 6 {
						//if task.txnCount > 1 || task.isChainDep() {
						p.taskQueue.pushTask(task)
					} else {
						task.setInvalid()
						<-task.nextOrderAndHeader
					}
				}
			} else {
				<-task.nextOrderAndHeader
			}
		}
	}
}

// setExtra sets the content used to initialize the block extra field.
func (p *Preplayer) setExtra(extra []byte) {
	p.taskBuilder0.setExtra(extra)
	p.taskBuilder1.setExtra(extra)
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
	p.taskBuilder0.setGlobalCache(globalCache)
	p.taskBuilder1.setGlobalCache(globalCache)
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
	p.taskBuilder0.close()
	p.taskBuilder1.close()
	p.routinePool.Release()
	close(p.exitCh)
}

func (p *Preplayer) wait() {
	time.Sleep(200 * time.Millisecond)
}

func (p *Preplayer) commitNewWork(task *TxnGroup, txnOrder TxnOrder, forecastHeader Header) {
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
		GasLimit:   forecastHeader.gasLimit,
		Coinbase:   forecastHeader.coinbase,
		Extra:      p.extra,
		Time:       forecastHeader.time,
	}

	executor := NewExecutor("0", p.config, p.engine, p.chain, p.eth.ChainDb(), orderMap,
		task.txns, pendingList, currentState, p.trigger, nil, false, true)

	executor.RoundID = p.globalCache.NewRoundID()

	currentState.StartTimeString = time.Now().Format("2006-01-02 15:04:05")

	// Execute, use pending for preplay
	executor.commit(header.Coinbase, parent, header, task.txns)

	// Update Cache, need initialize
	p.globalCache.CommitTxResult(executor.RoundID, currentState, task.txns, executor.resultMap)

	currentState.EndTimeString = time.Now().Format("2006-01-02 15:04:05")

	settings := p.chain.GetVMConfig().MSRAVMSettings
	if settings.PreplayRecord && !settings.Silent {
		p.globalCache.PreplayPrint(executor.RoundID, executor.executionOrder, currentState)
	}

	p.chain.Warmuper.AddWarmupTask(executor.RoundID, executor.executionOrder, parent.Root())

	task.preplayCount++
	for hash, result := range executor.resultMap {
		if result.Status == "will in" {
			task.preplayCountMap[hash]++
		}
	}
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
