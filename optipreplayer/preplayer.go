package optipreplayer

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/optipreplayer/config"
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
	// executorNum is the number of executor in preplayer.
	executorNum = 10
)

type Preplayer struct {
	nodeID string
	config *params.ChainConfig
	engine consensus.Engine
	eth    Backend
	chain  *core.BlockChain

	gasFloor uint64
	gasCeil  uint64

	exitCh chan struct{}

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

	// Package
	minerList *MinerList

	// Transaction distributor
	nowHeader *Header

	// Task queue
	groupTaskQueue   *TaskQueue
	preplayTaskQueue *TaskQueue

	// Log groups and transactions
	preplayLog *PreplayLog

	taskBuilder  *TaskBuilder
	missReporter *MissReporter
	routinePool  *grpool.Pool
}

func NewPreplayer(config *params.ChainConfig, engine consensus.Engine, eth Backend, gasFloor, gasCeil uint64, listener *Listener) *Preplayer {
	mu := new(sync.RWMutex)
	taskBuilder := NewTaskBuilder(config, eth, mu, TYPE0)
	preplayer := &Preplayer{
		config:           config,
		engine:           engine,
		eth:              eth,
		chain:            eth.BlockChain(),
		gasFloor:         gasFloor,
		gasCeil:          gasCeil,
		exitCh:           make(chan struct{}),
		trigger:          NewTrigger("TxsBlock1P1", executorNum),
		builderMu:        mu,
		listener:         listener,
		minerList:        NewMinerList(eth.BlockChain()),
		nowHeader:        new(Header),
		groupTaskQueue:   NewTaskQueue(),
		preplayTaskQueue: NewTaskQueue(),
		preplayLog:       NewPreplayLog(),
		taskBuilder:      taskBuilder,
		missReporter:     NewMissReporter(config.ChainID, eth.BlockChain().GetVMConfig().MSRAVMSettings.ReportMissDetail),
		routinePool:      grpool.NewPool(executorNum*2, executorNum*2),
	}
	preplayer.taskBuilder.setPreplayer(preplayer)
	preplayer.missReporter.preplayer = preplayer

	go taskBuilder.mainLoop()

	go preplayer.listenLoop()
	for i := 0; i < executorNum; i++ {
		preplayer.routinePool.JobQueue <- func() {
			preplayer.groupLoop()
		}
	}
	for i := 0; i < executorNum; i++ {
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

	go func() {
		var waitForNewTx func()
		p.taskBuilder.setMinPrice, waitForNewTx = p.listener.register()

		for {
			waitForNewTx()
			p.taskBuilder.startCh <- struct{}{}
			<-p.taskBuilder.finishOnceCh
		}
	}()

	for {
		select {
		case chainHeadEvent := <-chainHeadCh:
			p.builderMu.Lock()
			currentBlock := chainHeadEvent.Block
			p.minerList.addMiner(currentBlock.Coinbase())
			p.nowHeader.coinbase = p.minerList.topActive[0]
			p.nowHeader.time = p.globalCache.GetPreplayTimeStamp()
			p.nowHeader.gasLimit = core.CalcGasLimit(currentBlock, p.gasFloor, p.gasCeil)
			p.taskBuilder.chainHeadUpdate(currentBlock)
			p.preplayLog.disableGroup()
			p.preplayLog.printAndClearLog(currentBlock.NumberU64(), p.preplayTaskQueue.countTask())
			p.builderMu.Unlock()
		case <-p.exitCh:
			return
		}
	}
}

func (p *Preplayer) groupLoop() {
	for {
		if task := p.groupTaskQueue.popTask(); task == nil {
			time.Sleep(20 * time.Millisecond)
		} else {
			if orderAndHeader, ok := <-task.nextOrderAndHeader; ok {
				_, roundID := p.commitNewWork(task, orderAndHeader.order, orderAndHeader.header)
				task.roundIDCh <- roundID
			}
		}
	}
}

func (p *Preplayer) mainLoop() {
	for {
		if task := p.preplayTaskQueue.popTask(); task == nil {
			time.Sleep(50 * time.Millisecond)
		} else {
			if task.isValid() {
				if orderAndHeader, ok := <-task.nextOrderAndHeader; ok {
					resultMap, roundID := p.commitNewWork(task, orderAndHeader.order, orderAndHeader.header)
					if task.roundIDCh != nil {
						task.roundIDCh <- roundID
					}
					p.preplayLog.reportGroupPreplay(task)
					task.updateByPreplay(resultMap, orderAndHeader)
					if task.getPreplayCount() < config.TXN_PREPLAY_ROUND_LIMIT {
						p.preplayTaskQueue.pushTask(task)
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

func (p *Preplayer) commitNewWork(task *TxnGroup, txnOrder TxnOrder, forecastHeader Header) (map[common.Hash]*cache.ExtraResult, uint64) {
	if p.nodeID == "" || p.globalCache == nil {
		return nil, 0
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	parent := task.parent
	parentHash := parent.Hash()
	parentNumber := parent.Number()
	parentNumberU64 := parent.NumberU64()

	orderMap := make(map[common.Hash]int)
	for index, txn := range txnOrder {
		orderMap[txn] = index
	}

	totalDifficulty := p.chain.GetTd(parentHash, parentNumberU64)
	if totalDifficulty == nil {
		totalDifficulty = new(big.Int)
	}
	currentState := &cache.CurrentState{
		PreplayID:         p.nodeID,
		PreplayName:       p.trigger.Name,
		Number:            parentNumberU64,
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

	executor := NewExecutor("0", p.config, p.engine, p.chain, p.eth.ChainDb(), orderMap, task.txnPool, currentState,
		p.trigger, nil, false, true, task.basicPreplay, task.addrNotCopy)

	executor.RoundID = p.globalCache.NewRoundID()

	if task.basicPreplay {
		neededPreplayCount := new(big.Int)
		neededPreplayCount.Mul(task.orderCount, new(big.Int).SetInt64(int64(task.chainFactor)))
		if (task.RWRecord != nil && task.isChainDep()) ||
			neededPreplayCount.Cmp(new(big.Int).SetInt64(int64(config.TXN_PREPLAY_ROUND_LIMIT))) > 0 ||
			task.getPreplayCount() == 0{
			executor.EnableReuseTracer = true
		}

		currentState.StartTimeString = time.Now().Format("2006-01-02 15:04:05")
	}

	// Execute, use pending for preplay
	executor.commit(header.Coinbase, parent, header, task.txnPool)

	if task.basicPreplay {
		// Update Cache, need initialize
		p.globalCache.CommitTxResult(executor.RoundID, currentState, task.txnPool, executor.resultMap)

		currentState.EndTimeString = time.Now().Format("2006-01-02 15:04:05")

		settings := p.chain.GetVMConfig().MSRAVMSettings
		if settings.PreplayRecord && !settings.Silent {
			p.globalCache.PreplayPrint(executor.RoundID, executor.executionOrder, currentState)
		}
	}

	var rounds = make([]*cache.PreplayResult, 0, len(executor.executionOrder))
	for _, tx := range executor.executionOrder {
		if txPreplay := p.globalCache.PeekTxPreplay(tx.Hash()); txPreplay != nil {
			txPreplay.RLockRound()
			if round, _ := txPreplay.PeekRound(executor.RoundID); round != nil {
				rounds = append(rounds, round)
			}
			txPreplay.RUnlockRound()
		}
	}

	p.reportWobjectCopy(rounds)

	p.chain.Warmuper.AddWarmupTask(rounds, parent.Root())

	return executor.resultMap, executor.RoundID
}

func (p *Preplayer) reportWobjectCopy(rounds []*cache.PreplayResult) {
	for _, round := range rounds {
		addrList := make([]common.Address, 0, len(round.WObjectWeakRefs))
		for addr := range round.WObjectWeakRefs {
			addrList = append(addrList, addr)
		}
		p.preplayLog.reportWobjectCopy(round.WObjectCopy, round.WObjectNotCopy, addrList)
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
