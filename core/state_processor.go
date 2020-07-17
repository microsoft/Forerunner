// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/params"
	"math/big"
	"os"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"
)

type TransactionApplier interface {
	ApplyTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address,
		gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
		cfg *vm.Config, blockPre *cache.BlockPre, getHashFunc vm.GetHashFunc, precompiles map[common.Address]vm.PrecompiledContract,
		pMsg *types.Message, signer types.Signer) (*types.Receipt, error, *cmptypes.ReuseStatus)

	ReuseTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address,
		gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
		cfg *vm.Config, blockPre *cache.BlockPre, asyncPool *types.SingleThreadSpinningAsyncProcessor, controller *Controller,
		getHashFunc vm.GetHashFunc, precompiles map[common.Address]vm.PrecompiledContract,
		tmsg *types.Message, signer types.Signer) (*types.Receipt, error, *cmptypes.ReuseStatus)

	ReuseTransactionPerfTest(config *params.ChainConfig, bc ChainContext, author *common.Address,
		gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
		cfg *vm.Config, blockPre *cache.BlockPre, asyncPool *types.SingleThreadSpinningAsyncProcessor, controller *Controller,
		getHashFunc vm.GetHashFunc, precompiles map[common.Address]vm.PrecompiledContract,
		pMsg *types.Message, signer types.Signer) (*types.Receipt,
		error, *cmptypes.ReuseStatus, time.Duration, time.Duration)

	PreplayTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address,
		gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
		cfg vm.Config, RoundID uint64, blockPre *cache.BlockPre, groundFlag uint64, basicPreplay, enablePause bool) (*types.Receipt, error)
}

//
//type DefaultTransactionApplier struct{}
//
//func (*DefaultTransactionApplier) ApplyTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address,
//	gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
//	cfg vm.Config, RoundID uint64, blockPre *cache.BlockPre, groundFlag uint64) (*types.Receipt, error, uint64) {
//	receipt, err := ApplyTransaction(config, bc, author, gp, statedb, header, tx, usedGas, cfg)
//	return receipt, err, 0
//}

// StateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StateProcessor implements Processor.
type StateProcessor struct {
	config         *params.ChainConfig                       // Chain configuration options
	bc             *BlockChain                               // Canonical block chain
	engine         consensus.Engine                          // Consensus engine used for block rewards
	asyncProcessor *types.SingleThreadSpinningAsyncProcessor // global processor
	logFile        *os.File
}

// NewStateProcessor initialises a new StateProcessor.
func NewStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *StateProcessor {
	sp := &StateProcessor{
		config:         config,
		bc:             bc,
		engine:         engine,
		asyncProcessor: types.NewSingleThreadAsyncProcessor(),
	}

	if bc.vmConfig.MSRAVMSettings.TxApplyPerfLogging && bc.vmConfig.MSRAVMSettings.PerfLogging {
		panic("Cannot measure tx perf and block perf at the same time!")

	}

	if bc.vmConfig.MSRAVMSettings.TxApplyPerfLogging {
		var logFileName = "/tmp/txApplyPerfLog.baseline.txt"
		if bc.vmConfig.MSRAVMSettings.CmpReuse {
			logFileName = "/tmp/txApplyPerfLog.reuse.txt"
		}
		f, _ := os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
		sp.logFile = f
	}

	return sp
}

type NoError struct {
}

func (*NoError) Error() string {
	return "NoError"
}

var noError = &NoError{}

func CmpStateDB(groundStatedb, statedb *state.StateDB, groundReceipt *types.Receipt, receipt *types.Receipt, groundErr error, err error,
	tx *types.Transaction, i int, reuseStatus *cmptypes.ReuseStatus) bool {

	groundStateObjects := groundStatedb.GetPendingStateObject()
	stateObjects := statedb.GetPendingStateObject()
	var diff bool

	if groundErr != err {
		if groundErr == nil {
			groundErr = noError
		}
		if err == nil {
			err = noError
		}
		log.Info("Err diff", "index", i, "tx", tx.Hash().Hex(),
			"status", fmt.Sprintf("%v", reuseStatus), "ground", groundErr, "our err", err)
		diff = true
	}

	if groundReceipt != nil && receipt != nil {
		if groundReceipt.Status != receipt.Status {
			log.Info("Status diff", "index", i, "tx", tx.Hash().Hex(),
				"status", fmt.Sprintf("%v", reuseStatus), "ground", groundReceipt.Status, "our status", receipt.Status)
			diff = true
		}
		if groundReceipt.GasUsed != receipt.GasUsed {
			log.Info("Gasused diff", "index", i, "tx", tx.Hash().Hex(),
				"status", fmt.Sprintf("%v", reuseStatus), "ground", groundReceipt.GasUsed, "our gasused", receipt.GasUsed)
			diff = true
		}
		if len(groundReceipt.Logs) != len(receipt.Logs) {
			log.Info("Logs len diff", "index", i, "tx", tx.Hash().Hex(),
				"status", fmt.Sprintf("%v", reuseStatus), "ground", len(groundReceipt.Logs), "our log len", len(receipt.Logs))
			diff = true
		}

		for i, glog := range groundReceipt.Logs {
			rlog := receipt.Logs[i]
			gb, _ := json.Marshal(glog)
			lb, _ := json.Marshal(rlog)
			gs, ls := string(gb), string(lb)
			if gs != ls {
				log.Info("Log diff", "index", i, "tx", tx.Hash().Hex(),
					"status", fmt.Sprintf("%v", reuseStatus), "logIndex", i, "ground", gs, "our log", ls)
				diff = true
			}
		}

	}

	for address, groundObject := range groundStateObjects {
		if stateObject, ok := stateObjects[address]; ok {
			// we do not use groundStatedb.HasSuicided(address) because that after finalise,
			// a suicided object will be marked as deleted, which make make HasSuicided return false
			if groundStatedb.Exist(address) != statedb.Exist(address) {
				log.Info("Exist diff", "addr", address, "index", i, "tx", tx.Hash().Hex(),
					"status", fmt.Sprintf("%v", reuseStatus),
					"ground", groundStatedb.Exist(address), "our exist", statedb.Exist(address))
				diff = true
			}
			if !groundStatedb.Exist(address) {
				continue // do not check the details of suicided account
			}

			if groundStatedb.Exist(address) != statedb.Exist(address) {
				log.Info("Exist diff", "addr", address, "index", i, "tx", tx.Hash().Hex(),
					"status", fmt.Sprintf("%v", reuseStatus),
					"ground", groundStatedb.Exist(address), "our balance", statedb.Exist(address))
				diff = true
			}
			if groundStatedb.Empty(address) != statedb.Empty(address) {
				log.Info("Empty diff", "addr", address, "index", i, "tx", tx.Hash().Hex(),
					"status", fmt.Sprintf("%v", reuseStatus),
					"ground", groundStatedb.Empty(address), "our balance", statedb.Empty(address))
				diff = true
			}
			if groundObject.Balance().Cmp(stateObject.Balance()) != 0 {
				diffValue := new(big.Int).Sub(groundObject.Balance(), stateObject.Balance())
				log.Info("Balance diff", "addr", address, "index", i, "tx", tx.Hash().Hex(),
					"status", fmt.Sprintf("%v", reuseStatus), "diff", diffValue,
					"ground", groundObject.Balance(), "our balance", stateObject.Balance())
				diff = true
			}
			if groundObject.Nonce() != stateObject.Nonce() {
				log.Info("Nonce diff", "addr", address, "index", i, "tx", tx.Hash().Hex(),
					"status", fmt.Sprintf("%s", reuseStatus),
					"ground", groundObject.Nonce(), "our nonce", stateObject.Nonce())
				diff = true
			}
			groundStorage := groundObject.GetPendingStorage()
			for key, groundValue := range groundStorage {
				value := statedb.GetState(address, key)
				if groundValue != value {
					log.Info("Storage diff", "addr", address, "index", i, "tx", tx.Hash().Hex(),
						"status", fmt.Sprintf("%v", reuseStatus),
						"key", key, "ground", groundValue, "our state", value)
					diff = true
				}
			}
			statedbStorage := stateObject.GetPendingStorage()
			for key, value := range statedbStorage {
				groundValue := groundStatedb.GetState(address, key)
				if groundValue != value {
					log.Info("Storage diff", "addr", address, "index", i, "tx", tx.Hash().Hex(),
						"status", fmt.Sprintf("%v", reuseStatus),
						"key", key, "ground", groundValue, "our state", value)
					diff = true
				}
			}
		} else {
			// we do not use groundStatedb.HasSuicided(address) because that after finalise,
			// a suicided object will be marked as deleted, which make make HasSuicided return false
			if !groundStatedb.Exist(address) {
				continue // if the suicided contract is created within the tx, the state object will not be created by reuse
			}
			log.Info("Miss object in our state object", "addr", address, "index", i,
				"status", fmt.Sprintf("%v", reuseStatus),
				"ground db size", len(groundStateObjects), "our db size", len(stateObjects), "tx", tx.Hash().Hex())
			diff = true
		}
	}
	return diff
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
func (p *StateProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error) {
	var (
		receipts types.Receipts
		usedGas  = new(uint64)
		header   = block.Header()
		allLogs  []*types.Log
		gp       = new(GasPool).AddGas(block.GasLimit())

		groundGP      = new(GasPool).AddGas(block.GasLimit())
		groundUsedGas = new(uint64)
	)

	p.asyncProcessor.Start()
	defer p.asyncProcessor.Pause()

	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}

	if cfg.MSRAVMSettings.HasherParallelism > 0 {
		statedb.IsParallelHasher = true
	}

	// Record ground truth
	var groundStatedb *state.StateDB
	if cfg.MSRAVMSettings.GroundRecord {
		var err error
		groundStatedb, err = state.New(p.bc.GetBlockByHash(block.ParentHash()).Root(), p.bc.stateCache)
		if err != nil {
			panic("Failed to create groundStateDB")
		}
		groundStatedb.SetRWMode(true)
	}
	if cfg.MSRAVMSettings.CmpReuse && (cfg.MSRAVMSettings.CmpReuseChecking || cfg.MSRAVMSettings.TxApplyPerfLogging) {
		var err error
		groundStatedb, err = state.New(p.bc.GetBlockByHash(block.ParentHash()).Root(), p.bc.stateCache)
		if err != nil {
			panic("Failed to create groundStateDB")
		}
	}

	var blockPre *cache.BlockPre
	if cfg.MSRAVMSettings.IsEmulateMode{
		blockPre = p.bc.MSRACache.PeekBlockPre(block.Hash())
		if blockPre == nil{
			panic("there is a nil blockPre")
		}
	}else {
		// Add Block
		blockPre = p.bc.CommitBlockPre(block)
	}
	var reuseResult []*cmptypes.ReuseStatus

	if cfg.MSRAVMSettings.CmpReuse && cfg.MSRAVMSettings.ParallelizeReuse {
		statedb.ShareCopy()
	}
	var controller *Controller
	if cfg.MSRAVMSettings.ParallelizeReuse {
		controller = NewController()
	}

	signer := types.MakeSigner(p.config, header.Number)

	// here we avoid using channels to optimize out the wait
	var txMsgs []unsafe.Pointer
	pipelineHashingAndMsgCreation := cfg.MSRAVMSettings.PipelinedBloom && len(block.Transactions()) > 1
	if pipelineHashingAndMsgCreation {
		txMsgs = make([]unsafe.Pointer, len(block.Transactions()))
		go func() {
			for i, tx := range block.Transactions() {
				tx.Hash() // it caches the result internally
				tmsg, err := tx.AsMessage(signer)
				if err != nil {
					atomic.StorePointer(&txMsgs[i], unsafe.Pointer(&tmsg))
				}
			}
		}()
	}

	//asyncPool := types.NewSingleThreadAsyncProcessor()
	//defer asyncPool.Stop()

	blockHash := block.Hash()
	printCmp := true

	getHashFunc := GetHashFn(header, p.bc)
	chainConfig := p.config.Rules(header.Number)
	precompiles := vm.GetPrecompiledMapping(&chainConfig)

	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		statedb.Prepare(tx.Hash(), blockHash, i)

		var err error
		var receipt *types.Receipt
		t0 := time.Now()

		if cfg.MSRAVMSettings.GroundRecord && groundStatedb != nil {
			// Record ground truth
			//log.Info("GroundTruth")
			// var groundStatedb vm.StateDB
			// groundStatedb = state.NewRWStateDB(statedb.Copy())
			groundStatedb.Prepare(tx.Hash(), block.Hash(), i)
			receipt, err = p.bc.Cmpreuse.PreplayTransaction(p.config, p.bc, nil, groundGP, groundStatedb, header, tx,
				groundUsedGas, cfg, 0, blockPre, 1, true, false)
			//log.Info("GroundTruth Finish")
		}

		if cfg.MSRAVMSettings.CmpReuse {
			var reuseStatus *cmptypes.ReuseStatus
			var pMsg *types.Message
			if pipelineHashingAndMsgCreation {
				pMsg = (*types.Message)(atomic.LoadPointer(&txMsgs[i]))
			}

			if cfg.MSRAVMSettings.TxApplyPerfLogging {
				var reuseDuration, applyDuration time.Duration
				receipt, err, reuseStatus, reuseDuration, applyDuration = p.bc.Cmpreuse.ReuseTransactionPerfTest(
					p.config, p.bc, nil, gp, statedb, header,
					tx, usedGas, &cfg, blockPre, p.asyncProcessor, controller, getHashFunc, precompiles, pMsg, signer)

				groundStatedb.Prepare(tx.Hash(), block.Hash(), i)
				_, _, groundApply, _ := ApplyTransactionPerfTest(p.config, p.bc, nil, groundGP, groundStatedb, header, tx, groundUsedGas, cfg)

				reuseStr := reuseStatus.BaseStatus.String()
				txListen := p.bc.MSRACache.GetTxListen(tx.Hash())
				confirmationDelaySecond := 0.0

				if txListen != nil && txListen.ListenTimeNano < blockPre.ListenTimeNano {
					confirmationDelayNano := blockPre.ListenTimeNano - txListen.ListenTimeNano
					confirmationDelaySecond = float64(confirmationDelayNano) / float64(1000*1000*1000)
				} else {
					reuseStr = "NoListen"
				}

				if reuseStatus.BaseStatus == cmptypes.Hit {
					reuseStr += "-" + reuseStatus.HitType.String()
					if reuseStatus.HitType == cmptypes.MixHit {
						reuseStr += "-" + reuseStatus.MixHitStatus.MixHitType.String()
					}
					if reuseStatus.HitType == cmptypes.TraceHit {
						reuseStr += "-" + reuseStatus.TraceHitStatus.String()
					}
				}
				txHash := tx.Hash().Hex()
				rd := reuseDuration.Microseconds()
				ad := applyDuration.Microseconds()
				gad := groundApply.Microseconds()
				gu := receipt.GasUsed
				sar := float64(applyDuration) / float64(reuseDuration)
				sgr := float64(groundApply) / float64(reuseDuration)
				sga := float64(groundApply) / float64(applyDuration)
				uniqId := block.Hash().Hex() + "_" + strconv.Itoa(i)
				go func() {
					result := fmt.Sprintf("[%s] CmpReusePerf id %v tx %v delay %.1f reuse %v apply %v groundApply %v status %v gas %v speedup-g/a %v speedup-a/r %v speedup-g/r %v\n",
						time.Now().Format("01-02|15:04:05.000"), uniqId, txHash, confirmationDelaySecond, rd, ad, gad, reuseStr, gu, sga, sar, sgr)
					p.logFile.WriteString(result)
				}()
			} else {
				txStart := time.Now()
				if cfg.MSRAVMSettings.ParallelizeReuse {
					receipt, err, reuseStatus = p.bc.Cmpreuse.ReuseTransaction(p.config, p.bc, nil, gp, statedb, header,
						tx, usedGas, &cfg, blockPre, p.asyncProcessor, controller, getHashFunc, precompiles, pMsg, signer)
				} else {
					receipt, err, reuseStatus = p.bc.Cmpreuse.ApplyTransaction(p.config, p.bc, nil, gp, statedb, header,
						tx, usedGas, &cfg, blockPre, getHashFunc, precompiles, pMsg, signer)
				}
				txDuration := time.Since(txStart)
				if cfg.MSRAVMSettings.PerfLogging {
					txListen := p.bc.MSRACache.GetTxListen(tx.Hash())
					confirmationDelaySecond := -1.0
					if txListen != nil && txListen.ListenTimeNano < blockPre.ListenTimeNano {
						confirmationDelayNano := blockPre.ListenTimeNano - txListen.ListenTimeNano
						confirmationDelaySecond = float64(confirmationDelayNano) / float64(1000*1000*1000)
					}
					statedb.AddTxPerf(receipt, txDuration, reuseStatus, confirmationDelaySecond)
				}
			}
			reuseResult = append(reuseResult, reuseStatus)
			if reuseStatus.BaseStatus == cmptypes.Unknown {
				statedb.UnknownTxs = append(statedb.UnknownTxs, tx)
				statedb.UnknownTxReceipts = append(statedb.UnknownTxReceipts, receipt)
			}

			if cfg.MSRAVMSettings.CmpReuseChecking {
				groundStatedb.Prepare(tx.Hash(), block.Hash(), i)
				groundReceipt, groundErr := ApplyTransaction(p.config, p.bc, nil, groundGP, groundStatedb, header, tx, groundUsedGas, cfg)
				if printCmp {
					//groundRoot := groundStatedb.IntermediateRoot(true)
					//ourRoot := statedb.IntermediateRoot(true)
					//if groundRoot != ourRoot {
					//log.Info("Root diff", "index", i, "tx", tx.Hash().Hex(),
					//	"status", fmt.Sprintf("%v", reuseStatus), "ground", groundRoot, "our root", ourRoot)
					diff := CmpStateDB(groundStatedb, statedb, groundReceipt, receipt, err, groundErr, tx, i, reuseStatus)
					//printCmp = false
					if diff {
						panic("Reuse state diff!")
					}
					//}
				}
			}

			var checkDb = statedb
			if pair := statedb.GetPair(); pair != nil && reuseStatus.BaseStatus != cmptypes.Hit {
				checkDb = pair
			}
			if checkDb.HaveMiss() {
				word := "Should(Hit)"
				if reuseStatus.BaseStatus != cmptypes.Hit {
					word = fmt.Sprintf("May(%s)", reuseStatus.BaseStatus)
					if reuseStatus.BaseStatus == cmptypes.NoPreplay {
						txListen := p.bc.MSRACache.GetTxListen(tx.Hash())
						if txListen == nil || txListen.ListenTimeNano > blockPre.ListenTimeNano {
							word = "Can't(NoListen)"
						}
					}
				}
				cache.WarmupMissTxnCount[word]++
				cache.AccountCreate[word] += statedb.AccountCreate
				cache.AddrWarmupMiss[word] += statedb.AddrWarmupMiss
				cache.KeyWarmupMiss[word] += statedb.KeyWarmupMiss
				cache.AddrCreateWarmupMiss[word] += len(statedb.AddrCreateWarmupMiss)
				cache.KeyCreateWarmupMiss[word] += statedb.KeyCreateWarmupMiss

				if cfg.MSRAVMSettings.WarmupMissDetail {
					cache.AddrNoWarmup[word] += statedb.AddrNoWarmup
					cache.KeyNoWarmup[word] += statedb.KeyNoWarmup
				}
			}

			statedb.ClearMiss()
			if pair := statedb.GetPair(); pair != nil {
				pair.ClearMiss()
			}
		} else {
			if cfg.MSRAVMSettings.TxApplyPerfLogging {
				var baseApply time.Duration
				receipt, _, baseApply, _ = ApplyTransactionPerfTest(p.config, p.bc, nil, gp, statedb, header, tx, usedGas, cfg)
				txHash := tx.Hash().Hex()
				bad := baseApply.Microseconds()
				gu := receipt.GasUsed
				uniqId := block.Hash().Hex() + "_" + strconv.Itoa(i)
				go func() {
					result := fmt.Sprintf("[%s] BaselinePerf id %v tx %v baselineApply %v gas %v\n",
						time.Now().Format("01-02|15:04:05.000"), uniqId, txHash, bad, gu)
					p.logFile.WriteString(result)
				}()
			} else {
				txStart := time.Now()
				receipt, err = ApplyTransaction(p.config, p.bc, nil, gp, statedb, header, tx, usedGas, cfg)
				txDuration := time.Since(txStart)
				if cfg.MSRAVMSettings.PerfLogging {
					confirmationDelaySecond := -1.0
					if cfg.MSRAVMSettings.EnablePreplay {
						txListen := p.bc.MSRACache.GetTxListen(tx.Hash())
						if txListen != nil && txListen.ListenTimeNano < blockPre.ListenTimeNano {
							confirmationDelayNano := blockPre.ListenTimeNano - txListen.ListenTimeNano
							confirmationDelaySecond = float64(confirmationDelayNano) / float64(1000*1000*1000)
						}
					}
					statedb.AddTxPerf(receipt, txDuration, nil, confirmationDelaySecond)
				}

			}
		}
		t1 := time.Now()
		cache.Apply = append(cache.Apply, t1.Sub(t0))
		//log.Debug("[TransactionProcessTime]", "pTxHash", tx.Hash(), "processTime", t1.Sub(t0).Nanoseconds())

		if err != nil {
			log.Info("GroundTruth Error", "err", err.Error(), "tx", tx.Hash())
			return nil, nil, 0, err
		}
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
		if cache.ToScreen && cache.Apply[len(cache.Apply)-1] > time.Millisecond*50 {
			cache.LongExecutionCost += cache.Apply[len(cache.Apply)-1]
			if cache.Apply[len(cache.Apply)-1] > cache.MaxLongExecutionCost {
				cache.MaxLongExecutionCost = cache.Apply[len(cache.Apply)-1]
			}
			if len(reuseResult) > 0 {
				if cache.Reuse[len(cache.Reuse)-1] > cache.MaxLongExecutionReuseCost {
					cache.MaxLongExecutionReuseCost = cache.Reuse[len(cache.Reuse)-1]
				}
			}
			context := []interface{}{
				"number", block.Number(), "hash", block.Hash(), "index", i, "tx", tx.Hash().Hex(),
				//"now time", fmt.Sprintf("%.3fs", float64(runtime.GetNanoTime()-runtime.GetRunTimeInitTime())/1e9),
				"apply cost", common.PrettyDuration(cache.Apply[len(cache.Apply)-1]),
				"cum cost", common.PrettyDuration(cache.LongExecutionCost),
				"max cost", common.PrettyDuration(cache.MaxLongExecutionCost),
			}
			if len(reuseResult) > 0 {
				context = append(context,
					"getRW cost", common.PrettyDuration(cache.GetRW[len(cache.GetRW)-1]),
					"setDB cost", common.PrettyDuration(cache.SetDB[len(cache.SetDB)-1]),
					"reuse cost", common.PrettyDuration(cache.Reuse[len(cache.Reuse)-1]),
					"max reuse cost", common.PrettyDuration(cache.MaxLongExecutionReuseCost),
				)

				var status = reuseResult[len(reuseResult)-1]
				var statusStr = status.BaseStatus.String()
				if status.BaseStatus == cmptypes.Hit {
					statusStr += "-" + status.HitType.String()
					if status.HitType == cmptypes.MixHit {
						statusStr += "-" + status.MixHitStatus.MixHitType.String()
					}
					if status.HitType == cmptypes.TraceHit {
						statusStr += "-" + status.TraceHitStatus.String()
					}
				}
				context = append(context, "status", statusStr)
			}
			log.Info("Long execution", context...)
		}
	}
	if cfg.MSRAVMSettings.CmpReuse && cfg.MSRAVMSettings.ParallelizeReuse {
		statedb.MergeDelta()
	}
	t0 := time.Now()

	if cfg.MSRAVMSettings.CacheRecord && !cfg.MSRAVMSettings.Silent {
		p.bc.MSRACache.CachePrint(block, reuseResult)
	}

	if cfg.MSRAVMSettings.GroundRecord && !cfg.MSRAVMSettings.Silent {
		p.bc.MSRACache.GroundPrint(block)
	}

	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	p.engine.Finalize(p.bc, header, statedb, block.Transactions(), block.Uncles())
	cache.WaitUpdateRoot, cache.UpdateObj, cache.HashTrie = statedb.DumpFinalizeDetail()
	cache.Finalize = time.Since(t0)
	cache.ReuseResult = reuseResult
	return receipts, allLogs, *usedGas, nil
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, cfg vm.Config) (*types.Receipt, error) {
	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, err
	}
	// Create a new context to be used in the EVM environment
	context := NewEVMContext(msg, header, bc, author)
	// Create a new environment which holds all relevant information
	// about the transaction and calling mechanisms.
	vmenv := vm.NewEVM(context, statedb, config, cfg)
	// Apply the transaction to the current state (included in the env)
	_, gas, failed, err := ApplyMessage(vmenv, msg, gp)
	if err != nil {
		return nil, err
	}
	// Update the state with pending changes
	var root []byte
	if config.IsByzantium(header.Number) {
		statedb.Finalise(true)
	} else {
		root = statedb.IntermediateRoot(config.IsEIP158(header.Number)).Bytes()
	}
	*usedGas += gas

	// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx
	// based on the eip phase, we're passing whether the root touch-delete accounts.
	receipt := types.NewReceipt(root, failed, *usedGas)
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = gas
	// if the transaction created a contract, store the creation address in the receipt.
	if msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(vmenv.Context.Origin, tx.Nonce())
	}
	// Set the receipt logs and create a bloom for filtering
	receipt.Logs = statedb.GetLogs(tx.Hash())
	if statedb.BloomProcessor != nil {
		statedb.BloomProcessor.CreateBloomForTransaction(receipt)
	} else {
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	}

	receipt.BlockHash = statedb.BlockHash()
	receipt.BlockNumber = header.Number
	receipt.TransactionIndex = uint(statedb.TxIndex())

	return receipt, err
}

func ApplyTransactionPerfTest(config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, statedb *state.StateDB,
	header *types.Header, tx *types.Transaction, usedGas *uint64, cfg vm.Config) (*types.Receipt, error, time.Duration, time.Duration) {
	applyStart := time.Now()
	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, err, 0, 0
	}
	// Create a new context to be used in the EVM environment
	context := NewEVMContext(msg, header, bc, author)
	// Create a new environment which holds all relevant information
	// about the transaction and calling mechanisms.
	vmenv := vm.NewEVM(context, statedb, config, cfg)
	// Apply the transaction to the current state (included in the env)

	_, gas, failed, err := ApplyMessage(vmenv, msg, gp)
	applyDuration := time.Since(applyStart)
	if err != nil {
		return nil, err, applyDuration, 0
	}
	finaliseStart := time.Now()
	// Update the state with pending changes
	var root []byte
	if config.IsByzantium(header.Number) {
		statedb.Finalise(true)
	} else {
		root = statedb.IntermediateRoot(config.IsEIP158(header.Number)).Bytes()
	}
	*usedGas += gas

	// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx
	// based on the eip phase, we're passing whether the root touch-delete accounts.
	receipt := types.NewReceipt(root, failed, *usedGas)
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = gas
	// if the transaction created a contract, store the creation address in the receipt.
	if msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(vmenv.Context.Origin, tx.Nonce())
	}
	// Set the receipt logs and create a bloom for filtering
	receipt.Logs = statedb.GetLogs(tx.Hash())
	if statedb.BloomProcessor != nil {
		statedb.BloomProcessor.CreateBloomForTransaction(receipt)
	} else {
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	}

	receipt.BlockHash = statedb.BlockHash()
	receipt.BlockNumber = header.Number
	receipt.TransactionIndex = uint(statedb.TxIndex())
	finaliseDuration := time.Since(finaliseStart)

	return receipt, err, applyDuration, finaliseDuration
}
