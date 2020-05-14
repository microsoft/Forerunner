package cmpreuse

import (
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/params"
	"github.com/hashicorp/golang-lru"
	"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

var AlwaysFalse = func() bool { return false }

// SetReadDep set the read dep info for a tx in a given round
func IsNoDep(raddresses []*common.Address, statedb *state.StateDB) bool {
	for _, addr := range raddresses {

		if statedb.IsInPending(*addr) {
			return false
		}

	}
	return true
}

func (reuse *Cmpreuse) setAllResult(reuseStatus *cmptypes.ReuseStatus, curRoundID uint64, tx *types.Transaction, receipt *types.Receipt,
	sender common.Address, rwrecord *cache.RWRecord, wobjects state.ObjectMap, accChanges cmptypes.TxResIDMap, readDep []*cmptypes.AddrLocValue,
	preBlockHash common.Hash, trace *STrace, basicPreplay bool) {
	if receipt == nil || rwrecord == nil {
		panic("cmpreuse: receipt or rwrecord should not be nil")
	}

	txHash := tx.Hash()
	txPreplay := reuse.MSRACache.GetTxPreplay(txHash)
	if txPreplay == nil {
		txPreplay = reuse.addNewTx(tx)
	}
	txPreplay.Mu.Lock()
	defer txPreplay.Mu.Unlock()
	start := time.Now()

	// Generally, there are three scenarios :  1. NoHit  2. DepHit  3. DetailHit (Hit but not DepHit)
	// To set results more effectively, we should
	// Generate new round for all scenarios //* Generate new round for scenario 1 and 3 (which have new read deps, that means this preplay result does not exist before )
	// * Insert to readDep tree for scenario 1 and 3
	// * Insert rwrecord tree for scenario 1
	// * update the blocknumber of rwrecord for scenario 2 and 3 (Hit)
	if reuseStatus.BaseStatus != cmptypes.Hit ||
		!(reuseStatus.HitType == cmptypes.MixHit && reuseStatus.MixHitStatus.MixHitType == cmptypes.AllDepHit) {
		round, ok := reuse.MSRACache.SetMainResult(curRoundID, receipt, rwrecord, wobjects, accChanges, readDep, preBlockHash, txPreplay)
		if ok {
			round.Trace = trace
		}

		curBlockNumber := receipt.BlockNumber.Uint64()

		reuse.MSRACache.SetGasUsedCache(tx, receipt, sender)
		if !ok {
			return
		}
		reuse.setMixTree(txPreplay, round, curBlockNumber, &preBlockHash)

		if reuseStatus.BaseStatus != cmptypes.Hit {
			reuse.setRWRecordTrie(txPreplay, round, curBlockNumber)
			reuse.setDeltaTree(tx, txPreplay, round, curBlockNumber)
			if false && trace != nil {
				traceTrieStart := time.Now()
				reuse.setTraceTrie(tx, txPreplay, round, trace)
				cost := time.Since(traceTrieStart)
				if cost > 14*time.Second {
					log.Warn("Slow setTraceTrie", "txHash", txHash.Hex(), "Seconds", cost,
						"traceLen", len(trace.Stats), "traceRLCount", len(trace.RLNodeSet), "traceJSPCount", len(trace.JSPSet))
				}
			}
		}

		if time.Since(start) > 30*time.Second {
			log.Warn("Slow setMainResult", "txHash", txHash.Hex(), "readSize", len(round.RWrecord.ReadDetail.ReadDetailSeq), "writeSize", len(round.RWrecord.WState))
		}
	} else {
		if !basicPreplay {
			reuse.MSRACache.SetMainResult(curRoundID, receipt, rwrecord, wobjects, accChanges, readDep, preBlockHash, txPreplay)
		}
	}
}

func (reuse *Cmpreuse) setRWRecordTrie(txPreplay *cache.TxPreplay, round *cache.PreplayResult, curBlockNumber uint64) {
	trie := txPreplay.PreplayResults.RWRecordTrie
	InsertRecord(trie, round, curBlockNumber)
}

// Deprecated
func (reuse *Cmpreuse) setReadDepTree(txPreplay *cache.TxPreplay, round *cache.PreplayResult, curBlockNumber uint64, preBlockHash *common.Hash) bool {
	return InsertAccDep(txPreplay.PreplayResults.ReadDepTree, round, curBlockNumber, preBlockHash)
}

func (reuse *Cmpreuse) setMixTree(txPreplay *cache.TxPreplay, round *cache.PreplayResult, curBlockNumber uint64, preBlockHash *common.Hash) {
	InsertMixTree(txPreplay.PreplayResults.MixTree, round, curBlockNumber, preBlockHash)
}

func (reuse *Cmpreuse) setDeltaTree(tx *types.Transaction, txPreplay *cache.TxPreplay, round *cache.PreplayResult, curBlockNumber uint64) {
	InsertDelta(tx, txPreplay.PreplayResults.DeltaTree, round, curBlockNumber)
}

func (reuse *Cmpreuse) setTraceTrie(tx *types.Transaction, txPreplay *cache.TxPreplay, round *cache.PreplayResult, trace *STrace) {
	var traceTrie *TraceTrie
	if txPreplay.PreplayResults.TraceTrie != nil {
		traceTrie = txPreplay.PreplayResults.TraceTrie.(*TraceTrie)
	} else {
		traceTrie = NewTraceTrie(tx)
		txPreplay.PreplayResults.TraceTrie = traceTrie
	}
	traceTrie.InsertTrace(trace, round)
}

func (reuse *Cmpreuse) commitGround(tx *types.Transaction, receipt *types.Receipt, rwrecord *cache.RWRecord, groundFlag uint64) {
	nowTime := time.Now()
	groundResult := &cache.SimpleResult{
		TxHash:          tx.Hash(),
		Tx:              tx,
		Receipt:         receipt,
		RWrecord:        rwrecord,
		RWTimeStamp:     uint64(nowTime.Unix()),
		RWTimeStampNano: uint64(nowTime.UnixNano()),
	}
	switch groundFlag {
	case 1:
		reuse.MSRACache.CommitGround(groundResult)
	case 2:
		reuse.MSRACache.PrintGround(groundResult)
	}
}

func (reuse *Cmpreuse) addNewTx(tx *types.Transaction) *cache.TxPreplay {
	txPreplay := cache.NewTxPreplay(tx)
	reuse.MSRACache.AddTxPreplay(txPreplay)
	return txPreplay
}

var txTraceTries, _ = lru.New(3000) // make(map[common.Hash] *TraceTrie)
var traceMutex sync.Mutex

// PreplayTransaction attempts to preplay a transaction to the given state
// database and uses the input parameters for its environment. It returns
// the receipt for the transaction and an error if the transaction failed,
// indicating the block was invalid.
// Cmpreuse.PreplayTransaction used for these scenarios
// 		1. basic preplay,
// 		2. record the ground truth rw state when interChain (blockchain.processor.Process)
//		3. worker package the next block
//		4. preplay for grouping transactions
//		5. preplay for reporting miss
// external args (comparing with core.ApplyTransaction)
// 		`roundID`:
//			used for scenario 1. 0 for other scenarios
//		`blockPre`:
//			used for scenario 2. help `getValidRW` skip rounds which are later then `blockPre`
//		`groundFlag`:
// 			0 for no ground feature, scenario 1;
// 			1 for recording the ground truth rw states, scenario 2;
// 			2 for just printing the rw states, scenario 3.
//		`basicPreplay`:
//			true for basic preplay, scenario 1, 2 or 3;
//			false for preplay for grouping transactions or reporting miss, scenario 4 or 5;
func (reuse *Cmpreuse) PreplayTransaction(config *params.ChainConfig, bc core.ChainContext, author *common.Address,
	gp *core.GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
	cfg vm.Config, roundID uint64, blockPre *cache.BlockPre, groundFlag uint64, basicPreplay bool) (*types.Receipt, error) {

	if statedb.IsShared() || !statedb.IsRWMode() {
		panic("PreplayTransaction can't be used for process and statedb must be RW mode and not be shared.")
	}

	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, err
	}

	var (
		receipt     *types.Receipt
		rwrecord    *cache.RWRecord
		wobjects    state.ObjectMap
		gas         uint64
		failed      bool
		reuseStatus *cmptypes.ReuseStatus
		reuseRound  *cache.PreplayResult
		readDeps    []*cmptypes.AddrLocValue
		trace       *STrace
	)

	reuseStatus, reuseRound, _, _ = reuse.reuseTransaction(bc, author, gp, statedb, header, nil, nil, tx, blockPre, AlwaysFalse, false, &cfg)
	if reuseStatus.BaseStatus == cmptypes.Hit {
		cmptypes.MyAssert(reuseStatus.HitType != cmptypes.TraceHit)

		if reuseStatus.HitType == cmptypes.MixHit && reuseStatus.MixHitStatus.MixHitType == cmptypes.AllDepHit {
			statedb.SetEnableWObject(false)
		}
		receipt = reuse.finalise(config, statedb, header, tx, usedGas, reuseRound.Receipt.GasUsed, reuseRound.RWrecord.Failed, msg)
		if reuseStatus.HitType == cmptypes.MixHit && reuseStatus.MixHitStatus.MixHitType == cmptypes.AllDepHit {
			statedb.SetEnableWObject(true)
		}
		rwrecord = reuseRound.RWrecord
		trace = reuseRound.Trace.(*STrace)

	} else {
		gas, failed, err = reuse.realApplyTransaction(config, bc, author, gp, statedb, header, &cfg, core.NewController(), msg, tx)

		defer statedb.RWRecorder().RWClear() // Write set got
		if err == nil {
			receipt = reuse.finalise(config, statedb, header, tx, usedGas, gas, failed, msg)
			// Record RWSet
			rstate, rchain, wstate, readDetail := statedb.RWRecorder().RWDump()
			readDeps = readDetail.ReadAddressAndBlockSeq
			rwrecord = cache.NewRWRecord(rstate, rchain, wstate, readDetail, failed)
			wobjects = statedb.RWRecorder().WObjectDump()

			// test reuse tracer
			if statedb.ReuseTracer != nil {
				rt := statedb.ReuseTracer.(*ReuseTracer)
				statedb.ReuseTracer = nil
				stats := rt.Statements

				defer func() {
					if e := recover(); e != nil {
						txHex := tx.Hash().Hex()
						fmt.Printf("Tx %s Tracer Error\n  %s :%s", txHex, e, debug.Stack())
						rt.DumpDebugBuffer(fmt.Sprintf("/tmp/errTxTrace%v.txt", txHex))
						//rt.DumpDebugBuffer(fmt.Sprintf("/tmp/errTxTrace.txt"))
						panic(e)
					}
				}()

				debugOut := func(fmtStr string, args ...interface{}) {
					//fmt.Printf(fmtStr, args...)
					rt.DebugOut(fmtStr, args...)
				}

				if !rt.DebugFlag {
					debugOut = nil
				}

				if debugOut != nil {
					tmsg := fmt.Sprintf("Tx%d: %s Unimplemented %v, Completed %v, External %v, Size %v", ReuseTracerTracedTxCount,
						tx.Hash().Hex(), rt.EncounterUnimplementedCode, rt.IsCompleteTrace, rt.IsExternalTransfer, len(stats))
					debugOut(tmsg + "\n")
				}

				if !rt.IsCompleteTrace || rt.EncounterUnimplementedCode {
					panic("Tracer Error:InComplete Trace or Unimplemeted Code reached!")
				}

				trace = NewSTrace(stats, debugOut)

				//writeDoubleOut := func(fmtStr string, args ...interface{}) {
				//	//fmt.Printf(fmtStr, args...)
				//	debugOut(fmtStr, args...)
				//}
				CrossCheck(trace.CrosscheckStats, readDetail, wstate, receipt.Logs, debugOut)

				registerMapping, highestIndex := trace.RAlloc, trace.RegisterFileSize
				loadCount, readCount, storeCount, logCount := GetLRSL(trace.Stats)
				if debugOut != nil {
					summary := fmt.Sprintf("Crosscheck %v Passed for tx %v, with %v non-const variables which requires %v registers. %v loads, %v reads, %v stores, %v logs",
						atomic.LoadUint64(&ReuseTracerTracedTxCount), tx.Hash().Hex(), len(registerMapping), highestIndex, loadCount, readCount, storeCount, logCount)
					debugOut(summary + "\n")
				}

				if rt.IsExternalTransfer {
					trace = nil // do not process external transfer
				}
				//log.Info(summary)
				//_trie, ok := txTraceTries.Get(tx.Hash())
				//if !ok {
				//	_trie = NewTraceTrie(tx, nil)
				//	txTraceTries.Add(tx.Hash(), _trie)
				//}
				//trie := _trie.(*TraceTrie)
				//
				//trie.WriteOut = debugOut
				//trie.InsertTrace(trace)

				// sample tx trace for monitoring purpose
			}
			atomic.AddUint64(&ReuseTracerTracedTxCount, 1)

		} else {
			return nil, err
		}
	}

	if groundFlag == 0 {
		curTxRes := cmptypes.NewTxResID(tx.Hash(), roundID)
		var accChanges cmptypes.TxResIDMap

		if reuseStatus.BaseStatus == cmptypes.Hit {
			if reuseStatus.HitType == cmptypes.MixHit && reuseStatus.MixHitStatus.MixHitType == cmptypes.PartialHit {
				readDeps = updateNewReadDepSeq(statedb, reuseRound.ReadDepSeq)
				wobjects = statedb.RWRecorder().WObjectDump()

				oldAccChanged := reuseRound.AccountChanges
				accChanges = make(cmptypes.TxResIDMap, len(oldAccChanged))
				// update which accounts changed by this tx into statedb
				//reusedTxRes := reuseRound.TxResID
				hitAddrIndex := 0
				for addr := range oldAccChanged {
					if hitAddrIndex < len(reuseStatus.MixHitStatus.DepHitAddr) && addr == reuseStatus.MixHitStatus.DepHitAddr[hitAddrIndex] {
						statedb.UpdateAccountChanged(addr, oldAccChanged[addr])
						accChanges[addr] = oldAccChanged[addr]
						hitAddrIndex++
					} else {
						statedb.UpdateAccountChanged(addr, curTxRes)
						accChanges[addr] = curTxRes
					}
				}
			} else if reuseStatus.HitType == cmptypes.MixHit && reuseStatus.MixHitStatus.MixHitType == cmptypes.AllDepHit {
				readDeps = reuseRound.ReadDepSeq
				wobjects = reuseRound.WObjects
				accChanges = reuseRound.AccountChanges
				statedb.ApplyAccountChanged(accChanges)
			} else if reuseStatus.HitType == cmptypes.TraceHit {
				readDeps = updateNewReadDepSeq(statedb, reuseRound.ReadDepSeq)
				wobjects = statedb.RWRecorder().WObjectDump()
				accChanges = make(cmptypes.TxResIDMap, len(reuseRound.AccountChanges))
				if reuseStatus.TraceTrieHitAddrs != nil {
					for addr := range reuseRound.AccountChanges {
						reusedChange, ok := reuseStatus.TraceTrieHitAddrs[addr]
						if ok{
							accChanges[addr] = reusedChange
							statedb.UpdateAccountChanged(addr, reusedChange)
						}else{
							accChanges[addr] = curTxRes
							statedb.UpdateAccountChanged(addr, curTxRes)
						}
					}
				} else {
					for addr := range reuseRound.AccountChanges {
						accChanges[addr] = curTxRes
						statedb.UpdateAccountChanged(addr, curTxRes)
					}
				}
			} else {
				// all detail hit or delta hit
				readDeps = updateNewReadDepSeq(statedb, reuseRound.ReadDepSeq)
				wobjects = statedb.RWRecorder().WObjectDump()

				accChanges = make(cmptypes.TxResIDMap, len(reuseRound.AccountChanges))
				for addr := range reuseRound.AccountChanges {
					accChanges[addr] = curTxRes
					statedb.UpdateAccountChanged(addr, curTxRes)
				}
			}
		} else {
			accChanges = make(cmptypes.TxResIDMap, len(wobjects))
			for addr := range wobjects {
				accChanges[addr] = curTxRes
				statedb.UpdateAccountChanged(addr, curTxRes)
			}
			//
			//statedb.UpdateAccountChangedByMap(wobjects, curTxRes, nil)
		}

		reuse.setAllResult(reuseStatus, roundID, tx, receipt, msg.From(), rwrecord, wobjects, accChanges, readDeps, header.ParentHash, trace, basicPreplay)

		//if trace != nil && tx.To() != nil && tx.To().Hex() == "0x2a1530C4C41db0B0b2bB646CB5Eb1A67b7158667" {
		//	fn := fmt.Sprintf("/tmp/debug%v_round%v.txt", tx.Hash().Hex(), roundID)
		//	trace.Stats[0].inputs[0].tracer.DumpDebugBuffer(fn)
		//} else

		if trace != nil && len(trace.Stats) > 2000 && rand.Intn(200) < 1 {
			traceMutex.Lock()
			trace.Stats[0].inputs[0].tracer.DumpDebugBuffer("/tmp/sampleTxTrace.txt")
			traceMutex.Unlock()
		}

		if trace != nil {
			trace.Stats[0].inputs[0].tracer.ClearDebugBuffer()
		}

	} else {
		reuse.commitGround(tx, receipt, rwrecord, groundFlag)
	}
	return receipt, err
}

func updateNewReadDepSeq(db *state.StateDB, oldReadDepSeq []*cmptypes.AddrLocValue) []*cmptypes.AddrLocValue {
	var newReadDepSeq []*cmptypes.AddrLocValue
	for _, rd := range oldReadDepSeq {
		if rd.AddLoc.Field == cmptypes.Dependence {
			newChangedBy := db.GetTxDepByAccount(rd.AddLoc.Address)
			newReadDep := &cmptypes.AddrLocValue{AddLoc: rd.AddLoc, Value: newChangedBy}
			newReadDepSeq = append(newReadDepSeq, newReadDep)
		} else {
			newReadDepSeq = append(newReadDepSeq, rd)
		}
	}
	return newReadDepSeq
}
