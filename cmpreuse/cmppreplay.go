package cmpreuse

import (
	"encoding/json"
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
	sender common.Address, rwrecord *cache.RWRecord, wobjects state.ObjectMap, wobjectCopy, wobjectNotCopy uint64,
	accChanges cmptypes.TxResIDMap, readDep []*cmptypes.AddrLocValue, preBlockHash common.Hash, trace *STrace, enablePause bool,
	cfg *vm.MSRAVMConfig) {

	if receipt == nil || rwrecord == nil {
		panic("cmpreuse: receipt or rwrecord should not be nil")
	}

	txHash := tx.Hash()
	txPreplay := reuse.MSRACache.PeekTxPreplayInNonProcess(txHash)

	//setDelta := !cfg.NoTrace && !cfg.SingleFuture
	setDelta := false

	if txPreplay == nil {
		txPreplay = reuse.addNewTx(tx, rwrecord, setDelta)
	}

	start := time.Now()
	round, ok := reuse.MSRACache.SetMainResult(curRoundID, receipt, rwrecord, wobjects, wobjectCopy, wobjectNotCopy, accChanges, readDep, preBlockHash, txPreplay)
	reuse.MSRACache.AddAccountSnapByReadDetail(preBlockHash, readDep)

	if ok {
		round.Trace = trace
	}

	notHit := reuseStatus.BaseStatus != cmptypes.Hit
	if notHit {
		reuse.MSRACache.SetGasUsedCache(tx, receipt, sender)
		if !ok {
			return
		}
	}

	if enablePause {
		reuse.MSRACache.PauseForProcess()
	}

	traceNeeded := !(cfg.NoTrace || cfg.SingleFuture)

	var err error
	//isTraceHit := reuseStatus.BaseStatus == cmptypes.Hit && reuseStatus.HitType == cmptypes.TraceHit
	mixTreeNeeded := (!traceNeeded || cfg.AddFastPath) && !cfg.NoOverMatching
	newMixRecord := notHit || !(reuseStatus.HitType == cmptypes.MixHit && reuseStatus.MixStatus.MixHitType == cmptypes.AllDepHit)
	//newMixRecord := notHit || (!isTraceHit && !(reuseStatus.HitType == cmptypes.MixHit && reuseStatus.MixStatus.MixHitType == cmptypes.AllDepHit))
	if mixTreeNeeded && newMixRecord {
		txPreplay.PreplayResults.MixTreeMu.Lock()
		if trace == nil && txPreplay.PreplayResults.IsTraceBaseMixTree {
			// do not insert non-trace based rwrecord into tracebasedmixtree
		} else {
			if trace != nil && !txPreplay.PreplayResults.IsTraceBaseMixTree {
				if txPreplay.PreplayResults.MixTree.RoundCount != 0 {
					txPreplay.PreplayResults.MixTree = cmptypes.NewPreplayResTrie()
				}
				txPreplay.PreplayResults.IsTraceBaseMixTree = true
			}
			if !cfg.SingleFuture || txPreplay.PreplayResults.MixTree.RoundCount == 0 {
				//txPreplay.PreplayResults.MixTree = cmptypes.NewPreplayResTrie()
				if setDelta {
					curIsExternalTransfer := cache.IsExternalTransfer(rwrecord.ReadDetail, tx)
					if curIsExternalTransfer != txPreplay.PreplayResults.IsExternalTransfer {
						log.Warn("IsExternalTransfer changes", "tx", txPreplay.TxHash.Hex(),
							"old", txPreplay.PreplayResults.IsExternalTransfer, "newValue", curIsExternalTransfer)
						txPreplay.PreplayResults.MixTree = cmptypes.NewPreplayResTrie()
					}
					txPreplay.PreplayResults.IsExternalTransfer = curIsExternalTransfer
				}

				err = reuse.setMixTree(tx, txPreplay, round, setDelta)
			}
		}
		txPreplay.PreplayResults.MixTreeMu.Unlock()

		if err != nil {
			roundjs, _ := json.Marshal(round)
			log.Error("setMixTree error", "round", string(roundjs))
			panic(err.Error())
		}
	}

	rwTreeNeededBeforeTrace := (!traceNeeded || cfg.AddFastPath) && cfg.NoOverMatching
	rwTreeNeededAfterTrace := traceNeeded && !cfg.AddFastPath
	rwTreeNeeded := rwTreeNeededBeforeTrace || rwTreeNeededAfterTrace
	//newRWRecord := notHit || (!isTraceHit && rwTreeNeededBeforeTrace && (reuseStatus.HitType != cmptypes.TrieHit))
	newRWRecord := notHit || (rwTreeNeededBeforeTrace && (reuseStatus.HitType != cmptypes.TrieHit))

	if rwTreeNeeded && newRWRecord {

		curBlockNumber := receipt.BlockNumber.Uint64()

		txPreplay.PreplayResults.RWRecordTrieMu.Lock()
		if trace == nil && txPreplay.PreplayResults.IsTraceBasedRWTrie {
			// do not insert no-trace based RWRecord after switched to TraceBasedRWTrie
		} else {
			if trace != nil && !txPreplay.PreplayResults.IsTraceBasedRWTrie {
				// switch to trace based RWRecordTree
				if txPreplay.PreplayResults.RWRecordTrie.RoundCount != 0 {
					txPreplay.PreplayResults.RWRecordTrie = cmptypes.NewPreplayResTrie()
				}
				txPreplay.PreplayResults.IsTraceBasedRWTrie = true
			}
			if !cfg.SingleFuture || txPreplay.PreplayResults.RWRecordTrie.RoundCount == 0 {
				//txPreplay.PreplayResults.RWRecordTrie = cmptypes.NewPreplayResTrie()
				err = reuse.setRWRecordTrie(txPreplay, round, curBlockNumber)
			}
		}
		txPreplay.PreplayResults.RWRecordTrieMu.Unlock()

		if err != nil {
			roundjs, _ := json.Marshal(round)
			log.Error("setRWRecordTrie error", "round", string(roundjs))
			panic(err.Error())

		}
	}

	if enablePause {
		reuse.MSRACache.PauseForProcess()
	}

	newTraceRecord := false
	if notHit {
		newTraceRecord = true
	} else {
		if reuseStatus.HitType == cmptypes.TraceHit {
			if cfg.NoOverMatching {
				if reuseStatus.TraceStatus.TraceHitType == cmptypes.OpHit {
					newTraceRecord = true
				}
			} else {
				if !(reuseStatus.TraceStatus.TraceHitType == cmptypes.AllDepHit) {
					newTraceRecord = true
				}
			}
		}
	}

	if traceNeeded && newTraceRecord && trace != nil {
		traceTrieStart := time.Now()

		txPreplay.PreplayResults.TraceTrieMu.Lock()
		reuse.setTraceTrie(tx, txPreplay, round, trace, reuseStatus, cfg)
		txPreplay.PreplayResults.TraceTrieMu.Unlock()

		cost := time.Since(traceTrieStart)
		if cost > 14*time.Second {
			log.Warn("Slow setTraceTrie", "txHash", txHash.Hex(), "Seconds", cost,
				"traceLen", len(trace.Stats), "traceRLCount", len(trace.RLNodeSet), "traceJSPCount", len(trace.JSPSet))
		}
	}

	if time.Since(start) > 30*time.Second {
		log.Warn("Slow setMainResult", "txHash", txHash.Hex(), "readSize", len(round.RWrecord.ReadDetail.ReadDetailSeq), "writeSize", len(round.RWrecord.WState))
	}

	//}

	//if !cfg.NoTrace && !cfg.SingleFuture && notHitOrNotTraceHit {
	//	if txPreplay.PreplayResults.IsExternalTransfer {
	//		txPreplay.PreplayResults.DeltaTreeMu.Lock()
	//		err = reuse.setDeltaTree(tx, txPreplay, round, curBlockNumber)
	//		txPreplay.PreplayResults.DeltaTreeMu.Unlock()
	//
	//		if err != nil {
	//			roundjs, _ := json.Marshal(round)
	//			log.Error("setDeltaTree error", "round", string(roundjs))
	//			panic(err.Error())
	//		}
	//	}
	//}

}

func (reuse *Cmpreuse) setRWRecordTrie(txPreplay *cache.TxPreplay, round *cache.PreplayResult, curBlockNumber uint64) error {
	trie := txPreplay.PreplayResults.RWRecordTrie
	return InsertRecord(trie, round, curBlockNumber)
}

func (reuse *Cmpreuse) setMixTreeWithOnlyDependence(txPreplay *cache.TxPreplay, round *cache.PreplayResult) error {
	return InsertAccDep(txPreplay.PreplayResults.MixTree, round)
}

func (reuse *Cmpreuse) setMixTree(tx *types.Transaction, txPreplay *cache.TxPreplay, round *cache.PreplayResult, setDelta bool) error {
	if setDelta {
		panic("Delta should be disabled")
		setDelta = txPreplay.PreplayResults.IsExternalTransfer
	}
	return InsertMixTree(tx, txPreplay.PreplayResults.MixTree, round, setDelta)
}

func (reuse *Cmpreuse) setDeltaTree(tx *types.Transaction, txPreplay *cache.TxPreplay, round *cache.PreplayResult, curBlockNumber uint64) error {
	return InsertDelta(tx, txPreplay.PreplayResults.DeltaTree, round, curBlockNumber)
}

func (reuse *Cmpreuse) setTraceTrie(tx *types.Transaction, txPreplay *cache.TxPreplay, round *cache.PreplayResult, trace *STrace,
	reuseStatus *cmptypes.ReuseStatus, cfg *vm.MSRAVMConfig) {
	var traceTrie *TraceTrie
	if txPreplay.PreplayResults.TraceTrie != nil {
		traceTrie = txPreplay.PreplayResults.TraceTrie.(*TraceTrie)
	} else {
		traceTrie = NewTraceTrie(tx)
		txPreplay.PreplayResults.TraceTrie = traceTrie
	}
	var result *TraceTrieSearchResult
	if reuseStatus.BaseStatus == cmptypes.Hit && reuseStatus.HitType == cmptypes.TraceHit {
		result = reuseStatus.TraceStatus.SearchResult.(*TraceTrieSearchResult)
	}
	traceTrie.InsertTrace(trace, round, result, cfg, reuseStatus)
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

func (reuse *Cmpreuse) addNewTx(tx *types.Transaction, rwrecord *cache.RWRecord, setDelta bool) *cache.TxPreplay {
	txPreplay := cache.NewTxPreplay(tx)
	txPreplay.SetExternalTransferInfo(rwrecord, tx, setDelta)
	reuse.MSRACache.AddTxPreplay(txPreplay)
	return txPreplay
}

//var txTraceTries, _ = lru.New(3000) // make(map[common.Hash] *TraceTrie)
var traceMutex sync.Mutex

//func GetWObjectsFromWObjectWeakRefs(cache *cache.GlobalCache, refs cache.WObjectWeakRefMap) state.ObjectMap {
//	objMap := make(state.ObjectMap)
//	for addr, wref := range refs {
//		txPreplay := cache.PeekTxPreplay(wref.TxHash)
//		if txPreplay != nil && txPreplay.Timestamp == wref.Timestamp {
//			if txPreplay.PreplayResults != nil {
//				if objHolder, ok := txPreplay.PreplayResults.GetHolder(wref); ok {
//					cmptypes.MyAssert(objHolder.ObjID == wref.ObjectID)
//					objMap[addr] = objHolder.Obj
//				}
//			}
//		}
//	}
//	return objMap
//}

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
//		`fullPreplay`:
//			true for basic preplay, scenario 1, 2 or 3;
//			false for preplay for grouping transactions or reporting miss, scenario 4 or 5;
func (reuse *Cmpreuse) PreplayTransaction(config *params.ChainConfig, bc *core.BlockChain, author *common.Address,
	gp *core.GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
	cfg vm.Config, roundID uint64, blockPre *cache.BlockPre, groundFlag uint64, fullPreplay, enablePause bool) (*types.Receipt, error) {

	if statedb.IsShared() || !statedb.IsRWMode() {
		panic("PreplayTransaction can't be used for process and statedb must be RW mode and not be shared.")
	}

	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, err
	}

	var (
		receipt        *types.Receipt
		rwrecord       *cache.RWRecord
		wobjects       state.ObjectMap
		wobjectCopy    uint64
		wobjectNotCopy uint64
		gas            uint64
		failed         bool
		reuseStatus    *cmptypes.ReuseStatus
		reuseRound     *cache.PreplayResult
		readDeps       []*cmptypes.AddrLocValue
		trace          *STrace
	)

	getHashFunc := core.GetHashFn(header, bc)
	chainConfig := config.Rules(header.Number)
	precompiles := vm.GetPrecompiledMapping(&chainConfig)

	reuseStatus, reuseRound, _, _ = reuse.reuseTransaction(bc, author, gp, statedb, header, getHashFunc, precompiles, tx, blockPre, AlwaysFalse, false, fullPreplay, &cfg)
	if reuseStatus.BaseStatus == cmptypes.Hit {
		//cmptypes.MyAssert(reuseStatus.HitType != cmptypes.TraceHit)
		cmptypes.MyAssert(reuseStatus.HitType != cmptypes.DeltaHit)
		cmptypes.MyAssert(!(reuseStatus.HitType == cmptypes.MixHit && (reuseStatus.MixStatus.MixHitType == cmptypes.AllDeltaHit || reuseStatus.MixStatus.MixHitType == cmptypes.PartialDeltaHit)))

		isAllDepHit := (reuseStatus.HitType == cmptypes.MixHit && reuseStatus.MixStatus.MixHitType == cmptypes.AllDepHit) ||
			(reuseStatus.HitType == cmptypes.TraceHit && reuseStatus.TraceStatus.TraceHitType == cmptypes.AllDepHit)
		var saveFlag bool
		if isAllDepHit {
			saveFlag = statedb.IsAllowObjCopy()
			statedb.SetAllowObjCopy(false)
		}
		receipt = reuse.finalise(config, statedb, header, tx, usedGas, reuseRound.Receipt.GasUsed, reuseRound.RWrecord.Failed, msg)
		if isAllDepHit {
			statedb.SetAllowObjCopy(saveFlag)
		}

		if reuseStatus.HitType == cmptypes.TraceHit && reuseStatus.TraceStatus.TraceHitType == cmptypes.OpHit {
			hitTrace := reuseRound.Trace.(*STrace)
			registers := reuseStatus.TraceStatus.SearchResult.(*TraceTrieSearchResult).registers
			rwrecord = hitTrace.GenerateRWRecord(statedb, registers)
		} else {
			rwrecord = reuseRound.RWrecord
		}
		trace = reuseRound.Trace.(*STrace)
		if trace != nil && trace.DebugBuffer != nil {
			trace = trace.ShallowCopyAndExtendTrace()
			statusStr := reuseStatus.HitType.String()
			if reuseStatus.HitType == cmptypes.MixHit {
				statusStr += "-" + reuseStatus.MixStatus.MixHitType.String()
			} else if reuseStatus.HitType == cmptypes.TraceHit {
				statusStr += "-" + reuseStatus.TraceStatus.TraceHitType.String()
			}
			defer func() {
				if e := recover(); e != nil {
					txHex := tx.Hash().Hex()
					fmt.Printf("Tx %s Reuse %v Tracer Error\n  %s :%s", txHex, statusStr, e, debug.Stack())
					trace.DebugBuffer.DumpBufferToFile(fmt.Sprintf("/tmp/errTxTrace%v.txt", txHex))
					panic(e)
				}
			}()
		}

	} else {
		gas, failed, err, _ = reuse.realApplyTransaction(config, bc, author, gp, statedb, header, &cfg, AlwaysFalse, nil, msg, tx)

		defer statedb.RWRecorder().RWClear() // Write set got
		if err == nil {
			receipt = reuse.finalise(config, statedb, header, tx, usedGas, gas, failed, msg)
			// Record RWSet
			rstate, rchain, wstate, readDetail := statedb.RWRecorder().RWDump()
			readDeps = readDetail.ReadAddressAndBlockSeq
			rwrecord = cache.NewRWRecord(rstate, rchain, wstate, readDetail, failed)
			wobjects, wobjectCopy, wobjectNotCopy = statedb.RWRecorder().WObjectDump()

			// test reuse tracer
			if statedb.ReuseTracer != nil {
				rt := statedb.ReuseTracer.(*ReuseTracer)
				statedb.ReuseTracer = nil
				stats := rt.Statements
				debugBuffer := NewDebugBuffer(rt.DebugBuffer)

				defer func() {
					if e := recover(); e != nil {
						txHex := tx.Hash().Hex()
						fmt.Printf("Tx %s Tracer Error\n  %s :%s", txHex, e, debug.Stack())
						debugBuffer.DumpBufferToFile(fmt.Sprintf("/tmp/errTxTrace%v.txt", txHex))
						panic(e)
					}
				}()

				debugOut := func(fmtStr string, args ...interface{}) {
					debugBuffer.AppendLog(fmtStr, args...)
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

				trace = NewSTrace(stats, debugOut, debugBuffer)

				//writeDoubleOut := func(fmtStr string, args ...interface{}) {
				//	//fmt.Printf(fmtStr, args...)
				//	debugOut(fmtStr, args...)
				//}
				trace.ComputeDepAndJumpInfo()
				rwrecord = trace.GenerateRWRecord(statedb, nil)
				readDeps = rwrecord.ReadDetail.ReadAddressAndBlockSeq
				//readDetail, wstate = rwrecord.ReadDetail, rwrecord.WState
				//CrossCheck(trace.CrosscheckStats, readDetail, wstate, receipt.Logs, debugOut)

				registerMapping, highestIndex := trace.RAlloc, trace.RegisterFileSize
				loadCount, readCount, storeCount, logCount := GetLRSL(trace.Stats)
				if debugOut != nil {
					summary := fmt.Sprintf("Crosscheck %v Passed for tx %v, with %v non-const variables which requires %v registers. %v loads, %v reads, %v stores, %v logs",
						atomic.LoadUint64(&ReuseTracerTracedTxCount), tx.Hash().Hex(), len(registerMapping), highestIndex, loadCount, readCount, storeCount, logCount)
					debugOut(summary + "\n")
				}

				trace.TraceDuration = time.Since(rt.TraceStartTime)

				//if rt.IsExternalTransfer {
				//	trace = nil // do not process external transfer
				//}
			}
			atomic.AddUint64(&ReuseTracerTracedTxCount, 1)

		} else {
			return nil, err
		}
	}

	if groundFlag == 0 {
		curTxRes := cmptypes.NewTxResID(tx.Hash(), roundID)
		var accChanges cmptypes.TxResIDMap

		if !cfg.MSRAVMSettings.NoOverMatching {
			if reuseStatus.BaseStatus == cmptypes.Hit {
				if reuseStatus.HitType == cmptypes.MixHit &&
					(reuseStatus.MixStatus.MixHitType == cmptypes.PartialHit || reuseStatus.MixStatus.MixHitType == cmptypes.PartialDeltaHit) {
					readDeps = updateNewReadDepSeq(statedb, reuseRound.ReadDepSeq)
					wobjects, wobjectCopy, wobjectNotCopy = statedb.RWRecorder().WObjectDump()

					oldAccChanged := reuseRound.AccountChanges
					accChanges = make(cmptypes.TxResIDMap, len(oldAccChanged))
					// update which accounts changed by this tx into statedb
					//reusedTxRes := reuseRound.TxResID

					for addr, oldChange := range oldAccChanged {
						if _, ok := reuseStatus.MixStatus.DepHitAddrMap[addr]; ok {
							statedb.UpdateAccountChanged(addr, oldChange)
							accChanges[addr] = oldChange

						} else {
							statedb.UpdateAccountChanged(addr, curTxRes)
							accChanges[addr] = curTxRes
						}
					}
				} else if reuseStatus.HitType == cmptypes.MixHit && reuseStatus.MixStatus.MixHitType == cmptypes.AllDepHit {
					readDeps = reuseRound.ReadDepSeq
					wobjects = nil //GetWObjectsFromWObjectWeakRefs(reuse.MSRACache, reuseRound.WObjectWeakRefs)
					accChanges = reuseRound.AccountChanges
					statedb.ApplyAccountChanged(accChanges)
				} else if reuseStatus.HitType == cmptypes.TraceHit {
					traceHitType := reuseStatus.TraceStatus.TraceHitType
					if traceHitType == cmptypes.AllDepHit {
						readDeps = reuseRound.ReadDepSeq
						wobjects = nil
						checkReadDepSeq(statedb, readDeps)
						//accChanges = reuseRound.AccountChanges
						//statedb.ApplyAccountChanged(accChanges)
					} else {
						if traceHitType == cmptypes.PartialHit || traceHitType == cmptypes.AllDetailHit {
							readDeps = updateNewReadDepSeq(statedb, reuseRound.ReadDepSeq)
						} else {
							if traceHitType != cmptypes.OpHit {
								panic("Should be OpHit")
							}
							readDeps = rwrecord.ReadDetail.ReadAddressAndBlockSeq
						}
						wobjects, wobjectCopy, wobjectNotCopy = statedb.RWRecorder().WObjectDump()
					}
					accChanges = make(cmptypes.TxResIDMap, len(reuseRound.AccountChanges))
					if reuseStatus.TraceTrieHitAddrs != nil {
						for addr, reusedChange := range reuseStatus.TraceTrieHitAddrs {
							if reusedChange == nil {
								statedb.UpdateAccountChanged(addr, curTxRes)
								accChanges[addr] = curTxRes
							} else {
								statedb.UpdateAccountChanged(addr, reusedChange)
								accChanges[addr] = reusedChange
							}
						}
					} else {
						panic("can not find the TraceTrieHitAddrs")
					}
				} else {
					// all detail hit or delta hit
					readDeps = updateNewReadDepSeq(statedb, reuseRound.ReadDepSeq)
					wobjects, wobjectCopy, wobjectNotCopy = statedb.RWRecorder().WObjectDump()

					accChanges = make(cmptypes.TxResIDMap, len(reuseRound.AccountChanges))
					for addr := range reuseRound.AccountChanges {
						accChanges[addr] = curTxRes
						statedb.UpdateAccountChanged(addr, curTxRes)
					}
				}
			} else {
				accChanges = make(cmptypes.TxResIDMap, len(rwrecord.WState))
				for addr := range rwrecord.WState {
					accChanges[addr] = curTxRes
					statedb.UpdateAccountChanged(addr, curTxRes)
				}
				//
				//statedb.UpdateAccountChangedByMap(wobjects, curTxRes, nil)
			}
		}
		reuse.setAllResult(reuseStatus, roundID, tx, receipt, msg.From(), rwrecord, wobjects, wobjectCopy,
			wobjectNotCopy, accChanges, readDeps, header.ParentHash, trace, enablePause, &cfg.MSRAVMSettings)

		//if trace != nil && tx.To() != nil && tx.To().Hex() == "0x2a1530C4C41db0B0b2bB646CB5Eb1A67b7158667" {
		//	fn := fmt.Sprintf("/tmp/debug%v_round%v.txt", tx.Hash().Hex(), roundID)
		//	trace.Stats[0].inputs[0].tracer.DumpDebugBuffer(fn)
		//} else

		if trace != nil && trace.DebugBuffer != nil && len(trace.Stats) > 2000 && rand.Intn(200) < 1 {
			traceMutex.Lock()
			trace.DebugBuffer.DumpBufferToFile("/tmp/sampleTxTrace.txt")
			traceMutex.Unlock()
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

func checkReadDepSeq(db *state.StateDB, oldReadDepSeq []*cmptypes.AddrLocValue) {
	for _, rd := range oldReadDepSeq {
		if rd.AddLoc.Field == cmptypes.Dependence {
			newChangedBy := db.GetTxDepByAccount(rd.AddLoc.Address)
			changedBy := rd.Value.(cmptypes.AccountDepValue)
			if *changedBy.Hash() != *newChangedBy.Hash() {
				panic(fmt.Sprintf("Different dep at %v old %v new %v", rd.AddLoc.String(), changedBy.String(), newChangedBy.String()))
			}
		}
	}
}
