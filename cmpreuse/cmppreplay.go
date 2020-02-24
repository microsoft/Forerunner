package cmpreuse

import (
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/params"
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

func (reuse *Cmpreuse) setAllResult(reuseStatus *cmptypes.ReuseStatus, curRoundID uint64, tx *types.Transaction, receipt *types.Receipt, rwrecord *cache.RWRecord,
	wobjects state.ObjectMap, readDep []*cmptypes.AddrLocValue, preBlockHash common.Hash) {
	if receipt == nil || rwrecord == nil {
		panic("cmpreuse: receipt or rwrecord should not be nil")
	}

	txHash := tx.Hash()
	txPreplay := reuse.MSRACache.GetTxPreplay(txHash)
	if txPreplay == nil {
		txPreplay = reuse.addNewTx(tx)
	}
	curBlockNumber := receipt.BlockNumber.Uint64()
	txPreplay.Mu.Lock()
	defer txPreplay.Mu.Unlock()

	// Generally, there are three scenarios :  1. NoHit  2. DepHit  3. DetailHit (Hit but not DepHit)
	// To set results more effectively, we should
	// * Generate new round for scenario 1 and 3 (which have new read deps, that means this preplay result does not exist before )
	// * Insert to readDep tree for scenario 1 and 3
	// * Insert rwrecord tree for scenario 1
	// * update the blocknumber of rwrecord for scenario 2 and 3 (Hit)
	if reuseStatus.BaseStatus != cmptypes.Hit || reuseStatus.HitType != cmptypes.DepHit {
		round, ok := reuse.MSRACache.SetMainResult(curRoundID, tx.Hash(), receipt, rwrecord, wobjects, readDep, preBlockHash, txPreplay)
		if !ok {
			return
		}
		reuse.setReadDepTree(curRoundID, txPreplay, round, curBlockNumber, &preBlockHash)
		if reuseStatus.BaseStatus != cmptypes.Hit {
			reuse.setRWRecordTrie(curRoundID, txPreplay, round, curBlockNumber)
		}
	}

	if reuseStatus.BaseStatus == cmptypes.Hit {
		txPreplay.PreplayResults.RWRecordTrie.AddExistedRound(curBlockNumber)
	}
}

func (reuse *Cmpreuse) setRWRecordTrie(roundID uint64, txPreplay *cache.TxPreplay, round *cache.PreplayResult, curBlockNumber uint64) {
	trie := txPreplay.PreplayResults.RWRecordTrie
	//txPreplay.Mu.Lock()
	//defer txPreplay.Mu.Unlock()
	InsertRecord(trie, round, curBlockNumber)
}

func (reuse *Cmpreuse) setReadDepTree(roundID uint64, txPreplay *cache.TxPreplay, round *cache.PreplayResult, curBlockNumber uint64, preBlockHash *common.Hash) {
	InsertAccDep(txPreplay.PreplayResults.ReadDepTree, round, curBlockNumber, preBlockHash)
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
	//nowTime := time.Now()
	//reuse.MSRACache.CommitTxListen(&cache.TxListen{
	//	Tx:              tx,
	//	ListenTime:      uint64(nowTime.Unix()),
	//	ListenTimeNano:  uint64(nowTime.UnixNano()),
	//	ConfirmTime:     0,
	//	ConfirmBlockNum: 0,
	//})
	txPreplay := cache.NewTxPreplay(tx)
	reuse.MSRACache.CommitTxRreplay(txPreplay)
	return txPreplay
}

// PreplayTransaction attempts to preplay a transaction to the given state
// database and uses the input parameters for its environment. It returns
// the receipt for the transaction and an error if the transaction failed,
// indicating the block was invalid.
// Cmpreuse.PreplayTransaction used for these scenarios
// 		1. preplay,
// 		2. record the ground truth rw state when interChain (blockchain.processor.Process)
//		3. worker package the next block
// external args (comparing with core.ApplyTransaction)
// 		`roundID`:
//			used for scenario 1. 0 for other scenarios
//		`blockPre`:
//			used for scenario 2. help `getValidRW` skip rounds which are later then `blockPre`
//		`groundFlag`:
// 			0 for no ground feature, scenario 1;
// 			1 for recording the ground truth rw states, scenario 2;
// 			2 for just printing the rw states, scenario 3.
func (reuse *Cmpreuse) PreplayTransaction(config *params.ChainConfig, bc core.ChainContext, author *common.Address,
	gp *core.GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
	cfg vm.Config, roundID uint64, blockPre *cache.BlockPre, groundFlag uint64) (*types.Receipt, error) {

	if statedb.IsShared() || !statedb.IsRWMode() {
		panic("PreplayTransaction can't be used for process and statedb must be RW mode and not be shared.")
	}

	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, err
	}

	var (
		receipt      *types.Receipt
		rwrecord     *cache.RWRecord
		wobjects     state.ObjectMap
		gas          uint64
		failed       bool
		txResRoundID uint64
		reuseStatus  *cmptypes.ReuseStatus
		reuseRound   *cache.PreplayResult
		readDeps     []*cmptypes.AddrLocValue
	)
	reuseStatus, reuseRound, _, _, _ = reuse.reuseTransaction(bc, author, gp, statedb, header, tx, blockPre, AlwaysFalse, false)
	if reuseStatus.BaseStatus == cmptypes.Hit {

		receipt = reuse.finalise(config, statedb, header, tx, usedGas, reuseRound.Receipt.GasUsed, reuseRound.RWrecord.Failed, msg)
		rwrecord = reuseRound.RWrecord
		wobjects = statedb.RWRecorder().WObjectDump()

		if reuseStatus.HitType == cmptypes.DepHit {
			txResRoundID = reuseRound.RoundID

			readDeps = reuseRound.ReadDeps
			// FIXME: check
			//readDeps = reuseRound.ReadDeps.Copy()
			//readDeps = updateNewReadDep(statedb, reuseRound.ReadDeps)
		} else {
			txResRoundID = roundID
			readDeps = updateNewReadDep(statedb, reuseRound.ReadDeps)
		}

		//if len(rwrecord.ReadDetail.ReadAddress) > 2 {
		//	preDeps, _ := json.Marshal(reuseRound.ReadDeps)
		//	curDeps, _ := json.Marshal(readDeps)
		//	log.Info("preplay hit", "hittype", reuseStatus.HitType, "txhash", tx.Hash().Hex(), "curRoundId", roundID, "preRoundId", reuseRound.RoundID, "matchedRoundDeps", string(preDeps), "curDeps", string(curDeps))

		//for _, rd := range rwrecord.ReadDetail.ReadAddressAndBlockSeq {
		//	if rd.AddLoc.Field == cmptypes.Dependence && rd.Value != nil {
		//							break
		//	}
		//}
		//}

	} else {
		gas, failed, err = reuse.realApplyTransaction(config, bc, author, gp, statedb, header, cfg, core.NewController(), msg)

		defer statedb.RWRecorder().RWClear() // Write set got
		if err == nil {
			receipt = reuse.finalise(config, statedb, header, tx, usedGas, gas, failed, msg)
			// Record RWSet
			rstate, rchain, wstate, readDetail := statedb.RWRecorder().RWDump()
			readDeps = readDetail.ReadAddressAndBlockSeq
			rwrecord = cache.NewRWRecord(rstate, rchain, wstate, readDetail, failed)
			wobjects = statedb.RWRecorder().WObjectDump()
			txResRoundID = roundID

			//curDeps, _ := json.Marshal(readDeps)
			//log.Info("preplay unmatch ", "txhash", tx.Hash().Hex(), "curRoundId", roundID, "curDeps", string(curDeps))

		} else {
			//log.Info("preplay tx err", "txhash", tx.Hash().Hex(), "curRoundId", roundID)
			return nil, err
		}
	}

	if groundFlag == 0 {
		reuse.setAllResult(reuseStatus, roundID, tx, receipt, rwrecord, wobjects, readDeps, header.ParentHash)

		// update which accounts changed by this tx into statedb
		statedb.UpdateAccountChangedWithMap(wobjects, tx.Hash(), txResRoundID, nil)
	} else {
		reuse.commitGround(tx, receipt, rwrecord, groundFlag)
	}
	return receipt, err
}

func updateNewReadDep(db *state.StateDB, oldReadDep []*cmptypes.AddrLocValue) []*cmptypes.AddrLocValue {
	var newReadDepSeq []*cmptypes.AddrLocValue
	for _, rd := range oldReadDep {
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
