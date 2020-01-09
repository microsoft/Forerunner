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
func IsNoDep(raddresses []common.Address, statedb *state.StateDB) bool {
	isNoDep := true
	for _, addr := range raddresses {
		if statedb.IsInPending(addr) {
			isNoDep = false
			break
		}
	}
	return isNoDep
}

func (reuse *Cmpreuse) setAllResult(roundID uint64, tx *types.Transaction, receipt *types.Receipt, rwrecord *cache.RWRecord,
	wobjects state.ObjectMap, preBlockHash common.Hash, isNoDep bool) {
	if receipt == nil || rwrecord == nil {
		panic("cmpreuse: receipt or rwrecord should not be nil")
	}

	txHash := tx.Hash()
	txPreplay := reuse.MSRACache.GetTxPreplay(txHash)
	if txPreplay == nil {
		txPreplay = reuse.addNewTx(tx)
	}
	reuse.MSRACache.SetMainResult(roundID, tx.Hash(), receipt, rwrecord, wobjects, preBlockHash, txPreplay)
	if !isNoDep {
		return // record this condition when isNoDep is true only
	}
	reuse.MSRACache.SetReadDep(roundID, txHash, txPreplay, rwrecord, preBlockHash, isNoDep)

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
		isNoDep  = true
		receipt  *types.Receipt
		rwrecord *cache.RWRecord
		wobjects state.ObjectMap
		gas      uint64
		failed   bool
	)
	if reuseStatus, round, _, _, _ := reuse.reuseTransaction(bc, author, gp, statedb, header, tx, blockPre, AlwaysFalse);
		reuseStatus == cmptypes.Hit || reuseStatus == cmptypes.FastHit {

		if groundFlag == 0 {
			isNoDep = IsNoDep(round.RWrecord.RAddresses, statedb)
		}
		receipt = reuse.finalise(config, statedb, header, tx, usedGas, round.Receipt.GasUsed, round.RWrecord.Failed, msg)
		rwrecord = round.RWrecord
		wobjects = statedb.RWRecorder().WObjectDump()
	} else {
		gas, failed, err = reuse.realApplyTransaction(config, bc, author, gp, statedb, header, cfg, core.NewController(), msg)
		if groundFlag == 0 {
			_, _, _, rAddresses := statedb.RWRecorder().RWDump()
			isNoDep = IsNoDep(rAddresses, statedb)
		}

		defer statedb.RWRecorder().RWClear() // Write set got
		if err == nil {
			receipt = reuse.finalise(config, statedb, header, tx, usedGas, gas, failed, msg)
			// Record RWSet
			rstate, rchain, wstate, radd := statedb.RWRecorder().RWDump()
			rwrecord = cache.NewRWRecord(rstate, rchain, wstate, radd, failed)
			wobjects = statedb.RWRecorder().WObjectDump()
		} else {
			return nil, err
		}
	}

	if groundFlag == 0 {
		reuse.setAllResult(roundID, tx, receipt, rwrecord, wobjects, header.ParentHash, isNoDep)
	} else {
		reuse.commitGround(tx, receipt, rwrecord, groundFlag)
	}
	return receipt, err
}
