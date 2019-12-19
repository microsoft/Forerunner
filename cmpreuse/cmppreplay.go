package cmpreuse

import (
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

func (reuse *Cmpreuse) setMainResult(roundID uint64, tx *types.Transaction, receipt *types.Receipt, rwrecord *cache.RWRecord) {
	if ok := reuse.MSRACache.SetMainResult(roundID, tx.Hash(), receipt, rwrecord); ok {
		return
	}
	nowTime := time.Now()
	reuse.MSRACache.CommitTxListen(&cache.TxListen{
		Tx: tx,
		//From:            ,
		ListenTime:      uint64(nowTime.Unix()),
		ListenTimeNano:  uint64(nowTime.UnixNano()),
		ConfirmTime:     0,
		ConfirmBlockNum: 0,
	})
	reuse.MSRACache.CommitTxRreplay(cache.NewTxPreplay(tx))
	reuse.MSRACache.SetMainResult(roundID, tx.Hash(), receipt, rwrecord)
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
		receipt  *types.Receipt
		rwrecord *cache.RWRecord
		gas      uint64
		failed   bool
	)

	if reuseStatus, round, _, _, _ := reuse.reuseTransaction(bc, author, gp, statedb, header, tx, blockPre, AlwaysFalse); reuseStatus == hit {
		receipt = reuse.finalise(config, statedb, header, tx, usedGas, round.Receipt.GasUsed, round.RWrecord.Failed, msg)
		rwrecord = round.RWrecord
	} else {
		gas, failed, err = reuse.realApplyTransaction(config, bc, author, gp, statedb, header, cfg, core.NewController(), msg)
		if err == nil {
			receipt = reuse.finalise(config, statedb, header, tx, usedGas, gas, failed, msg)
		}

		// Record RWSet
		rstate, rchain, wstate := statedb.RWRecorder().RWDump()
		rwrecord = cache.NewRWRecord(rstate, rchain, wstate, failed)
		statedb.RWRecorder().RWClear() // Write set got
	}
	if groundFlag == 0 {
		reuse.setMainResult(roundID, tx, receipt, rwrecord)
	} else {
		reuse.commitGround(tx, receipt, rwrecord, groundFlag)
	}
	return receipt, err
}
