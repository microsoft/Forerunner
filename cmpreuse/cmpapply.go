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
)

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction and an error if the transaction failed, indicating the
// block was invalid.
// Cmpreuse.ReuseTransaction used for only one scenario:
// 		process the block with reusing preplay results, execute transaction serially
// external args (comparing with core.ApplyTransaction)
//		`blockPre`:
//			used to help `getValidRW` skip rounds which are later then `blockPre`
// For the last return uint64 value,
// 		0: error;
// 		1: cache hit;
// 		2: no cache;
// 		3: cache but not in;
//		4: cache but result not match;
//		5: abort before hit or miss;
func (reuse *Cmpreuse) ApplyTransaction(config *params.ChainConfig, bc core.ChainContext, author *common.Address,
	gp *core.GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
	cfg vm.Config, blockPre *cache.BlockPre) (*types.Receipt, error, cmptypes.ReuseStatus) {

	if statedb.IsShared() || statedb.IsRWMode() {
		panic("ApplyTransaction can only be used for process and statedb must not be shared and not be RW mode.")
	}

	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, err, cmptypes.Fail
	}

	if reuseStatus, round, _, _, _ := reuse.reuseTransaction(bc, author, gp, statedb, header, tx, blockPre, AlwaysFalse);
		reuseStatus == cmptypes.Hit || reuseStatus == cmptypes.FastHit {
		receipt := reuse.finalise(config, statedb, header, tx, usedGas, round.Receipt.GasUsed, round.RWrecord.Failed, msg)
		return receipt, nil, reuseStatus
	} else {
		receipt, err := core.ApplyTransaction(config, bc, author, gp, statedb, header, tx, usedGas, cfg)
		return receipt, err, reuseStatus
	}
}
