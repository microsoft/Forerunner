// MSRA Computation Reuse Model

package cmpreuse

import (
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ivpusic/grpool"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/params"
)

// Cmpreuse struct
type Cmpreuse struct {
	MSRACache *cache.GlobalCache
}

// NewCmpreuse create new cmpreuse
func NewCmpreuse() *Cmpreuse {
	return &Cmpreuse{}
}

func (reuse *Cmpreuse) tryRealApplyTransaction(config *params.ChainConfig, bc core.ChainContext, author *common.Address,
	gp *core.GasPool, statedb *state.StateDB, header *types.Header, cfg vm.Config, c *core.Controller, msg core.Message) (uint64,
	bool, error) {

	t := time.Now()
	gas, failed, err := reuse.realApplyTransaction(config, bc, author, gp, statedb, header, cfg, c, msg)
	d := time.Since(t)

	if c.TryFinish() {
		cache.WaitRealApply = append(cache.WaitRealApply, time.Since(t))
		cache.RunTx = append(cache.RunTx, d)
		return gas, failed, err // apply finish and win compete
	} else {
		return 0, false, nil
	}
}

func (reuse *Cmpreuse) tryReuseTransaction(bc core.ChainContext, author *common.Address, gp *core.GasPool, statedb *state.StateDB,
	header *types.Header, tx *types.Transaction, c *core.Controller, blockPre *cache.BlockPre) (uint64, *cache.PreplayResult) {

	t := time.Now()
	status, round, cmpCnt, d0, d1 := reuse.reuseTransaction(bc, author, gp, statedb, header, tx, blockPre, c.IsFinish)

	if status == hit && c.TryFinish() {
		c.StopEvm()
		cache.WaitReuse = append(cache.WaitReuse, time.Since(t))
		cache.GetRW = append(cache.GetRW, d0)
		cache.SetDB = append(cache.SetDB, d1)
		cache.ReuseGasCount += round.Receipt.GasUsed
		cache.RWCmpCnt = append(cache.RWCmpCnt, cmpCnt)
		return hit, round // reuse finish and win compete
	} else {
		return status, nil // reuse finish but lost compete
	}
}

func (reuse *Cmpreuse) finalise(config *params.ChainConfig, statedb *state.StateDB, header *types.Header, tx *types.Transaction,
	usedGas *uint64, gasUsed uint64, failed bool, msg core.Message) *types.Receipt {
	// Update the state with pending changes
	var root []byte
	if config.IsByzantium(header.Number) {
		statedb.Finalise(true)
	} else {
		root = statedb.IntermediateRoot(config.IsEIP158(header.Number)).Bytes()
	}
	*usedGas += gasUsed

	// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx
	// based on the eip phase, we're passing whether the root touch-delete accounts.
	receipt := types.NewReceipt(root, failed, *usedGas)
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = gasUsed
	// if the transaction created a contract, store the creation address in the receipt.
	if msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(msg.From(), tx.Nonce())
	}
	// Set the receipt logs and create a bloom for filtering
	receipt.Logs = statedb.GetLogs(tx.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = statedb.BlockHash()
	receipt.BlockNumber = header.Number
	receipt.TransactionIndex = uint(statedb.TxIndex())
	return receipt
}

// ReuseTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction and an error if the transaction failed, indicating the
// block was invalid.
// Cmpreuse.ReuseTransaction used for only one scenario:
// 		process the block with reusing preplay results
// external args (comparing with core.ApplyTransaction)
//		`blockPre`:
//			used to help `getValidRW` skip rounds which are later then `blockPre`
//      `routinePool`:
//			used to routine reuse for reducing overhead of new routine
//		`controller`:
//			used to inter-process synchronization
// For the last return uint64 value,
// 		0: error;
// 		1: cache hit;
// 		2: no cache;
// 		3: cache but not in;
//		4: cache but result not match;
//		5: abort before hit or miss;
func (reuse *Cmpreuse) ReuseTransaction(config *params.ChainConfig, bc core.ChainContext, author *common.Address,
	gp *core.GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
	cfg vm.Config, blockPre *cache.BlockPre, routinePool *grpool.Pool, controller *core.Controller) (*types.Receipt,
	error, uint64) {

	if statedb.IsRWMode() || !statedb.IsShared() {
		panic("ReuseTransaction can only be used for process and statedb must be shared and not be RW mode.")
	}

	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, err, fail
	}

	reuseGp, reuseDB := *gp, statedb
	applyGp, applyDB := *gp, reuseDB.GetPair()

	controller.Reset()

	var gasUsed uint64
	var failed bool
	var realApplyErr error
	var realApply sync.WaitGroup
	realApply.Add(1)
	routinePool.JobQueue <- func() {
		gasUsed, failed, realApplyErr = reuse.tryRealApplyTransaction(config, bc, author, &applyGp, applyDB, header, cfg, controller, msg)
		realApply.Done()
	}

	if reuseStatus, round := reuse.tryReuseTransaction(bc, author, &reuseGp, reuseDB, header, tx, controller, blockPre); round != nil {
		t0 := time.Now()
		receipt := reuse.finalise(config, reuseDB, header, tx, usedGas, round.Receipt.GasUsed, round.RWrecord.Failed, msg)
		cache.TxFinalize = append(cache.TxFinalize, time.Since(t0))

		realApply.Wait() // can only updatePair after real apply is completed

		t1 := time.Now()
		reuseDB.UpdatePair()
		cache.UpdatePair = append(cache.UpdatePair, time.Since(t1))

		*gp = reuseGp
		return receipt, nil, reuseStatus // reuse first
	} else {
		realApply.Wait() //wait for real apply result set

		t0 := time.Now()
		var receipt *types.Receipt
		if realApplyErr == nil {
			receipt = reuse.finalise(config, applyDB, header, tx, usedGas, gasUsed, failed, msg)
		}
		cache.TxFinalize = append(cache.TxFinalize, time.Since(t0))

		t1 := time.Now()
		applyDB.UpdatePair()
		cache.UpdatePair = append(cache.UpdatePair, time.Since(t1))

		*gp = applyGp
		return receipt, realApplyErr, reuseStatus // real apply first
	}
}