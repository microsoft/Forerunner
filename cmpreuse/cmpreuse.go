// MSRA Computation Reuse Model

package cmpreuse

import (
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/crypto"
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
	gp *core.GasPool, statedb *state.StateDB, header *types.Header, cfg *vm.Config, c *core.Controller, msg core.Message) (uint64,
	bool, error) {

	t := time.Now()
	gas, failed, err := reuse.realApplyTransaction(config, bc, author, gp, statedb, header, cfg, c, msg, nil)
	d := time.Since(t)

	if c.TryAbortCounterpart() {
		cache.RunTx = append(cache.RunTx, d)
		return gas, failed, err // apply finish and win compete
	} else {
		return 0, false, nil
	}
}

func (reuse *Cmpreuse) tryReuseTransaction(bc core.ChainContext, author *common.Address, gp *core.GasPool, statedb *state.StateDB,
	header *types.Header, getHashFunc vm.GetHashFunc, precompiles map[common.Address]vm.PrecompiledContract, tx *types.Transaction, c *core.Controller, blockPre *cache.BlockPre, cfg *vm.Config) (*cmptypes.ReuseStatus, *cache.PreplayResult) {

	if cfg.MSRAVMSettings.CmpReuseChecking {
		defer c.ReuseDone.Done()
	}

	status, round, d0, d1 := reuse.reuseTransaction(bc, author, gp, statedb, header, getHashFunc, precompiles, tx, blockPre, c.IsAborted, true, cfg)

	if status.BaseStatus == cmptypes.Hit && c.TryAbortCounterpart() {
		c.StopEvm()
		cache.GetRW = append(cache.GetRW, d0)
		cache.SetDB = append(cache.SetDB, d1)
		cache.ReuseGasCount += round.Receipt.GasUsed
		return status, round // reuse finish and win compete
	} else {
		return status, nil // reuse finish but lost compete
	}
}

func (reuse *Cmpreuse) finaliseByRealapply(config *params.ChainConfig, statedb *state.StateDB, header *types.Header, tx *types.Transaction,
	usedGas *uint64, gasUsed uint64, failed bool, msg core.Message) *types.Receipt {
	return reuse.finalise(config, statedb, header, tx, usedGas, gasUsed, failed, msg)
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

	receipt := reuse.TryCreateReceipt(statedb, header, tx, msg)
	// update the basic info
	receipt.PostState = common.CopyBytes(root)
	receipt.CumulativeGasUsed = *usedGas
	receipt.GasUsed = gasUsed
	if failed {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}

	return receipt
	//// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx
	//// based on the eip phase, we're passing whether the root touch-delete accounts.
	////receipt := types.NewReceipt(root, failed, *usedGas)
	////receipt.TxHash = tx.Hash()
	//receipt.GasUsed = gasUsed
	//// if the transaction created a contract, store the creation address in the receipt.
	//if msg.To() == nil {
	//	receipt.ContractAddress = crypto.CreateAddress(msg.From(), tx.Nonce())
	//}
	//// Set the receipt logs and create a bloom for filtering
	//receipt.Logs = statedb.GetLogs(tx.Hash())
	//if statedb.BloomProcessor != nil {
	//	statedb.BloomProcessor.CreateBloomForTransaction(receipt)
	//} else {
	//	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	//}
	//receipt.BlockHash = statedb.BlockHash()
	//receipt.BlockNumber = header.Number
	//receipt.TransactionIndex = uint(statedb.TxIndex())
	//return receipt
}

func (reuse *Cmpreuse) TryCreateReceipt(statedb *state.StateDB, header *types.Header, tx *types.Transaction, msg core.Message) *types.Receipt {

	statedb.ReceiptMutex.Lock()
	defer statedb.ReceiptMutex.Unlock()

	if statedb.CurrentReceipt != nil && statedb.CurrentReceipt.TxHash == tx.Hash() {
		return statedb.CurrentReceipt
	}

	// set the initial values temporarily
	receipt := types.NewReceipt(nil, false, 0)
	receipt.TxHash = tx.Hash()
	// if the transaction created a contract, store the creation address in the receipt.
	if msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(msg.From(), tx.Nonce())
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

	statedb.CurrentReceipt = receipt

	return receipt
}

// ReuseTransaction attempts to reuse a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction and an error if the transaction failed, indicating the
// block was invalid.
// Cmpreuse.ReuseTransaction used for only one scenario:
// 		process the block with reusing preplay results, execute transaction concurrently
// external args (comparing with core.ApplyTransaction)
//		`blockPre`:
//			used to help `getValidRW` skip rounds which are later then `blockPre`
//      `routinePool`:
//			used to routine reuse for reducing overhead of new routine
//		`controller`:
//			used to inter-process synchronization
// For the last return uint64 value,
// 		0: error;
// 		1: no preplay;
// 		2: cache hit;
// 		3: cache miss(cache but not in or cache but result not match);
//		4: abort before hit or miss;
func (reuse *Cmpreuse) ReuseTransaction(config *params.ChainConfig, bc core.ChainContext, author *common.Address,
	gp *core.GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
	cfg *vm.Config, blockPre *cache.BlockPre, asyncPool *types.SingleThreadSpinningAsyncProcessor, controller *core.Controller,
	getHashFunc vm.GetHashFunc, precompiles map[common.Address]vm.PrecompiledContract,
	pMsg *types.Message, signer types.Signer) (*types.Receipt,
	error, *cmptypes.ReuseStatus) {

	if statedb.IsRWMode() || !statedb.IsShared() {
		panic("ReuseTransaction can only be used for process and statedb must be shared and not be RW mode.")
	}

	var msg types.Message
	if pMsg != nil {
		msg = *pMsg
	} else {
		var err error
		if signer == nil {
			signer = types.MakeSigner(config, header.Number)
		}
		msg, err = tx.AsMessage(signer)
		if err != nil {
			return nil, err, &cmptypes.ReuseStatus{BaseStatus: cmptypes.Fail}
		}
	}

	reuseGp, reuseDB := *gp, statedb
	applyGp, applyDB := *gp, reuseDB.GetPair()

	controller.Reset()

	var gasUsed uint64
	var failed bool
	var realApplyErr error
	var realApply sync.WaitGroup
	realApply.Add(1)
	realApplyStart := time.Now()

	if cfg.MSRAVMSettings.CmpReuseChecking {
		controller.ReuseDone.Add(1)
	}

	//routinePool.JobQueue <-
	doRealApply := func() {
		if cfg.MSRAVMSettings.CmpReuseChecking {
			controller.ReuseDone.Wait()
		}
		gasUsed, failed, realApplyErr = reuse.tryRealApplyTransaction(config, bc, author, &applyGp, applyDB, header, cfg, controller, msg)
		if gasUsed == 0 { // tryReuseTransaction win
			// try to help create receipt in parallel
			reuse.TryCreateReceipt(reuseDB, header, tx, msg)
		}
		realApply.Done()
	}

	asyncPool.RunJob(doRealApply)
	//go doRealApply()

	reuseStart := time.Now()
	if reuseStatus, round := reuse.tryReuseTransaction(bc, author, &reuseGp, reuseDB, header, getHashFunc, precompiles, tx, controller, blockPre, cfg); round != nil {
		waitReuse := time.Since(reuseStart)
		//MyAssert(reuseStatus.HitType != cmptypes.TraceHit)

		//var roundId uint64

		//if reuseStatus.BaseStatus == cmptypes.Hit && reuseStatus.HitType == cmptypes.DepHit && msg.From() != header.Coinbase && (msg.To() == nil || *msg.To() != header.Coinbase) {
		//	roundId = round.RoundID
		//} else {
		//	roundId = 0 // roundId = 0 means this res dep not be matched
		//}
		//reuseDB.UpdateAccountChangedByMap(round.WObjects, tx.Hash(), roundId, &header.Coinbase)

		t0 := time.Now()
		receipt := reuse.finalise(config, reuseDB, header, tx, usedGas, round.Receipt.GasUsed, round.RWrecord.Failed, msg)
		cache.TxFinalize = append(cache.TxFinalize, time.Since(t0))

		waitStart := time.Now()
		realApply.Wait() // can only updatePair after real apply is completed
		cache.WaitReuse = append(cache.WaitReuse, time.Since(waitStart)+waitReuse)

		t1 := time.Now()

		if reuseStatus.BaseStatus == cmptypes.Hit && reuseStatus.HitType == cmptypes.MixHit && reuseStatus.MixHitStatus.MixHitType == cmptypes.PartialHit {
			//use account level update instead of :
			curtxResId := cmptypes.DEFAULT_TXRESID
			for addr, change := range round.AccountChanges {
				if _, ok := reuseStatus.MixHitStatus.DepHitAddrMap[addr]; ok && header.Coinbase != addr {
					reuseDB.UpdateAccountChanged(addr, change)
				} else {
					reuseDB.UpdateAccountChanged(addr, curtxResId)
				}
			}
			reuseDB.UpdateAccountChanged(header.Coinbase, curtxResId)
		} else if reuseStatus.BaseStatus == cmptypes.Hit &&
			(reuseStatus.HitType == cmptypes.MixHit && reuseStatus.MixHitStatus.MixHitType == cmptypes.AllDepHit) {
			reuseDB.ApplyAccountChanged(round.AccountChanges)
			//if msg.From() == header.Coinbase || (msg.To() != nil && *msg.To() == header.Coinbase) {
			//	reuseDB.UpdateAccountChanged(header.Coinbase, cmptypes.DEFAULT_TXRESID)
			//}
			reuseDB.UpdateAccountChanged(header.Coinbase, cmptypes.DEFAULT_TXRESID)
		} else if reuseStatus.BaseStatus == cmptypes.Hit && reuseStatus.HitType == cmptypes.TraceHit {
			if reuseStatus.TraceTrieHitAddrs != nil {
				for addr, reusedChange := range reuseStatus.TraceTrieHitAddrs {
					if reusedChange == nil {
						statedb.UpdateAccountChanged(addr, cmptypes.DEFAULT_TXRESID)
					} else {
						statedb.UpdateAccountChanged(addr, reusedChange)
					}
				}
				reuseDB.UpdateAccountChanged(header.Coinbase, cmptypes.DEFAULT_TXRESID)
			} else {
				panic("can not find the TraceTrieHitAddrs")
			}

		} else {
			curtxResId := cmptypes.DEFAULT_TXRESID
			reuseDB.UpdateAccountChangedByMap2(round.AccountChanges, curtxResId, &header.Coinbase)
		}

		reuseDB.Update()
		cache.Update = append(cache.Update, time.Since(t1))

		*gp = reuseGp
		return receipt, nil, reuseStatus // reuse first
	} else {
		realApply.Wait() //wait for real apply result set
		cache.WaitRealApply = append(cache.WaitRealApply, time.Since(realApplyStart))

		t0 := time.Now()
		var receipt *types.Receipt
		if realApplyErr == nil {
			receipt = reuse.finaliseByRealapply(config, applyDB, header, tx, usedGas, gasUsed, failed, msg)
		}
		cache.TxFinalize = append(cache.TxFinalize, time.Since(t0))

		t1 := time.Now()
		// XXX
		reuseDB.UpdateAccountChangedBySlice(append(applyDB.DirtyAddress(), header.Coinbase), cmptypes.DEFAULT_TXRESID)

		applyDB.Update()
		cache.Update = append(cache.Update, time.Since(t1))

		*gp = applyGp
		return receipt, realApplyErr, reuseStatus // real apply first
	}
}
