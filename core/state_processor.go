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
	"github.com/ivpusic/grpool"
	"time"
)

type TransactionApplier interface {
	ApplyTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address,
		gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
		cfg vm.Config, blockPre *cache.BlockPre) (*types.Receipt, error, *cmptypes.ReuseStatus)

	ReuseTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address,
		gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
		cfg vm.Config, blockPre *cache.BlockPre, routinePool *grpool.Pool, controller *Controller) (*types.Receipt,
		error, *cmptypes.ReuseStatus)

	PreplayTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address,
		gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
		cfg vm.Config, RoundID uint64, blockPre *cache.BlockPre, groundFlag uint64) (*types.Receipt, error)
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
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for block rewards
}

// NewStateProcessor initialises a new StateProcessor.
func NewStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *StateProcessor {
	return &StateProcessor{
		config: config,
		bc:     bc,
		engine: engine,
	}
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
	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}

	// Record ground truth
	var groundStatedb *state.StateDB
	if cfg.MSRAVMSettings.GroundRecord {
		groundStatedb = state.NewRWStateDB(statedb.Copy())
	}
	// Add Block
	blockPre := cache.NewBlockPre(block)
	if p.bc.MSRACache != nil {
		p.bc.MSRACache.CommitBlockPre(blockPre)
	}
	var reuseResult []*cmptypes.ReuseStatus

	if cfg.MSRAVMSettings.CmpReuse {
		statedb.ShareCopy()
	}
	controller := NewController()
	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		statedb.Prepare(tx.Hash(), block.Hash(), i)

		var err error
		var receipt *types.Receipt
		t0 := time.Now()

		if cfg.MSRAVMSettings.GroundRecord && groundStatedb != nil {
			// Record ground truth
			//log.Info("GroundTruth")
			// var groundStatedb vm.StateDB
			// groundStatedb = state.NewRWStateDB(statedb.Copy())
			groundStatedb.Prepare(tx.Hash(), block.Hash(), i)
			receipt, err = p.bc.Cmpreuse.PreplayTransaction(p.config, p.bc, nil, groundGP, groundStatedb, header, tx, groundUsedGas, cfg, 0, blockPre, 1)
			//log.Info("GroundTruth Finish")
		}

		if cfg.MSRAVMSettings.CmpReuse {
			var reuseStatus *cmptypes.ReuseStatus
			receipt, err, reuseStatus = p.bc.Cmpreuse.ReuseTransaction(p.config, p.bc, nil, gp, statedb, header, tx, usedGas, cfg, blockPre, p.bc.MSRACache.RoutinePool, controller)
			reuseResult = append(reuseResult, reuseStatus)

		} else {
			receipt, err = ApplyTransaction(p.config, p.bc, nil, gp, statedb, header, tx, usedGas, cfg)
		}
		t1 := time.Now()
		cache.Apply = append(cache.Apply, t1.Sub(t0))
		//log.Debug("[TransactionProcessTime]", "txHash", tx.Hash(), "processTime", t1.Sub(t0).Nanoseconds())

		if err != nil {
			log.Info("GroundTruth Error", "err", err.Error())
			return nil, nil, 0, err
		}
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
		//if reuseResult != nil && cache.Apply[len(cache.Apply)-1] > time.Millisecond {
		//	context := []interface{}{
		//		"transactionIndex", i,
		//		"reuseStatus", reuseResult[len(reuseResult)-1],
		//	}
		//	if reuseResult[i] == 1 {
		//		lastIndex := len(cache.WaitReuse) - 1
		//		context = append(context, "waitReuse", common.PrettyDuration(cache.WaitReuse[lastIndex]),
		//			"getRW(cnt)", fmt.Sprintf("%s(%d)",
		//				common.PrettyDuration(cache.GetRW[lastIndex]), cache.RWCmpCnt[lastIndex]),
		//			"setDB", common.PrettyDuration(cache.SetDB[lastIndex]))
		//	} else {
		//		lastIndex := len(cache.WaitRealApply) - 1
		//		context = append(context, "waitRealApply", common.PrettyDuration(cache.WaitRealApply[lastIndex]),
		//			"realApply", common.PrettyDuration(cache.RunTx[lastIndex]))
		//	}
		//	context = append(context, "updatePair", common.PrettyDuration(cache.Update[len(cache.Update)-1]),
		//		"txFinalize", common.PrettyDuration(cache.TxFinalize[len(cache.TxFinalize)-1]))
		//	log.Info("Apply new transaction", context...)
		//}
	}
	if cfg.MSRAVMSettings.CmpReuse {
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
	cache.Finalize += time.Since(t0)
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
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = statedb.BlockHash()
	receipt.BlockNumber = header.Number
	receipt.TransactionIndex = uint(statedb.TxIndex())

	return receipt, err
}
