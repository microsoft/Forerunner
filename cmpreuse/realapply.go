// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

package cmpreuse

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"runtime/debug"
	"time"
)

func (reuse *Cmpreuse) realApplyTransaction(config *params.ChainConfig, bc *core.BlockChain, author *common.Address,
	gp *core.GasPool, statedb *state.StateDB, header *types.Header, cfg *vm.Config, abort func() bool, setEvm func(evm *vm.EVM),
	msg core.Message, tx *types.Transaction) (gas uint64, failed bool, err error, d time.Duration) {
	d = 0
	//t := time.Now()
	//defer func() {
	//	d = time.Since(t)
	//}()

	//if abort != nil && abort() {
	//	return
	//}
	snap := statedb.Snapshot()
	if statedb.IsRWMode() {
		statedb.RWRecorder().RWClear()
	}
	//if abort != nil && abort() {
	//	return
	//}
	context := core.NewEVMContext(msg, header, bc, author)
	//if abort != nil && abort() {
	//	return
	//}
	evm := vm.NewEVM(context, statedb, config, *cfg)
	//expCount := 10000000
	//expCount = 1
	if cfg.MSRAVMSettings.EnableReuseTracer && !cfg.MSRAVMSettings.NoTrace {
		rt := NewReuseTracer(statedb, header, evm.GetHash, evm.ChainConfig().ChainID, evm.ChainConfig().Rules(header.Number))
		if cfg.MSRAVMSettings.ReuseTracerChecking {
			rt.DebugFlag = true
		}
		evm.RTracer = rt
		defer func() {
			if e := recover(); e != nil {
				txHex := tx.Hash().Hex()
				fmt.Printf("Tx %s Tracer Error\n  %s :%s", txHex, e, debug.Stack())
				rt.DebugBuffer.DumpBufferToFile(fmt.Sprintf("/tmp/errTxTrace%v.txt", txHex))
				panic(e)
			}
		}()
	}

	//if setEvm != nil {
	//	setEvm(evm)
	//}
	//if abort != nil && abort() {
	//	return
	//}
	_, gas, failed, err = core.ApplyMessage(evm, msg, gp)

	if err != nil {
		statedb.RevertToSnapshot(snap)
		if statedb.IsRWMode() {
			statedb.RWRecorder().RWClear()
		}
	}

	if err == nil && cfg.MSRAVMSettings.EnableReuseTracer && evm.RTracer != nil {
		statedb.ReuseTracer = evm.RTracer
	}

	return
}
