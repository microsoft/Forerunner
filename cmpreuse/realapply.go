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

func (reuse *Cmpreuse) realApplyTransaction(config *params.ChainConfig, bc core.ChainContext, author *common.Address,
	gp *core.GasPool, statedb *state.StateDB, header *types.Header, cfg *vm.Config, abort func() bool, setEvm func(evm *vm.EVM),
	msg core.Message, tx *types.Transaction) (gas uint64, failed bool, err error, d time.Duration) {
	t := time.Now()
	defer func() {
		d = time.Since(t)
	}()

	if abort() {
		return
	}
	snap := statedb.Snapshot()
	if statedb.IsRWMode() {
		statedb.RWRecorder().RWClear()
	}
	if abort() {
		return
	}
	context := core.NewEVMContext(msg, header, bc, author)
	if abort() {
		return
	}
	evm := vm.NewEVM(context, statedb, config, *cfg)
	//expCount := 10000000
	//expCount = 1
	if cfg.MSRAVMSettings.EnableReuseTracer { //&& ReuseTracerTracedTxCount < expCount {
		//if strings.ToLower(tx.To().Hex()) == "0xdac17f958d2ee523a2206206994597c13d831ec7" {
		rt := NewReuseTracer(statedb, header, evm.GetHash, evm.ChainConfig().ChainID, evm.ChainConfig().Rules(header.Number))
		if cfg.MSRAVMSettings.ReuseTracerChecking {
			rt.DebugFlag = true
		}
		evm.RTracer = rt
		defer func() {
			if e := recover(); e != nil {
				txHex := tx.Hash().Hex()
				fmt.Printf("Tx %s Tracer Error\n  %s :%s", txHex, e, debug.Stack())
				rt.DumpDebugBuffer(fmt.Sprintf("/tmp/errTxTrace%v.txt", txHex))
				//rt.DumpDebugBuffer(fmt.Sprintf("/tmp/errTxTrace.txt"))
				panic(e)
			}
		}()
	}

	if setEvm != nil {
		setEvm(evm)
	}
	if abort() {
		return
	}
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
