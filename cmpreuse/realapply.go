package cmpreuse

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

func (reuse *Cmpreuse) realApplyTransaction(config *params.ChainConfig, bc core.ChainContext, author *common.Address,
	gp *core.GasPool, statedb *state.StateDB, header *types.Header, cfg *vm.Config, c *core.Controller, msg core.Message) (gas uint64,
	failed bool, err error) {

	if c.IsFinish() {
		return
	}
	snap := statedb.Snapshot()
	if statedb.IsRWMode() {
		statedb.RWRecorder().RWClear()
	}
	if c.IsFinish() {
		return
	}
	context := core.NewEVMContext(msg, header, bc, author)
	if c.IsFinish() {
		return
	}
	evm := vm.NewEVM(context, statedb, config, *cfg)
	c.SetEvm(evm)
	if c.IsFinish() {
		return
	}
	_, gas, failed, err = core.ApplyMessage(evm, msg, gp)
	if err != nil {
		statedb.RevertToSnapshot(snap)
		if statedb.IsRWMode() {
			statedb.RWRecorder().RWClear()
		}
	}

	return
}
