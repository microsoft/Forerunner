package cmpreuse

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"time"
)

func (reuse *Cmpreuse) realApplyTransaction(config *params.ChainConfig, bc core.ChainContext, author *common.Address,
	gp *core.GasPool, statedb vm.StateDB, header *types.Header, pCfg *vm.Config, c *Controller, msg core.Message) {

	var d time.Duration

	t0 := time.Now()
	snap := statedb.Snapshot()
	if statedb.IsRWMode() {
		statedb.RWRecorder().RWClear()
	}
	context := core.NewEVMContext(msg, header, bc, author)
	c.evm = vm.NewEVM(context, statedb, config, *pCfg)
	_, gas, failed, err := core.ApplyMessage(c.evm, msg, gp)
	if err != nil || c.evm.Cancelled() {
		statedb.RevertToSnapshot(snap)
		if statedb.IsRWMode() {
			statedb.RWRecorder().RWClear()
		}
	}
	d = time.Since(t0)

	if c.Finish(false) { // real apply faster
		c.applyDoneCh <- ApplyResult{
			gasUsed: gas,
			failed:  failed,
			err:     err,

			duration: d,
		} // apply finish and win compete
	}
}
