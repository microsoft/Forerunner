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

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction and an error if the transaction failed, indicating the
// block was invalid.
// Cmpreuse.ApplyTransaction used for only one scenario:
// 		process the block with reusing preplay results, execute transaction serially
// external args (comparing with core.ApplyTransaction)
//		`blockPre`:
//			used to help `getValidRW` skip rounds which are later then `blockPre`
// For the last return uint64 value,
// 		0: error;
// 		1: no preplay;
// 		2: cache hit;
// 		3: cache miss(cache but not in or cache but result not match);
//		4: abort before hit or miss;
func (reuse *Cmpreuse) ApplyTransaction(config *params.ChainConfig, bc *core.BlockChain, author *common.Address,
	gp *core.GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
	cfg *vm.Config, blockPre *cache.BlockPre, getHashFunc vm.GetHashFunc, precompiles map[common.Address]vm.PrecompiledContract,
	pMsg *types.Message, signer types.Signer) (*types.Receipt, error, *cmptypes.ReuseStatus, time.Time) {

	if statedb.IsShared() || statedb.IsRWMode() {
		panic("ApplyTransaction can only be used for process and statedb must not be shared and not be RW mode.")
	}

	detailedTime := cfg.MSRAVMSettings.DetailTime

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
			return nil, err, &cmptypes.ReuseStatus{BaseStatus: cmptypes.Fail}, time.Now()
		}
	}

	var reuseStart time.Time
    if detailedTime {
		reuseStart = time.Now()
	}
	reuseStatus, round, d0, d1 := reuse.reuseTransaction(bc, author, gp, statedb, header, getHashFunc, precompiles, tx, blockPre, nil, true, false, cfg)
	if detailedTime {//false {
		cache.GetRW = append(cache.GetRW, d0)
		cache.SetDB = append(cache.SetDB, d1)
		cache.Reuse = append(cache.Reuse, time.Since(reuseStart))
	}
	if reuseStatus.BaseStatus == cmptypes.Hit {
		cache.ReuseGasCount += round.Receipt.GasUsed

		if !cfg.MSRAVMSettings.NoOverMatching {
			var t1 time.Time
			if detailedTime {
				t1 = time.Now()
			}
			if reuseStatus.BaseStatus == cmptypes.Hit && reuseStatus.HitType == cmptypes.MixHit && reuseStatus.MixStatus.MixHitType == cmptypes.PartialHit {

				for addr, change := range round.AccountChanges {
					if _, ok := reuseStatus.MixStatus.DepHitAddrMap[addr]; ok && header.Coinbase != addr {
						statedb.UpdateAccountChanged(addr, change)
					} else {
						statedb.UpdateAccountChanged(addr, cmptypes.DEFAULT_TXRESID)
					}
				}
				statedb.UpdateAccountChanged(header.Coinbase, cmptypes.DEFAULT_TXRESID)
			} else if reuseStatus.BaseStatus == cmptypes.Hit && reuseStatus.HitType == cmptypes.MixHit && reuseStatus.MixStatus.MixHitType == cmptypes.AllDepHit {
				statedb.ApplyAccountChanged(round.AccountChanges)
				//if msg.From() == header.Coinbase || (msg.To() != nil && *msg.To() == header.Coinbase) {
				//	reuseDB.UpdateAccountChanged(header.Coinbase, cmptypes.DEFAULT_TXRESID)
				//}
				statedb.UpdateAccountChanged(header.Coinbase, cmptypes.DEFAULT_TXRESID)
			} else if reuseStatus.BaseStatus == cmptypes.Hit && reuseStatus.HitType == cmptypes.TraceHit {
				if reuseStatus.TraceTrieHitAddrs != nil {
					for addr, reusedChange := range reuseStatus.TraceTrieHitAddrs {
						if reusedChange == nil {
							statedb.UpdateAccountChanged(addr, cmptypes.DEFAULT_TXRESID)
						} else {
							statedb.UpdateAccountChanged(addr, reusedChange)
						}
					}
					statedb.UpdateAccountChanged(header.Coinbase, cmptypes.DEFAULT_TXRESID)
				} else {
					panic("can not find the TraceTrieHitAddrs")
				}
			} else {
				statedb.UpdateAccountChangedByMap2(round.AccountChanges, cmptypes.DEFAULT_TXRESID, &header.Coinbase)
			}
			statedb.ClearSavedDirties()
			if detailedTime {
				cache.Update = append(cache.Update, time.Since(t1))
			}
		}

		var t0 time.Time
		if detailedTime {
			t0 = time.Now()
		}
		receipt, endTime := reuse.finaliseWithTime(config, statedb, header, tx, usedGas, round.Receipt.GasUsed, round.RWrecord.Failed, msg)
		if detailedTime {
			cache.TxFinalize = append(cache.TxFinalize, time.Since(t0))
		}
		return receipt, nil, reuseStatus, endTime
	} else {
		var realApplyStart time.Time
		if detailedTime {
			realApplyStart = time.Now()
		}
		gasUsed, failed, err, _ := reuse.realApplyTransaction(config, bc, author, gp, statedb, header, cfg, AlwaysFalse, nil, msg, nil)
		if detailedTime {
			cache.RealApply = append(cache.RealApply, time.Since(realApplyStart))
		}

		var t0 time.Time
		if detailedTime {
			t0 = time.Now()
		}
		var receipt *types.Receipt
		var endTime time.Time
		if err == nil {
			//receipt = reuse.finaliseByRealapply(config, statedb, header, tx, usedGas, gasUsed, failed, msg)
			receipt, endTime = reuse.finaliseWithTime(config, statedb, header, tx, usedGas, gasUsed, failed, msg)
		}else {
			endTime = time.Now()
		}
		if detailedTime {
			cache.TxFinalize = append(cache.TxFinalize, time.Since(t0))
		}
		if err == nil && !cfg.MSRAVMSettings.NoOverMatching {
			var t1 time.Time
			if detailedTime {
				t1 = time.Now()
			}
			// XXX
			statedb.UpdateAccountChangedBySlice(append(statedb.DirtyAddress(), header.Coinbase), cmptypes.DEFAULT_TXRESID)
			statedb.ClearSavedDirties()
			if detailedTime {
				cache.Update = append(cache.Update, time.Since(t1))
			}
		}
		return receipt, err, reuseStatus, endTime
	}
}
