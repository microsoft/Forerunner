// MSRA Computation Reuse Model

package cmpreuse

import (
	"encoding/json"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/crypto"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
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

// getGroundRrecord Get a valid Round
func getGroundRrecord(rw *cache.RWRecord, bc core.ChainContext, statedb state.StateDB, header *types.Header) *cache.RWRecord {
	if rw == nil {
		return nil
	}

	resRW := &cache.RWRecord{
		RState: make(map[common.Address]map[cmptypes.Field]interface{}),
		RChain: make(map[cmptypes.Field]interface{}),
		WState: make(map[common.Address]map[cmptypes.Field]interface{}),
		Hashed: false,
	}

	// turn off rw mode temporarily
	isRWMode := statedb.IsRWMode()
	statedb.SetRWMode(false)
	defer statedb.SetRWMode(isRWMode)

	rw.IterMu.Lock()
	defer rw.IterMu.Unlock()
	for key, value := range rw.RChain {
		switch key {
		case cmptypes.Coinbase:
			resRW.RChain[key] = header.Coinbase
		case cmptypes.Timestamp:
			resRW.RChain[key] = new(big.Int).SetUint64(header.Time)
		case cmptypes.Number:
			resRW.RChain[key] = header.Number
		case cmptypes.Difficulty:
			resRW.RChain[key] = header.Difficulty
		case cmptypes.GasLimit:
			resRW.RChain[key] = header.GasLimit
		case cmptypes.Blockhash:
			resRW.RChain[key] = make(map[uint64]*big.Int)
			mBlockHash := value.(map[uint64]common.Hash)
			currentNum := header.Number.Uint64()
			getHashFn := core.GetHashFn(header, bc)
			for num := range mBlockHash {
				if num > (currentNum-257) && num < currentNum {
					resRW.RChain[key].(map[uint64]*big.Int)[num] = getHashFn(num).Big()
				} else {
					resRW.RChain[key].(map[uint64]*big.Int)[num] = common.Big0
				}
			}
		}
	}

	for addr, mValues := range rw.RState {
		resRW.RState[addr] = make(map[cmptypes.Field]interface{})
		for key, value := range mValues {
			switch key {
			case cmptypes.Exist:
				resRW.RState[addr][key] = statedb.Exist(addr)
			case cmptypes.Empty:
				resRW.RState[addr][key] = statedb.Empty(addr)
			case cmptypes.Balance:
				resRW.RState[addr][key] = statedb.GetBalance(addr)
			case cmptypes.Nonce:
				resRW.RState[addr][key] = statedb.GetNonce(addr)
			case cmptypes.CodeHash:
				resRW.RState[addr][key] = statedb.GetCodeHash(addr)
			case cmptypes.Storage:
				resRW.RState[addr][key] = make(map[common.Hash]common.Hash)
				storage := value.(map[common.Hash]common.Hash)
				for k := range storage {
					resRW.RState[addr][key].(map[common.Hash]common.Hash)[k] = statedb.GetState(addr, k)
				}
			case cmptypes.CommittedStorage:
				resRW.RState[addr][key] = make(map[common.Hash]common.Hash)
				storage := value.(map[common.Hash]common.Hash)
				for k := range storage {
					resRW.RState[addr][key].(map[common.Hash]common.Hash)[k] = statedb.GetCommittedState(addr, k)
				}
			}
		}
	}

	return resRW
}

// finalise finalise
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

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
// Cmpreuse.ApplyTransaction used for these scenarios
// 		1. preplay,
// 		2. validate a block when interChain (blockchain.processor.Process)
//			2.1 record the ground truth rw state to be used for analyze
//			2.2 process the block with reusing preplay results
//		3. worker package the next block
// external args (comparing with core.ApplyTransaction)
// 		`roundID`:
//			used for scenario 1. 0 for other scenarios
//		`blockPre`:
//			used for scenario 2. help `getValidRW` skip rounds which are later then `blockPre`
//		`groundFlag`:
// 			0 for no ground feature, scenario 1 and 2.1;
// 			1 for recording the ground truth rw states, scenario 2.2 ;
// 			2 for just printing the rw states, scenario 3.
// For the last return uint64 value,
// 		0: error;
// 		1: cache hit;
// 		2: no cache;
// 		3: cache but not in;
//		4: cache but result not match;
//		5: abort before hit or miss;
func (reuse *Cmpreuse) ApplyTransaction(config *params.ChainConfig, bc core.ChainContext, author *common.Address,
	gp *core.GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64,
	cfg vm.Config, roundID uint64, blockPre *cache.BlockPre, groundFlag uint64, fromProcess bool) (*types.Receipt, error, uint64) {
	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, err, 0
	}

	reuseGp, reuseDB := *gp, statedb
	applyGp, applyDB := *gp, reuseDB.GetPair()

	controller := NewController()
	controller.wg.Add(2)
	defer controller.Close()

	t0 := time.Now()
	go reuse.reuseTransaction(bc, author, &reuseGp, reuseDB, header, tx, controller, blockPre)
	go reuse.realApplyTransaction(config, bc, author, &applyGp, applyDB, header, &cfg, controller, msg)

	select {
	case result := <-controller.reuseDoneCh:
		if fromProcess {
			cache.WaitReuse = append(cache.WaitReuse, time.Since(t0))
			cache.GetRW = append(cache.GetRW, result.duration[0])
			cache.SetDB = append(cache.SetDB, result.duration[1])
			cache.ReuseCount++
			cache.RWCmpCnt = append(cache.RWCmpCnt, result.cmpCnt)
		}

		t1 := time.Now()
		reuseDB.UpdatePair()
		if fromProcess {
			cache.UpdatePair = append(cache.UpdatePair, time.Since(t1))
		}

		t2 := time.Now()
		receipt := reuse.finalise(config, reuseDB, header, tx, usedGas, result.gasUsed, result.rwrecord.Failed, msg)
		if fromProcess {
			cache.TxFinalize = append(cache.TxFinalize, time.Since(t2))
		}

		*gp = reuseGp

		if groundFlag == 0 {
			if reuseDB.IsRWMode() {
				reuse.setMainResult(roundID, tx, receipt, result.rwrecord)
			}
		} else {
			reuse.commitGround(tx, receipt, result.rwrecord, groundFlag)
		}
		return receipt, nil, controller.reuseStatus // reuse first
	case result := <-controller.applyDoneCh:
		if fromProcess {
			cache.WaitRealApply = append(cache.WaitRealApply, time.Since(t0))
			cache.RunTx = append(cache.RunTx, result.duration)
		}

		t1 := time.Now()
		applyDB.UpdatePair()
		if fromProcess {
			cache.UpdatePair = append(cache.UpdatePair, time.Since(t1))
		}

		t2 := time.Now()
		var receipt *types.Receipt
		if result.err == nil {
			receipt = reuse.finalise(config, applyDB, header, tx, usedGas, result.gasUsed, result.failed, msg)
		} else {
			receipt = nil
		}
		if fromProcess {
			cache.TxFinalize = append(cache.TxFinalize, time.Since(t2))
		}

		*gp = applyGp

		// Record RWSet if it's RWStateDB
		var newTxRcd *cache.RWRecord
		if applyDB.IsRWMode() {
			rstate, rchain, wstate := applyDB.RWRecorder().RWDump()
			newTxRcd = cache.NewRWRecord(rstate, rchain, wstate, receipt != nil && receipt.Status == types.ReceiptStatusFailed)
			applyDB.RWRecorder().RWClear() // Write set got
		}
		if groundFlag == 0 {
			if applyDB.IsRWMode() {
				reuse.setMainResult(roundID, tx, receipt, newTxRcd)
			}
		} else {
			reuse.commitGround(tx, receipt, newTxRcd, groundFlag)
		}
		return receipt, result.err, controller.reuseStatus // real apply first
	}
}

func (reuse *Cmpreuse) setMainResult(roundID uint64, tx *types.Transaction, receipt *types.Receipt, rwrecord *cache.RWRecord) {
	ok := reuse.MSRACache.SetMainResult(roundID, tx.Hash(), receipt, rwrecord)
	if ok {
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
	if groundFlag == 1 {
		reuse.MSRACache.CommitGround(groundResult)
	} else if groundFlag == 2 {
		reuse.printGround(groundResult)
	}
}

func (reuse *Cmpreuse) printGround(ground *cache.SimpleResult) {
	bytes, e := json.Marshal(&cache.LogRWrecord{
		TxHash:        ground.TxHash,
		RoundID:       0,
		Receipt:       ground.Receipt,
		RWrecord:      ground.RWrecord,
		Timestamp:     ground.RWTimeStamp,
		TimestampNano: ground.RWTimeStampNano,
		Filled:        -1,
	})
	if e == nil {
		log.Info("😋 " + string(bytes))
	} else {
		log.Info("😋", "ground", e)
	}
}
