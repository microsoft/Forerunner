package cmpreuse

import (
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"math/big"
	"time"
)

var emptyCodeHash = crypto.Keccak256Hash(nil)

const fail = 0
const hit = 1
const noCache = 2
const cacheNoIn = 3
const cacheNoMatch = 4
const unknown = 5

// codeHashEquivalent compare two common.Hash
func codeHashEquivalent(l common.Hash, r common.Hash) bool {
	if l == emptyCodeHash {
		l = common.Hash{}
	}
	if r == emptyCodeHash {
		r = common.Hash{}
	}
	return l == r
}

// CheckRChain check rw.Rchain
func CheckRChain(rw *cache.RWRecord, bc core.ChainContext, header *types.Header) bool {
	for key, value := range rw.RChain {
		switch key {
		case cmptypes.Coinbase:
			v := value.(common.Address)
			// fixed with direct ==
			if v != header.Coinbase {
				// log.Info("RChain Coinbase miss", "pve", v, "now", header.Coinbase)
				return false
			}
		case cmptypes.Timestamp:
			v := value.(*big.Int)
			if v.Uint64() != header.Time {
				// log.Info("RChain Timestamp miss", "pve", v, "now", header.Time)
				return false
			}
		case cmptypes.Number:
			v := value.(*big.Int)
			if v.Cmp(header.Number) != 0 {
				// log.Info("RChain Number miss", "pve", v, "now", header.Number)
				return false
			}
		case cmptypes.Difficulty:
			v := value.(*big.Int)
			if v.Cmp(header.Difficulty) != 0 {
				// log.Info("RChain Difficulty miss", "pve", v, "now", header.Difficulty)
				return false
			}
		case cmptypes.GasLimit:
			v := value.(uint64)
			if v != header.GasLimit {
				// log.Info("RChain GasLimit miss", "pve", v, "now", header.GasLimit)
				return false
			}
		case cmptypes.Blockhash:
			mBlockHash := value.(map[uint64]common.Hash)
			currentNum := header.Number.Uint64()
			getHashFn := core.GetHashFn(header, bc)
			for num, blkHash := range mBlockHash {
				if num > (currentNum-257) && num < currentNum {
					if blkHash != getHashFn(num) {
						// log.Info("RChain block hash miss", "pve", blkHash, "now", getHashFn(num))
						return false
					}
				} else if blkHash.Big().Cmp(common.Big0) != 0 {
					// log.Info("RChain block hash miss", "pve", blkHash, "now", "0")
					return false
				}
			}
		}
	}
	return true
}

// CheckRState check rw.Rstate
func CheckRState(rw *cache.RWRecord, statedb *state.StateDB, debugDiff bool) bool {
	// turn off rw mode temporarily
	isRWMode := statedb.IsRWMode()
	statedb.SetRWMode(false)
	defer statedb.SetRWMode(isRWMode)

	for addr, mValues := range rw.RState {
		for key, value := range mValues {
			switch key {
			case cmptypes.Exist:
				v := value.(bool)
				if v != statedb.Exist(addr) {
					if debugDiff {
						log.Info("RState Exist miss", "addr", addr, "pve", v, "now", statedb.Exist(addr))
					}
					return false
				}
			case cmptypes.Empty:
				v := value.(bool)
				if v != statedb.Empty(addr) {
					if debugDiff {
						log.Info("RState Empty miss", "addr", addr, "pve", v, "now", statedb.Empty(addr))
					}
					return false
				}
			case cmptypes.Balance:
				v := value.(*big.Int)
				if v.Cmp(statedb.GetBalance(addr)) != 0 {
					if debugDiff {
						log.Info("RState Balance miss", "addr", addr, "pve", v, "now", statedb.GetBalance(addr))
					}
					return false
				}
			case cmptypes.Nonce:
				v := value.(uint64)
				if v != statedb.GetNonce(addr) {
					if debugDiff {
						log.Info("RState Nonce miss", "addr", addr, "pve", v, "now", statedb.GetNonce(addr))
					}
					return false
				}
			case cmptypes.CodeHash:
				v := value.(common.Hash)
				w := statedb.GetCodeHash(addr)
				if !codeHashEquivalent(v, w) {
					if debugDiff {
						log.Info("RState CodeHash miss", "addr", addr, "pve", v.Big(), "now", statedb.GetCodeHash(addr).Big())
					}
					return false
				}
			case cmptypes.Storage:
				storage := value.(map[common.Hash]common.Hash)
				for k, v := range storage {
					if statedb.GetState(addr, k) != v {
						if debugDiff {
							log.Info("RState Storage miss", "addr", addr, "pve", v.Big(), "now", statedb.GetState(addr, k).Big())
						}
						return false
					}
				}
			case cmptypes.CommittedStorage:
				storage := value.(map[common.Hash]common.Hash)
				for k, v := range storage {
					if statedb.GetCommittedState(addr, k) != v {
						if debugDiff {
							log.Info("RState CommittedStorage miss", "addr", addr, "pve", v.Big(), "now", statedb.GetCommittedState(addr, k).Big())
						}
						return false
					}
				}
			}
		}
	}
	return true
}

// ApplyRWRecord apply reuse result
func ApplyRWRecord(statedb *state.StateDB, rw *cache.RWRecord, abort func() bool) {
	var suicideAddr []common.Address
	for addr, mValues := range rw.WState {
		for key, value := range mValues {
			if abort() {
				return
			}
			switch key {
			case cmptypes.Suicided:
				// Make sure Suicide is called after other mod operations to ensure that a state_object is always there
				// Otherwise, Suicide will fail to mark the state_object as suicided
				// Without deferring, it might cause trouble when a new account gets created and suicides within the same transaction
				// In that case, as we apply write sets out of order, if Suicide is applied before other mod operations to the account,
				// the account will be left in the state causing mismatch.
				suicideAddr = append(suicideAddr, addr)
			case cmptypes.Nonce:
				v := value.(uint64)
				statedb.SetNonce(addr, v)
			case cmptypes.Balance:
				v := value.(*big.Int)
				statedb.SetBalance(addr, v)
			case cmptypes.Code:
				v := value.(state.Code)
				statedb.SetCode(addr, v)
			case cmptypes.DirtyStorage:
				dirtyStorage := value.(map[common.Hash]common.Hash)
				for k, v := range dirtyStorage {
					statedb.SetState(addr, k, v)
				}
			}
		}
	}
	for _, addr := range suicideAddr {
		if abort() {
			return
		}
		statedb.Suicide(addr)
	}
}

// getValidRW get valid transaction from tx preplay cache
func (reuse *Cmpreuse) getValidRW(txPreplay *cache.TxPreplay, bc core.ChainContext, statedb *state.StateDB, header *types.Header,
	blockPre *cache.BlockPre, abort func() bool) (*cache.PreplayResult, bool, bool, bool, int64) {
	txPreplay.Mu.Lock()
	defer txPreplay.Mu.Unlock()

	var leastOne, isAbort bool
	var cmpCnt int64
	rwrecordKeys := txPreplay.PreplayResults.RWrecords.Keys()

	// rwrecordKeys are from oldest to newest. iterate them reversely
	for i := len(rwrecordKeys) - 1; i >= 0; i-- {
		if abort() {
			isAbort = true
			break
		}
		rawKey := rwrecordKeys[i].(uint64)

		rawRWrecord, _ := txPreplay.PreplayResults.RWrecords.Peek(rawKey)
		rwrecord := rawRWrecord.(*cache.RWRecord)
		if rwrecord == nil {
			continue
		}
		// can not use this as result
		if blockPre != nil && blockPre.ListenTimeNano < rwrecord.Round.TimestampNano {
			continue
		}
		leastOne = true
		cmpCnt++
		if CheckRChain(rwrecord, bc, header) && CheckRState(rwrecord, statedb, false) {
			// update the priority of the correct rwrecord simply
			txPreplay.PreplayResults.RWrecords.Get(rawKey)
			return rwrecord.Round, true, leastOne, isAbort, cmpCnt
		}
	}
	return nil, false, leastOne, isAbort, cmpCnt
}

// setStateDB use RW to update stateDB, add logs, benefit miner and deduct gas from the pool
func (reuse *Cmpreuse) setStateDB(bc core.ChainContext, author *common.Address, statedb *state.StateDB, header *types.Header,
	tx *types.Transaction, round *cache.PreplayResult, abort func() bool) {

	ApplyRWRecord(statedb, round.RWrecord, abort)

	if abort() {
		return
	}
	// Add Logs
	for _, receiptLog := range round.Receipt.Logs {
		if abort() {
			return
		}
		statedb.AddLog(&types.Log{
			Address:     receiptLog.Address,
			Topics:      receiptLog.Topics,
			Data:        receiptLog.Data,
			BlockNumber: header.Number.Uint64(),
		})
	}

	if abort() {
		return
	}
	if statedb.IsRWMode() { // Has write set in finalization
		statedb.RWRecorder().RWClear()
	} else { // Add gas fee to coinbase
		var beneficiary common.Address
		if author == nil {
			beneficiary, _ = bc.Engine().Author(header) // Ignore error, we're past header validation
		} else {
			beneficiary = *author
		}
		statedb.AddBalance(beneficiary, new(big.Int).Mul(new(big.Int).SetUint64(round.Receipt.GasUsed), tx.GasPrice()))
	}
}

func (reuse *Cmpreuse) reuseTransaction(bc core.ChainContext, author *common.Address, gp *core.GasPool, statedb *state.StateDB,
	header *types.Header, tx *types.Transaction, blockPre *cache.BlockPre, isFinish func() bool) (status uint64,
	round *cache.PreplayResult, cmpCnt int64, d0 time.Duration, d1 time.Duration) {

	var ok, leastOne, abort bool

	t0 := time.Now()
	txPreplay := reuse.MSRACache.GetTxPreplay(tx.Hash())
	if txPreplay == nil || txPreplay.PreplayResults.RWrecords.Len() == 0 {
		status = noCache // no cache, quit compete
		return
	}
	round, ok, leastOne, abort, cmpCnt = reuse.getValidRW(txPreplay, bc, statedb, header, blockPre, isFinish)
	d0 = time.Since(t0)

	if ok {
		if err := gp.SubGas(round.Receipt.GasUsed); err == nil {
			status = hit // cache hit
		} else {
			status = fail // fail for gas limit reach, quit compete
			return
		}
	} else {
		switch {
		case abort:
			status = unknown // abort before hit or miss
		case leastOne:
			status = cacheNoMatch // cache but result not match, quit compete
		default:
			status = cacheNoIn // cache but not in, quit compete
		}
		return
	}

	t1 := time.Now()
	reuse.setStateDB(bc, author, statedb, header, tx, round, isFinish)
	d1 = time.Since(t1)

	return
}
