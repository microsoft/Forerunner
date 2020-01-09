package cmpreuse

import (
	"fmt"
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
	if rw.RChain.Coinbase != nil && header.Coinbase != *rw.RChain.Coinbase {
		// log.Info("RChain Coinbase miss", "pve", *rw.RChain.Coinbase, "now", header.Coinbase)
		return false
	}
	if rw.RChain.Timestamp != nil && header.Time != rw.RChain.Timestamp.Uint64() {
		// log.Info("RChain Timestamp miss", "pve", rw.RChain.Timestamp, "now", header.Time)
		return false
	}
	if rw.RChain.Number != nil && header.Number.Cmp(rw.RChain.Number) != 0 {
		// log.Info("RChain Number miss", "pve", rw.RChain.Number, "now", header.Number)
		return false
	}
	if rw.RChain.Difficulty != nil && header.Difficulty.Cmp(rw.RChain.Difficulty) != 0 {
		// log.Info("RChain Difficulty miss", "pve", rw.RChain.Difficulty, "now", header.Difficulty)
		return false
	}
	if rw.RChain.GasLimit != nil && header.GasLimit != *rw.RChain.GasLimit {
		// log.Info("RChain GasLimit miss", "pve", rw.RChain.GasLimit, "now", header.GasLimit)
		return false
	}
	if rw.RChain.Blockhash != nil {
		currentNum := header.Number.Uint64()
		getHashFn := core.GetHashFn(header, bc)
		for num, blkHash := range rw.RChain.Blockhash {
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
	return true
}

func CheckRChainField(key cmptypes.Field, value interface{}, bc core.ChainContext, header *types.Header) bool {
	switch key {
	case cmptypes.Coinbase:
		v := value.(common.Address)
		// fixed with direct ==
		return v == header.Coinbase
	case cmptypes.Timestamp:
		v := value.(*big.Int)
		return v.Uint64() == header.Time
	case cmptypes.Number:
		v := value.(*big.Int)
		return v.Cmp(header.Number) == 0
	case cmptypes.Difficulty:
		v := value.(*big.Int)
		return v.Cmp(header.Difficulty) == 0
	case cmptypes.GasLimit:
		v := value.(uint64)
		return v == header.GasLimit
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
		return true
	}
	return false
}

// CheckRState check rw.Rstate
func CheckRState(rw *cache.RWRecord, statedb *state.StateDB, debugDiff bool) bool {
	// turn off rw mode temporarily
	isRWMode := statedb.IsRWMode()
	statedb.SetRWMode(false)
	defer statedb.SetRWMode(isRWMode)

	for addr, rstate := range rw.RState {
		if rstate.Exist != nil && *rstate.Exist != statedb.Exist(addr) {
			if debugDiff {
				log.Info("RState Exist miss", "addr", addr, "pve", *rstate.Exist, "now", statedb.Exist(addr))
			}
			return false
		}
		if rstate.Empty != nil && *rstate.Empty != statedb.Empty(addr) {
			if debugDiff {
				log.Info("RState Empty miss", "addr", addr, "pve", *rstate.Empty, "now", statedb.Empty(addr))
			}
			return false
		}
		if rstate.Balance != nil && rstate.Balance.Cmp(statedb.GetBalance(addr)) != 0 {
			if debugDiff {
				log.Info("RState Balance miss", "addr", addr, "pve", rstate.Balance, "now", statedb.GetBalance(addr))
			}
			return false
		}
		if rstate.Nonce != nil && *rstate.Nonce != statedb.GetNonce(addr) {
			if debugDiff {
				log.Info("RState Nonce miss", "addr", addr, "pve", *rstate.Nonce, "now", statedb.GetNonce(addr))
			}
			return false
		}
		if rstate.CodeHash != nil && !codeHashEquivalent(*rstate.CodeHash, statedb.GetCodeHash(addr)) {
			if debugDiff {
				log.Info("RState CodeHash miss", "addr", addr, "pve", *rstate.CodeHash, "now", statedb.GetCodeHash(addr).Big())
			}
			return false
		}
		if rstate.Storage != nil {
			for k, v := range rstate.Storage {
				if statedb.GetState(addr, k) != v {
					if debugDiff {
						log.Info("RState Storage miss", "addr", addr, "pve", v.Big(), "now", statedb.GetState(addr, k).Big())
					}
					return false
				}
			}
		}
		if rstate.CommittedStorage != nil {
			for k, v := range rstate.CommittedStorage {
				if statedb.GetCommittedState(addr, k) != v {
					if debugDiff {
						log.Info("RState CommittedStorage miss", "addr", addr, "pve", v.Big(), "now", statedb.GetCommittedState(addr, k).Big())
					}
					return false
				}
			}
		}
	}
	return true
}

//func GetRChainField(loc *cache.Location, bc core.ChainContext, header *types.Header) interface{} {
//	switch loc.Field {
//	case cmptypes.Coinbase:
//		return header.Coinbase
//	case cmptypes.Timestamp:
//		return header.Time
//	case cmptypes.Number:
//		return header.Number
//	case cmptypes.Difficulty:
//		return header.Difficulty
//	case cmptypes.GasLimit:
//		return header.GasLimit
//	case cmptypes.Blockhash:
//		getHashFn := core.GetHashFn(header, bc)
//		number := loc.Loc.(uint64)
//		return getHashFn(number)
//	}
//	return nil
//}
//
//func GetRStateValue(addrLoc *cache.AddrLocation, statedb *state.StateDB) interface{} {
//	addr := addrLoc.Address
//	switch addrLoc.Location.Field {
//	case cmptypes.Exist:
//		return statedb.Exist(addr)
//	case cmptypes.Empty:
//		return statedb.Empty(addr)
//	case cmptypes.Balance:
//		return statedb.GetBalance(addr)
//	case cmptypes.Nonce:
//		return statedb.GetNonce(addr)
//	case cmptypes.CodeHash:
//		return statedb.GetCodeHash(addr)
//	case cmptypes.Storage:
//		position := addrLoc.Location.Loc.(common.Hash)
//		return statedb.GetState(addr, position)
//	case cmptypes.CommittedStorage:
//		position := addrLoc.Location.Loc.(common.Hash)
//		return statedb.GetCommittedState(addr, position)
//	}
//	return nil
//}

// ApplyWState apply one write state for one address, return whether this account is suicided.
func ApplyWState(statedb *state.StateDB, addr common.Address, wstate *state.WriteState) bool {
	if wstate.Nonce != nil {
		statedb.SetNonce(addr, *wstate.Nonce)
	}
	if wstate.Balance != nil {
		statedb.SetBalance(addr, wstate.Balance)
	}
	if wstate.Code != nil {
		statedb.SetCode(addr, *wstate.Code)
	}
	if wstate.DirtyStorage != nil {
		for k, v := range wstate.DirtyStorage {
			statedb.SetState(addr, k, v)
		}
	}
	// Make sure Suicide is called after other mod operations to ensure that a state_object is always there
	// Otherwise, Suicide will fail to mark the state_object as suicided
	// Without deferring, it might cause trouble when a new account gets created and suicides within the same transaction
	// In that case, as we apply write sets out of order, if Suicide is applied before other mod operations to the account,
	// the account will be left in the state causing mismatch.
	return wstate.Suicided != nil && *wstate.Suicided
}

// ApplyWStates apply write state for each address in wstate
func ApplyWStates(statedb *state.StateDB, rw *cache.RWRecord, abort func() bool) {
	var suicideAddr []common.Address
	for addr, wstate := range rw.WState {
		if abort() {
			return
		}
		if ApplyWState(statedb, addr, wstate) {
			suicideAddr = append(suicideAddr, addr)
		}
	}
	for _, addr := range suicideAddr {
		if abort() {
			return
		}
		statedb.Suicide(addr)
	}
}

func ApplyWObjects(statedb *state.StateDB, rw *cache.RWRecord, wobject state.ObjectMap, abort func() bool) {
	var suicideAddr []common.Address
	for addr, object := range wobject {
		if abort() {
			return
		}
		if object == nil {
			wstate, ok := rw.WState[addr]
			if !ok {
				panic(fmt.Sprintf("Write object miss, addr = %s", addr.Hex()))
			}
			if ApplyWState(statedb, addr, wstate) {
				suicideAddr = append(suicideAddr, addr)
			}
		} else {
			statedb.ApplyStateObject(object)
			wobject[addr] = nil
		}
	}
	for _, addr := range suicideAddr {
		if abort() {
			return
		}
		statedb.Suicide(addr)
	}
}

func (reuse *Cmpreuse) fastCheckProcessOrder(txPreplay *cache.TxPreplay, db *state.StateDB, header *types.Header, blockPre *cache.BlockPre) {
	// TODO
}

func (reuse *Cmpreuse) fastCheckRState(txPreplay *cache.TxPreplay, bc core.ChainContext, statedb *state.StateDB,
	header *types.Header, blockPre *cache.BlockPre) (*cache.PreplayResult, bool) {
	readDep, ok := txPreplay.PreplayResults.ReadDeps.Get(header.ParentHash)
	if ok {
		dep := readDep.(*cache.ReadDep)
		if !dep.IsNoDep {
			return nil, false
		}
		res, ok2 := txPreplay.PreplayResults.Rounds.Peek(dep.RoundID)
		if ok2 {
			preplayRes := res.(*cache.PreplayResult)
			record := preplayRes.RWrecord
			if record == nil {
				return nil, false
			}
			// can not use this as result
			if blockPre != nil && blockPre.ListenTimeNano < record.Round.TimestampNano {
				return nil, false
			}

			if !CheckRChain(record, bc, header) {
				return nil, false
			}
			// check whether addresses in record have been changed in current pending of statedb
			for _, addr := range record.RAddresses {
				if statedb.IsInPending(addr) {
					return nil, false
				}
			}

			return preplayRes, true
		}
	}
	return nil, false
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
	tx *types.Transaction, round *cache.PreplayResult, status cmptypes.ReuseStatus, abort func() bool) {

	if status == cmptypes.FastHit && !statedb.IsRWMode() {
		ApplyWObjects(statedb, round.RWrecord, round.WObjects, abort)
	} else {
		ApplyWStates(statedb, round.RWrecord, abort)
	}

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
	header *types.Header, tx *types.Transaction, blockPre *cache.BlockPre, isFinish func() bool) (status cmptypes.ReuseStatus,
	round *cache.PreplayResult, cmpCnt int64, d0 time.Duration, d1 time.Duration) {

	var ok, leastOne, abort bool

	t0 := time.Now()
	txPreplay := reuse.MSRACache.GetTxPreplay(tx.Hash())
	if txPreplay == nil || txPreplay.PreplayResults.RWrecords.Len() == 0 {
		status = cmptypes.NoCache // no cache, quit compete
		return
	}

	round, ok = reuse.fastCheckRState(txPreplay, bc, statedb, header, blockPre)
	if ok {
		d0 = time.Since(t0)
		if round.RWrecord == nil {
			panic(" > > > > > > > > > > > > > > fastcheck return nil rwrecord")
		}
		cmpCnt = 1
		status = cmptypes.FastHit
	} else {
		round, ok, leastOne, abort, cmpCnt = reuse.getValidRW(txPreplay, bc, statedb, header, blockPre, isFinish)
		if ok {
			d0 = time.Since(t0)
			if round.RWrecord == nil {
				panic(" > > > > > > > > > > > > > > getValidRW return nil rwrecord")
			}
			status = cmptypes.Hit // cache hit
		} else {
			switch {
			case abort:
				status = cmptypes.Unknown // abort before hit or miss
			case leastOne:
				status = cmptypes.CacheNoMatch // cache but result not match, quit compete
			default:
				status = cmptypes.CacheNoIn // cache but not in, quit compete
			}
			return
		}
	}
	if err := gp.SubGas(round.Receipt.GasUsed); err != nil {
		status = cmptypes.Fail // fail for gas limit reach, quit compete
		return
	}

	t1 := time.Now()
	reuse.setStateDB(bc, author, statedb, header, tx, round, status, isFinish)
	d1 = time.Since(t1)

	return
}
