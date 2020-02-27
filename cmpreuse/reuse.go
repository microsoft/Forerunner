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

// CheckRState check rw.Rstate
func CheckRState(rw *cache.RWRecord, statedb *state.StateDB, debugDiff bool) bool {
	// turn off rw mode temporarily
	isRWMode := statedb.IsRWMode()
	statedb.SetRWMode(false)
	defer statedb.SetRWMode(isRWMode)
	res := true
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
						log.Info("RState Storage miss", "addr", addr, "pos", k.Hex(), "pve", v.Big(), "now", statedb.GetState(addr, k).Big())
						res = false
						continue
					}
					return false
				}
			}
		}
		if rstate.CommittedStorage != nil {
			for k, v := range rstate.CommittedStorage {
				if statedb.GetCommittedState(addr, k) != v {
					if debugDiff {
						log.Info("RState CommittedStorage miss", "addr", addr, "pos", k.Hex(), "pve", v.Big(), "now", statedb.GetCommittedState(addr, k).Big())
						res = false
						continue
					}
					return false
				}
			}
		}
	}
	return res
}

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

func (reuse *Cmpreuse) depCheck(txPreplay *cache.TxPreplay, bc core.ChainContext, statedb *state.StateDB,
	header *types.Header, blockPre *cache.BlockPre, isBlockProcess bool) (*cache.PreplayResult, bool) {
	//txPreplay.Mu.Lock()
	//defer txPreplay.Mu.Unlock()

	trie := txPreplay.PreplayResults.ReadDepTree
	trieNode, ok := SearchAccDep(trie, statedb, bc, header)
	if ok {
		res := trieNode.Round.(*cache.PreplayResult)
		if res == nil {
			panic(">>>>>>>>>>>>>> trieCheck found nil round PreplayRes")
		}
		if blockPre != nil && blockPre.ListenTimeNano < res.TimestampNano {
			return nil, false
		}
		//if !CheckRChain(res.RWrecord, bc, header) {
		//	if isBlockProcess { // TODO: debug code
		//		log.Warn("depcheck unmatch due to chain info", "txhash", txPreplay.TxHash.Hex())
		//	}
		//	panic("depcheck unmatch due to chain info: txhash=" + txPreplay.TxHash.Hex())
		//	return nil, false
		//} else if !CheckRState(res.RWrecord, statedb, true) {
		//	log.Warn("depcheck unmatch due to state info", "blockHash", header.Hash(), "blockprocess", isBlockProcess, "txhash", txPreplay.TxHash.Hex())
		//	br, _ := json.Marshal(res)
		//	log.Warn("unmatch round ", "round", string(br))
		//
		//	formertxsbs, _ := json.Marshal(statedb.ProcessedTxs)
		//	log.Warn("former tx", "former txs", string(formertxsbs))
		//
		//	currentReadDep := statedb.GetTxDepsByAccounts(res.RWrecord.ReadDetail.ReadAddress)
		//	depMatch := true
		//	for _, adv := range res.ReadDeps {
		//		if adv.AddLoc.Field != cmptypes.Dependence {
		//			continue
		//		}
		//		addr := adv.AddLoc.Address
		//		preTxResId := adv.Value.(*cmptypes.ChangedBy).LastTxResID
		//		if preTxResId == nil {
		//			log.Info("show depcheck match read dep (no tx changed)", "readaddress", addr)
		//		} else {
		//			log.Info("show depcheck match read dep", "readaddress", addr, "lastTxhash", preTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID)
		//			preTxPreplay := reuse.MSRACache.GetTxPreplay(*preTxResId.Txhash)
		//			if preTxPreplay != nil {
		//				preTxRound, okk := preTxPreplay.GetRound(preTxResId.RoundID)
		//				if okk {
		//					pretxbs, _ := json.Marshal(preTxRound)
		//					//if jerr != nil {
		//					log.Info("@@@@@@ pre tx info", "pretxRound", string(pretxbs))
		//					//}
		//				} else {
		//					log.Warn("no this tx round")
		//				}
		//			} else {
		//				log.Warn("no this tx preplay")
		//			}
		//
		//		}
		//		newTxResId := currentReadDep[addr].LastTxResID
		//		if newTxResId == nil {
		//			log.Info("show read dep (no tx changed)", "readaddress", addr)
		//		} else {
		//			log.Info("show read dep", "readaddress", addr, "lastTxhash", newTxResId.Txhash.Hex(), "preRoundID", newTxResId.RoundID)
		//		}
		//		//if preTxResId == nil {
		//		//	if newTxResId == nil {
		//		//		continue
		//		//	} else {
		//		//		depMatch = false
		//		//		log.Warn("preTxResId is nil and newTxResId is not", "curHash", newTxResId.Hash(),
		//		//			"curTxhash", newTxResId.Txhash.Hex(), "curRoundID", newTxResId.RoundID)
		//		//		continue
		//		//	}
		//		//} else {
		//		//	if newTxResId == nil {
		//		//		depMatch = false
		//		//
		//		//		log.Warn("newTxResId is nil and preTxResId is not", "preHash", preTxResId.Hash(),
		//		//			"preTxhash", preTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID, )
		//		//		continue
		//		//	}
		//		//}
		//		//if preTxResId.Hash().Hex() == newTxResId.Hash().Hex() {
		//		//	if !(preTxResId.Txhash.Hex() == newTxResId.Txhash.Hex() && preTxResId.RoundID == newTxResId.RoundID) {
		//		//		depMatch = false
		//		//		log.Warn("!! TxResID hash conflict: hash same; content diff", "preHash", preTxResId.Hash(), "curHash", newTxResId.Hash(),
		//		//			"preTxhash", preTxResId.Txhash.Hex(), "curTxhash", newTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID, "curRoundID", newTxResId.RoundID)
		//		//	}
		//		//} else {
		//		//	depMatch = false
		//		//	if preTxResId.Txhash.Hex() == newTxResId.Txhash.Hex() && preTxResId.RoundID == newTxResId.RoundID {
		//		//		log.Warn("!!!!! TxResID hash : hash diff; content same", "preHash", preTxResId.Hash(), "curHash", newTxResId.Hash(),
		//		//			"preTxhash", preTxResId.Txhash.Hex(), "curTxhash", newTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID, "curRoundID", newTxResId.RoundID)
		//		//	} else {
		//		//		log.Warn("!!>> read dep hash and content diff <<!!", "preTxhash", preTxResId.Txhash.Hex(), "curTxhash", newTxResId.Txhash.Hex(),
		//		//			"preRoundID", preTxResId.RoundID, "curRoundID", newTxResId.RoundID)
		//		//	}
		//		//}
		//	}
		//	if !depMatch {
		//		panic("search res wrong!!!!!!!!!!!!!!!!!!!!!!")
		//	}
		//
		//	log.Warn("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
		//	panic("depcheck unmatch due to state info: txhash=" + txPreplay.TxHash.Hex())
		//
		//	return nil, false
		//}
		return res, true
	} else {
		return nil, false
	}
}

func (reuse *Cmpreuse) trieCheckRState(txPreplay *cache.TxPreplay, bc core.ChainContext, db *state.StateDB, header *types.Header,
	blockPre *cache.BlockPre) (*cache.PreplayResult, bool) {
	//txPreplay.Mu.Lock()
	//defer txPreplay.Mu.Unlock()

	trie := txPreplay.PreplayResults.RWRecordTrie
	trieNode, ok := SearchPreplayRes(trie, db, bc, header)
	if ok {
		res := trieNode.RWRecord.GetPreplayRes().(*cache.PreplayResult)
		if res == nil {
			panic(">>>>>>>>>>>>>> trieCheck found nil round PreplayRes")
		}
		if blockPre != nil && blockPre.ListenTimeNano < res.TimestampNano {
			return nil, false
		}

		return res, true
	} else {
		return nil, false
	}
}

// getValidRW get valid transaction from tx preplay cache
func (reuse *Cmpreuse) getValidRW(txPreplay *cache.TxPreplay, bc core.ChainContext, statedb *state.StateDB, header *types.Header,
	blockPre *cache.BlockPre, abort func() bool) (*cache.PreplayResult, bool, bool, bool, int64) {
	//txPreplay.Mu.Lock()
	//defer txPreplay.Mu.Unlock()

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
	tx *types.Transaction, round *cache.PreplayResult, status *cmptypes.ReuseStatus, abort func() bool) {

	//if status.BaseStatus == cmptypes.Hit && (status.HitType == cmptypes.FastHit) && !statedb.IsRWMode() {
	if false && status.BaseStatus == cmptypes.Hit && status.HitType == cmptypes.DepHit && !statedb.IsRWMode() {
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
	header *types.Header, tx *types.Transaction, blockPre *cache.BlockPre, isFinish func() bool, isBlockProcess bool) (status *cmptypes.ReuseStatus,
	round *cache.PreplayResult, cmpCnt int64, d0 time.Duration, d1 time.Duration) {

	var ok, leastOne, abort bool

	t0 := time.Now()
	txPreplay := reuse.MSRACache.GetTxPreplay(tx.Hash())
	if txPreplay == nil || txPreplay.PreplayResults.RWrecords.Len() == 0 {
		status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.NoPreplay} // no cache, quit compete
		return
	}
	txPreplay.Mu.Lock()

	round, ok = reuse.depCheck(txPreplay, bc, statedb, header, blockPre, isBlockProcess)
	if ok {
		d0 = time.Since(t0)
		if round.RWrecord == nil {
			panic(" > > > > > > > > > > > > > > depcheck return nil rwrecord")
		}
		cmpCnt = 1
		status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Hit, HitType: cmptypes.DepHit}
	} else {
		round, ok = reuse.trieCheckRState(txPreplay, bc, statedb, header, blockPre)
		if ok {
			d0 = time.Since(t0)
			cmpCnt = 1
			status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Hit, HitType: cmptypes.TrieHit}

		} else {
			round, ok, leastOne, abort, cmpCnt = reuse.getValidRW(txPreplay, bc, statedb, header, blockPre, isFinish)
			d0 = time.Since(t0)
			if ok {
				//if isBlockProcess {
				//	// TODO : debug code
				//	rid := round.RoundID
				//	rids := txPreplay.PreplayResults.RWRecordTrie.RoundIds
				//	if rids[rid] {
				//		log.Warn(">>>>>>>>>> round in trie but is not found")
				//		bs, _ := json.Marshal(round.RWrecord.ReadDetail.ReadDetailSeq)
				//		log.Warn("round rwrecord", "tx", tx.Hash(), string(bs))
				//	} else {
				//		log.Warn("<<<<<<<<<< not in trie, found by iteration", "trie size",
				//			txPreplay.PreplayResults.RWRecordTrie.LeafCount, "record lru size", txPreplay.PreplayResults.RWrecords.Len())
				//	}
				//}
				status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Hit, HitType: cmptypes.IteraHit} // cache hit

			} else {
				switch {
				case abort:
					status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Unknown} // abort before hit or miss
				case leastOne:
					status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Miss, MissType: cmptypes.NoMatchMiss} // cache but result not match, quit compete
				default:
					status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Miss, MissType: cmptypes.NoInMiss} // cache but not in, quit compete
				}
				txPreplay.Mu.Unlock()

				return
			}
		}
	}

	txPreplay.Mu.Unlock()
	if err := gp.SubGas(round.Receipt.GasUsed); err != nil {
		status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Fail} // fail for gas limit reach, quit compete
		return
	}

	t1 := time.Now()
	reuse.setStateDB(bc, author, statedb, header, tx, round, status, isFinish)
	d1 = time.Since(t1)

	return
}
