package cmpreuse

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"math/big"
	"time"
)

var emptyCodeHash = crypto.Keccak256Hash(nil)
var nilCodeHash = common.Hash{}

// codeHashEquivalent compare two common.Hash
func codeHashEquivalent(l common.Hash, r common.Hash) bool {
	if l == emptyCodeHash {
		l = nilCodeHash
	}
	if r == emptyCodeHash {
		r = nilCodeHash
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

// ApplyWState apply one write state for one address, return whether this account is suicided.
func ApplyWStateDelta(statedb *state.StateDB, addr common.Address, wstate *state.WriteState, wstateDelta *cache.WStateDelta) bool {

	if wstate.Nonce != nil {
		statedb.SetNonce(addr, *wstate.Nonce)
	}
	if wstate.Balance != nil {
		if wstateDelta.Balance != nil {
			statedb.SetBalance(addr, new(big.Int).Add(statedb.GetBalance(addr), wstateDelta.Balance))
		} else {
			statedb.SetBalance(addr, wstate.Balance)
		}
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
func ApplyWDelta(statedb *state.StateDB, rw *cache.RWRecord, abort func() bool) {
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

func ApplyDelta(statedb *state.StateDB, rw *cache.RWRecord, abort func() bool) {
	var suicideAddr []common.Address
	for addr, wstate := range rw.WState {
		if abort() {
			return
		}
		wStateDelta, ok := rw.WStateDelta[addr]
		if ok {
			if ApplyWStateDelta(statedb, addr, wstate, wStateDelta) {
				suicideAddr = append(suicideAddr, addr)
			}
		} else {
			if ApplyWState(statedb, addr, wstate) {
				suicideAddr = append(suicideAddr, addr)
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

func ApplyDeltaWithObj(statedb *state.StateDB, rw *cache.RWRecord, wobject state.ObjectMap, mixStatus *cmptypes.MixHitStatus, abort func() bool) {
	var suicideAddr []common.Address
	for addr, wstate := range rw.WState {
		if abort() {
			return
		}
		wStateDelta, ok := rw.WStateDelta[addr]
		if ok {
			if ApplyWStateDelta(statedb, addr, wstate, wStateDelta) {
				suicideAddr = append(suicideAddr, addr)
			}
		} else {
			wObj, objOk := wobject[addr]
			if objOk {
				statedb.ApplyStateObject(wObj)
				wobject[addr] = nil
			} else {
				if ApplyWState(statedb, addr, wstate) {
					suicideAddr = append(suicideAddr, addr)
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

func MixApplyObjState(statedb *state.StateDB, rw *cache.RWRecord, wobject state.ObjectMap, mixStatus *cmptypes.MixHitStatus, abort func() bool) {
	var suicideAddr []common.Address
	for addr, object := range wobject {
		if abort() {
			return
		}
		_, depHit := mixStatus.DepHitAddrMap[addr]
		if object != nil && depHit {
			statedb.ApplyStateObject(object)
			wobject[addr] = nil
		} else {
			wstate, ok := rw.WState[addr]
			if ok && ApplyWState(statedb, addr, wstate) {
				suicideAddr = append(suicideAddr, addr)
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

func (reuse *Cmpreuse) mixCheck(txPreplay *cache.TxPreplay, bc core.ChainContext, statedb *state.StateDB,
	header *types.Header, blockPre *cache.BlockPre, abort func() bool, isBlockProcess bool, cmpReuseChecking bool) (
	*cache.PreplayResult, *cmptypes.MixHitStatus, *cmptypes.PreplayResTrieNode, interface{}, bool, bool) {
	//txPreplay.Mu.Lock()
	//defer txPreplay.Mu.Unlock()

	trie := txPreplay.PreplayResults.MixTree
	res, mixHitStatus, missNode, missValue, isAbort, ok := SearchMixTree(trie, statedb, bc, header, abort, false, isBlockProcess)
	if ok {
		//res := trieNode.Round.(*cache.PreplayResult)

		if blockPre != nil && blockPre.ListenTimeNano < res.TimestampNano {
			if isBlockProcess {
				log.Warn("mixhit fail caused by BLOCKPRE", "txhash", txPreplay.TxHash.Hex())
			}
			return nil, mixHitStatus, missNode, missValue, isAbort, false
		}
		if cmpReuseChecking {
			if !CheckRChain(res.RWrecord, bc, header) {
				if isBlockProcess { // TODO: debug code
					log.Warn("depcheck unmatch due to chain info", "txhash", txPreplay.TxHash.Hex())
				}
				br, _ := json.Marshal(res)
				log.Warn("unmatch round ", "round", string(br))
				panic("depcheck unmatch due to chain info: txhash=" + txPreplay.TxHash.Hex())

			} else if !CheckRState(res.RWrecord, statedb, true) {
				log.Warn("****************************************************")
				SearchMixTree(trie, statedb, bc, header, abort, true, isBlockProcess)

				br, _ := json.Marshal(res)
				dab, _ := json.Marshal(mixHitStatus.DepHitAddrMap)
				log.Warn("depcheck unmatch due to state info", "blockHash", header.Hash(), "blockprocess",
					isBlockProcess, "txhash", txPreplay.TxHash.Hex(), "depMatchedAddr", string(dab), "round", string(br))

				formertxsbs, _ := json.Marshal(statedb.ProcessedTxs)
				log.Warn("former tx", "former txs", string(formertxsbs))

				log.Warn("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
				panic("depcheck unmatch due to state info: txhash=" + txPreplay.TxHash.Hex())

			}
		}
		if mixHitStatus.MixHitType == cmptypes.PartialHit {
			// assert  len(mixHitStatus.DepHitAddr)>0
			headUmatched := 0
			for _, alv := range res.ReadDepSeq {
				if alv.AddLoc.Address == mixHitStatus.DepHitAddr[0] {
					break
				} else if alv.AddLoc.Field == cmptypes.Dependence {
					headUmatched++
				}
			}
			//// TODO: debug log, need to remove
			//if isBlockProcess {
			//	if headUmatched == 1 {
			//		statedbChanged := []*cmptypes.ChangedBy{}
			//		for _, alv := range res.ReadDepSeq {
			//			if alv.AddLoc.Field == cmptypes.Dependence {
			//				changed := statedb.GetTxDepByAccount(alv.AddLoc.Address)
			//				statedbChanged = append(statedbChanged, changed)
			//			}
			//		}
			//		scs, _ := json.Marshal(statedbChanged)
			//		resbs, _ := json.Marshal(res)
			//		log.Info("Partial hit head=1", "txhash", txPreplay.TxHash.Hex(), "statechanged", string(scs), "round", string(resbs))
			//	}
			//}

			mixHitStatus.DepUnmatchedInHead = headUmatched
		}

		return res, mixHitStatus, missNode, missValue, isAbort, true
	} else {
		return nil, mixHitStatus, missNode, missValue, isAbort, false
	}
}

func (reuse *Cmpreuse) depCheck(txPreplay *cache.TxPreplay, bc core.ChainContext, statedb *state.StateDB,
	header *types.Header, blockPre *cache.BlockPre, abort func() bool, isBlockProcess bool, cfg *vm.Config) (
	res *cache.PreplayResult, isAbort bool, ok bool) {
	//txPreplay.Mu.Lock()
	//defer txPreplay.Mu.Unlock()

	trie := txPreplay.PreplayResults.ReadDepTree
	trieNode, isAbort, ok := SearchTree(trie, statedb, bc, header, abort, false)
	if ok {
		res := trieNode.Round.(*cache.PreplayResult)

		if blockPre != nil && blockPre.ListenTimeNano < res.TimestampNano {
			return nil, isAbort, false
		}
		if cfg.MSRAVMSettings.CmpReuseChecking {
			if !CheckRChain(res.RWrecord, bc, header) {
				if isBlockProcess { // TODO: debug code
					log.Warn("depcheck unmatch due to chain info", "txhash", txPreplay.TxHash.Hex())
				}
				panic("depcheck unmatch due to chain info: txhash=" + txPreplay.TxHash.Hex())
				return nil, isAbort, false
			} else if !CheckRState(res.RWrecord, statedb, true) {
				log.Warn("depcheck unmatch due to state info", "blockHash", header.Hash(), "blockprocess", isBlockProcess, "txhash", txPreplay.TxHash.Hex())
				br, _ := json.Marshal(res)
				log.Warn("unmatch round ", "round", string(br))

				formertxsbs, _ := json.Marshal(statedb.ProcessedTxs)
				log.Warn("former tx", "former txs", string(formertxsbs))

				depMatch := true
				for _, adv := range res.ReadDepSeq {
					if adv.AddLoc.Field != cmptypes.Dependence {
						continue
					}
					addr := adv.AddLoc.Address
					preTxResId := adv.Value.(*cmptypes.ChangedBy).LastTxResID
					if preTxResId == nil {
						log.Info("show depcheck match read dep (no tx changed)", "readaddress", addr)
					} else {
						log.Info("show depcheck match read dep", "readaddress", addr, "lastTxhash", preTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID)
						preTxPreplay := reuse.MSRACache.GetTxPreplay(*preTxResId.Txhash)
						if preTxPreplay != nil {
							preTxRound, okk := preTxPreplay.GetRound(preTxResId.RoundID)
							if okk {
								pretxbs, _ := json.Marshal(preTxRound)
								//if jerr != nil {
								log.Info("@@@@@@ pre tx info", "pretxRound", string(pretxbs))
								//}
							} else {
								log.Warn("no this tx round")
							}
						} else {
							log.Warn("no this tx preplay")
						}

					}
					newTxResId := statedb.GetTxDepByAccount(addr).LastTxResID
					if newTxResId == nil {
						log.Info("show read dep (no tx changed)", "readaddress", addr)
					} else {
						log.Info("show read dep", "readaddress", addr, "lastTxhash", newTxResId.Txhash.Hex(), "preRoundID", newTxResId.RoundID)
					}
					if preTxResId == nil {
						if newTxResId == nil {
							continue
						} else {
							depMatch = false
							log.Warn("preTxResId is nil and newTxResId is not", "curHash", newTxResId.Hash(),
								"curTxhash", newTxResId.Txhash.Hex(), "curRoundID", newTxResId.RoundID)
							continue
						}
					} else {
						if newTxResId == nil {
							depMatch = false

							log.Warn("newTxResId is nil and preTxResId is not", "preHash", preTxResId.Hash(),
								"preTxhash", preTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID)
							continue
						}
					}
					if preTxResId.Hash().Hex() == newTxResId.Hash().Hex() {
						if !(preTxResId.Txhash.Hex() == newTxResId.Txhash.Hex() && preTxResId.RoundID == newTxResId.RoundID) {
							depMatch = false
							log.Warn("!! TxResID hash conflict: hash same; content diff", "preHash", preTxResId.Hash(), "curHash", newTxResId.Hash(),
								"preTxhash", preTxResId.Txhash.Hex(), "curTxhash", newTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID, "curRoundID", newTxResId.RoundID)
						}
					} else {
						depMatch = false
						if preTxResId.Txhash.Hex() == newTxResId.Txhash.Hex() && preTxResId.RoundID == newTxResId.RoundID {
							log.Warn("!!!!! TxResID hash : hash diff; content same", "preHash", preTxResId.Hash(), "curHash", newTxResId.Hash(),
								"preTxhash", preTxResId.Txhash.Hex(), "curTxhash", newTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID, "curRoundID", newTxResId.RoundID)
						} else {
							log.Warn("!!>> read dep hash and content diff <<!!", "preTxhash", preTxResId.Txhash.Hex(), "curTxhash", newTxResId.Txhash.Hex(),
								"preRoundID", preTxResId.RoundID, "curRoundID", newTxResId.RoundID)
						}
					}
				}
				if !depMatch {
					panic("search res wrong!!!!!!!!!!!!!!!!!!!!!!")
				}

				log.Warn("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
				panic("depcheck unmatch due to state info: txhash=" + txPreplay.TxHash.Hex())

				return nil, false, false
			}
		}
		return res, isAbort, true
	} else {
		return nil, isAbort, false
	}
}

func (reuse *Cmpreuse) trieCheck(txPreplay *cache.TxPreplay, bc core.ChainContext, statedb *state.StateDB, header *types.Header, abort func() bool,
	blockPre *cache.BlockPre, isBlockProcess bool, cfg *vm.Config) (res *cache.PreplayResult, isAbort bool, ok bool) {
	trie := txPreplay.PreplayResults.RWRecordTrie
	trieNode, isAbort, ok := SearchTree(trie, statedb, bc, header, abort, false)
	
	if ok {
		res := trieNode.Round.(*cache.PreplayResult)
		if blockPre != nil && blockPre.ListenTimeNano < res.TimestampNano {
			return nil, false, false
		}
		if cfg.MSRAVMSettings.CmpReuseChecking {
			if !CheckRChain(res.RWrecord, bc, header) {
				log.Warn("depcheck unmatch due to chain info", "txhash", txPreplay.TxHash.Hex())

				panic("depcheck unmatch due to chain info: txhash=" + txPreplay.TxHash.Hex())
				return nil, isAbort, false
			} else if !CheckRState(res.RWrecord, statedb, true) {
				log.Warn("depcheck unmatch due to state info", "blockHash", header.Hash(), "blockprocess", isBlockProcess, "txhash", txPreplay.TxHash.Hex())
				br, _ := json.Marshal(res)
				log.Warn("unmatch round ", "round", string(br))

				formertxsbs, _ := json.Marshal(statedb.ProcessedTxs)
				log.Warn("former tx", "former txs", string(formertxsbs))

				depMatch := true
				for _, adv := range res.ReadDepSeq {
					if adv.AddLoc.Field != cmptypes.Dependence {
						continue
					}
					addr := adv.AddLoc.Address
					preTxResId := adv.Value.(*cmptypes.ChangedBy).LastTxResID
					if preTxResId == nil {
						log.Info("show depcheck match read dep (no tx changed)", "readaddress", addr)
					} else {
						log.Info("show depcheck match read dep", "readaddress", addr, "lastTxhash", preTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID)
						preTxPreplay := reuse.MSRACache.GetTxPreplay(*preTxResId.Txhash)
						if preTxPreplay != nil {
							preTxRound, okk := preTxPreplay.GetRound(preTxResId.RoundID)
							if okk {
								pretxbs, _ := json.Marshal(preTxRound)
								//if jerr != nil {
								log.Info("@@@@@@ pre tx info", "pretxRound", string(pretxbs))
								//}
							} else {
								log.Warn("no this tx round")
							}
						} else {
							log.Warn("no this tx preplay")
						}

					}
					newTxResId := statedb.GetTxDepByAccount(addr).LastTxResID
					if newTxResId == nil {
						log.Info("show read dep (no tx changed)", "readaddress", addr)
					} else {
						log.Info("show read dep", "readaddress", addr, "lastTxhash", newTxResId.Txhash.Hex(), "preRoundID", newTxResId.RoundID)
					}
					if preTxResId == nil {
						if newTxResId == nil {
							continue
						} else {
							depMatch = false
							log.Warn("preTxResId is nil and newTxResId is not", "curHash", newTxResId.Hash(),
								"curTxhash", newTxResId.Txhash.Hex(), "curRoundID", newTxResId.RoundID)
							continue
						}
					} else {
						if newTxResId == nil {
							depMatch = false

							log.Warn("newTxResId is nil and preTxResId is not", "preHash", preTxResId.Hash(),
								"preTxhash", preTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID)
							continue
						}
					}
					if preTxResId.Hash().Hex() == newTxResId.Hash().Hex() {
						if !(preTxResId.Txhash.Hex() == newTxResId.Txhash.Hex() && preTxResId.RoundID == newTxResId.RoundID) {
							depMatch = false
							log.Warn("!! TxResID hash conflict: hash same; content diff", "preHash", preTxResId.Hash(), "curHash", newTxResId.Hash(),
								"preTxhash", preTxResId.Txhash.Hex(), "curTxhash", newTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID, "curRoundID", newTxResId.RoundID)
						}
					} else {
						depMatch = false
						if preTxResId.Txhash.Hex() == newTxResId.Txhash.Hex() && preTxResId.RoundID == newTxResId.RoundID {
							log.Warn("!!!!! TxResID hash : hash diff; content same", "preHash", preTxResId.Hash(), "curHash", newTxResId.Hash(),
								"preTxhash", preTxResId.Txhash.Hex(), "curTxhash", newTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID, "curRoundID", newTxResId.RoundID)
						} else {
							log.Warn("!!>> read dep hash and content diff <<!!", "preTxhash", preTxResId.Txhash.Hex(), "curTxhash", newTxResId.Txhash.Hex(),
								"preRoundID", preTxResId.RoundID, "curRoundID", newTxResId.RoundID)
						}
					}
				}
				if !depMatch {
					panic("search res wrong!!!!!!!!!!!!!!!!!!!!!!")
				}

				log.Warn("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
				panic("depcheck unmatch due to state info: txhash=" + txPreplay.TxHash.Hex())

				return nil, false, false
			}
		}

		return res, isAbort, true
	}
	return nil, isAbort, false
}

func (reuse *Cmpreuse) deltaCheck(txPreplay *cache.TxPreplay, bc core.ChainContext, db *state.StateDB, header *types.Header, abort func() bool,
	blockPre *cache.BlockPre) (res *cache.PreplayResult, isAbort bool, ok bool) {
	trie := txPreplay.PreplayResults.DeltaTree
	trieNode, isAbort, ok := SearchTree(trie, db, bc, header, abort, false)
	if ok {
		res := trieNode.Round.(*cache.PreplayResult)
		if blockPre != nil && blockPre.ListenTimeNano < res.TimestampNano {
			return nil, false, false
		}
		return res, isAbort, true
	}
	return nil, isAbort, false
}

func (reuse *Cmpreuse) traceCheck(txPreplay *cache.TxPreplay, db *state.StateDB, header *types.Header, getHashFunc vm.GetHashFunc, precompiles map[common.Address]vm.PrecompiledContract, abort func() bool,
	blockPre *cache.BlockPre, isBlockProcess bool, tracerCheck bool) *TraceTrieSearchResult {
	if !isBlockProcess {
		return nil
	} else {
		if getHashFunc == nil || precompiles == nil {
			panic("Should not be nil!")
		}
	}
	var trie *TraceTrie
	t := txPreplay.PreplayResults.TraceTrie
	if t != nil {
		trie = t.(*TraceTrie)
	}

	if trie != nil {
		sr, _, _ := trie.SearchTraceTrie(db, header, getHashFunc, precompiles, abort, tracerCheck, reuse.MSRACache)
		return sr
	}
	return nil
}

// Deprecated:  getValidRW get valid transaction from tx preplay cache
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
	tx *types.Transaction, round *cache.PreplayResult, status *cmptypes.ReuseStatus, sr *TraceTrieSearchResult, abort func() bool) {
	if status.BaseStatus != cmptypes.Hit {
		panic("wrong call")
	}
	if !statedb.IsRWMode() {
		switch {
		case status.HitType == cmptypes.DeltaHit:
			ApplyDelta(statedb, round.RWrecord, abort)
		case status.HitType == cmptypes.TrieHit:
			ApplyWStates(statedb, round.RWrecord, abort)
		case status.HitType == cmptypes.MixHit:
			MixApplyObjState(statedb, round.RWrecord, round.WObjects, status.MixHitStatus, abort)
		case status.HitType == cmptypes.TraceHit:
			isAbort, txResMap := sr.ApplyStores(abort)
			if isAbort {
				return
			} else {
				status.TraceTrieHitAddrs = txResMap
			}
		default:
			panic("there is a unexpected hit type")
		}

	} else {
		cmptypes.MyAssert(status.HitType != cmptypes.TraceHit)
		if status.HitType == cmptypes.DeltaHit {
			ApplyDelta(statedb, round.RWrecord, abort)
		} else {
			ApplyWStates(statedb, round.RWrecord, abort)
		}
	}

	if abort() {
		return
	}
	// Add Logs
	if status.HitType != cmptypes.TraceHit {
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
	header *types.Header, getHashFunc vm.GetHashFunc, precompiles map[common.Address]vm.PrecompiledContract, tx *types.Transaction, blockPre *cache.BlockPre, abort func() bool, isBlockProcess bool, cfg *vm.Config) (status *cmptypes.ReuseStatus,
	round *cache.PreplayResult, d0 time.Duration, d1 time.Duration) {

	var ok, isAbort bool
	var mixStatus *cmptypes.MixHitStatus
	var missNode *cmptypes.PreplayResTrieNode
	var missValue interface{}
	var sr *TraceTrieSearchResult

	t0 := time.Now()
	txPreplay := reuse.MSRACache.GetTxPreplay(tx.Hash())
	if txPreplay == nil {
		status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.NoPreplay} // no cache, quit compete
		return
	}
	txPreplay.Mu.Lock()
	defer txPreplay.Mu.Unlock()

	if status == nil {
		round, mixStatus, missNode, missValue, isAbort, ok = reuse.mixCheck(txPreplay, bc, statedb, header, blockPre, abort, isBlockProcess, cfg.MSRAVMSettings.CmpReuseChecking)
		if ok {
			d0 = time.Since(t0)
			status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Hit, HitType: cmptypes.MixHit, MixHitStatus: mixStatus}
		} else if isAbort {
			d0 = time.Since(t0)
			status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Unknown, AbortStage: cmptypes.MixCheck} // abort before hit or miss
			return
		}
	}

	if status == nil {

		if isBlockProcess {
			sr = reuse.traceCheck(txPreplay, statedb, header, getHashFunc, precompiles, abort, blockPre, isBlockProcess, cfg.MSRAVMSettings.CmpReuseChecking)

			if sr != nil && sr.hit {
				d0 = time.Since(t0)
				status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Hit, HitType: cmptypes.TraceHit}
				round = sr.GetAnyRound()
			} else if sr != nil && sr.aborted {
				d0 = time.Since(t0)
				status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Unknown, AbortStage: cmptypes.TraceCheck}
				return
			}
		}
	}

	if status == nil {
		if sr != nil {
			d0 = time.Since(t0)
			status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Miss, MissType: cmptypes.NoMatchMiss,
				MissNode: missNode, MissValue: missValue}
			return
		}
	}

	if status == nil {
		if isBlockProcess {
			round, isAbort, ok = reuse.deltaCheck(txPreplay, bc, statedb, header, abort, blockPre)
			if ok {
				d0 = time.Since(t0)
				status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Hit, HitType: cmptypes.DeltaHit, MixHitStatus: mixStatus}
				// debug info
				//rss, _ := json.Marshal(status)
				//log.Info("reuse delta", "txhash", tx.Hash().Hex(), "status", string(rss))
			} else if isAbort {
				d0 = time.Since(t0)
				status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Unknown, AbortStage: cmptypes.DeltaCheck} // abort before hit or miss
				return
			}
		}
	}

	if status == nil {
		round, isAbort, ok = reuse.trieCheck(txPreplay, bc, statedb, header, abort, blockPre, isBlockProcess, cfg)
		if ok {
			d0 = time.Since(t0)
			status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Hit, HitType: cmptypes.TrieHit, MixHitStatus: mixStatus}

			// why trie hit instead of mixhit:
			//       over matching dep leads to a wrong way; the round found by trie might miss all deps
			if false && isBlockProcess {
				mixbs, _ := json.Marshal(mixStatus)
				roundbs, _ := json.Marshal(round)
				log.Warn("Mixhit miss !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", "tx", txPreplay.TxHash.Hex(), "mixstatus", string(mixbs), "round", string(roundbs))
				SearchMixTree(txPreplay.PreplayResults.MixTree, statedb, bc, header, func() bool { return false }, true, isBlockProcess)
				log.Warn(". . . . . . . . . . . . . . . . . . . . . . . . . ")
				SearchTree(txPreplay.PreplayResults.RWRecordTrie, statedb, bc, header, func() bool { return false }, true)
				formertxsbs, _ := json.Marshal(statedb.ProcessedTxs)
				log.Warn("former tx", "former txs", string(formertxsbs))

				depMatch := true
				for _, adv := range round.ReadDepSeq {
					if adv.AddLoc.Field != cmptypes.Dependence {
						continue
					}
					addr := adv.AddLoc.Address
					preTxResId := adv.Value.(*cmptypes.ChangedBy).LastTxResID
					if preTxResId == nil {
						log.Info("show depcheck match read dep (no tx changed)", "readaddress", addr)
					} else {
						log.Info("show depcheck match read dep", "readaddress", addr, "lastTxhash", preTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID)
						preTxPreplay := reuse.MSRACache.GetTxPreplay(*preTxResId.Txhash)
						if preTxPreplay != nil {
							preTxRound, okk := preTxPreplay.PeekRound(preTxResId.RoundID)
							if okk {
								pretxbs, _ := json.Marshal(preTxRound)
								//if jerr != nil {
								log.Info("@@@@@@ pre tx info", "pretxRound", string(pretxbs))
								//}
							} else {
								log.Warn("no this tx round")
							}
						} else {
							log.Warn("no this tx preplay")
						}

					}
					newTxResId := statedb.GetTxDepByAccount(addr).LastTxResID
					if newTxResId == nil {
						log.Info("show read dep (no tx changed)", "readaddress", addr)
					} else {
						log.Info("show read dep", "readaddress", addr, "lastTxhash", newTxResId.Txhash.Hex(), "preRoundID", newTxResId.RoundID)
					}
					if preTxResId == nil {
						if newTxResId == nil {
							continue
						} else {
							depMatch = false
							log.Warn("preTxResId is nil and newTxResId is not", "curHash", newTxResId.Hash(),
								"curTxhash", newTxResId.Txhash.Hex(), "curRoundID", newTxResId.RoundID)
							continue
						}
					} else {
						if newTxResId == nil {
							depMatch = false

							log.Warn("newTxResId is nil and preTxResId is not", "preHash", preTxResId.Hash(),
								"preTxhash", preTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID)
							continue
						}
					}
					if preTxResId.Hash().Hex() == newTxResId.Hash().Hex() {
						if !(preTxResId.Txhash.Hex() == newTxResId.Txhash.Hex() && preTxResId.RoundID == newTxResId.RoundID) {
							depMatch = false
							log.Warn("!! TxResID hash conflict: hash same; content diff", "preHash", preTxResId.Hash(), "curHash", newTxResId.Hash(),
								"preTxhash", preTxResId.Txhash.Hex(), "curTxhash", newTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID, "curRoundID", newTxResId.RoundID)
						}
					} else {
						depMatch = false
						if preTxResId.Txhash.Hex() == newTxResId.Txhash.Hex() && preTxResId.RoundID == newTxResId.RoundID {
							log.Warn("!!!!! TxResID hash : hash diff; content same", "preHash", preTxResId.Hash(), "curHash", newTxResId.Hash(),
								"preTxhash", preTxResId.Txhash.Hex(), "curTxhash", newTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID, "curRoundID", newTxResId.RoundID)
						} else {
							log.Warn("!!>> read dep hash and content diff <<!!", "preTxhash", preTxResId.Txhash.Hex(), "curTxhash", newTxResId.Txhash.Hex(),
								"preRoundID", preTxResId.RoundID, "curRoundID", newTxResId.RoundID)
						}
					}
				}

				log.Warn("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++", "depmatch", depMatch)

				//panic("unsupposed match by trie hit")
			}
		} else if isAbort {
			d0 = time.Since(t0)
			status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Unknown, AbortStage: cmptypes.TrieCheck} // abort before hit or miss
			return
		}
	}

	if status == nil {
		d0 = time.Since(t0)
		status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Miss, MissType: cmptypes.NoMatchMiss,
			MissNode: missNode, MissValue: missValue}
		return
	}

	if status.BaseStatus != cmptypes.Hit {
		panic("Should be Hit!")
	}

	if err := gp.SubGas(round.Receipt.GasUsed); err != nil {
		status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Fail} // fail for gas limit reach, quit compete
		return
	}

	if isBlockProcess && cfg.MSRAVMSettings.CmpReuseChecking{
		sjs, _ := json.Marshal(status)
		log.Info("status", "tx", tx.Hash().Hex(), "status", string(sjs))
	}

	t1 := time.Now()
	reuse.setStateDB(bc, author, statedb, header, tx, round, status, sr, abort)
	d1 = time.Since(t1)
	return
}
