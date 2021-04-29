package cmpreuse

import (
	"encoding/json"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"math/big"
	"time"
	"unsafe"
)

// codeHashEquivalent compare two common.Hash
func codeHashEquivalent(l common.Hash, r common.Hash) bool {
	if l == cmptypes.EmptyCodeHash {
		l = cmptypes.NilCodeHash
	}
	if r == cmptypes.EmptyCodeHash {
		r = cmptypes.NilCodeHash
	}
	return l == r
}

// CheckRChain check rw.Rchain
func CheckRChain(rw *cache.RWRecord, bc core.ChainContext, header *types.Header, debugDiff bool) bool {
	if rw.RChain.Coinbase != nil && header.Coinbase != *rw.RChain.Coinbase {
		if debugDiff {
			log.Info("RChain Coinbase miss", "pve", *rw.RChain.Coinbase, "now", header.Coinbase)
		}
		return false
	}
	if rw.RChain.Timestamp != nil && header.Time != rw.RChain.Timestamp.Uint64() {
		if debugDiff {
			log.Info("RChain Timestamp miss", "pve", rw.RChain.Timestamp, "now", header.Time)
		}
		return false
	}
	if rw.RChain.Number != nil && header.Number.Cmp(rw.RChain.Number) != 0 {
		if debugDiff {
			log.Info("RChain Number miss", "pve", rw.RChain.Number, "now", header.Number)
		}
		return false
	}
	if rw.RChain.Difficulty != nil && header.Difficulty.Cmp(rw.RChain.Difficulty) != 0 {
		if debugDiff {
			log.Info("RChain Difficulty miss", "pve", rw.RChain.Difficulty, "now", header.Difficulty)
		}
		return false
	}
	if rw.RChain.GasLimit != nil && header.GasLimit != *rw.RChain.GasLimit {
		if debugDiff {
			log.Info("RChain GasLimit miss", "pve", rw.RChain.GasLimit, "now", header.GasLimit)
		}
		return false
	}
	if rw.RChain.Blockhash != nil {
		currentNum := header.Number.Uint64()
		getHashFn := core.GetHashFn(header, bc)
		for num, blkHash := range rw.RChain.Blockhash {
			if num > (currentNum-257) && num < currentNum {
				if blkHash != getHashFn(num) {
					if debugDiff {
						log.Info("RChain block hash miss", "pve", blkHash, "now", getHashFn(num))
					}
					return false
				}
			} else if blkHash.Big().Cmp(common.Big0) != 0 {
				if debugDiff {
					log.Info("RChain block hash miss", "pve", blkHash, "now", "0")
				}
				return false
			}
		}
	}
	return true
}

// CheckRState check rw.Rstate
func CheckRState(rw *cache.RWRecord, statedb *state.StateDB, debugDiff bool, noDeltaCheck bool) bool {
	// turn off rw mode temporarily
	isRWMode := statedb.IsRWMode()
	statedb.SetRWMode(false)
	defer statedb.SetRWMode(isRWMode)
	res := true
	for addr, rstate := range rw.RState {
		if noDeltaCheck {
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

	// Make sure Suicide is called after other mod operations to ensure that a state_object is always there
	// Otherwise, Suicide will fail to mark the state_object as suicided
	// Without deferring, it might cause trouble when a new account gets created and suicides within the same transaction
	// In that case, as we apply write sets out of order, if Suicide is applied before other mod operations to the account,
	// the account will be left in the state causing mismatch.
	return wstate.Suicided != nil && *wstate.Suicided
}

func ApplyDelta(statedb *state.StateDB, rw *cache.RWRecord, WDeltas map[common.Address]*cache.WStateDelta, mixStatus *cmptypes.MixStatus, abort func() bool) {
	var suicideAddr []common.Address
	wroteDetailCount := 0

	for addr, wstate := range rw.WState {
		if abort != nil && abort() {
			return
		}
		wStateDelta, ok := WDeltas[addr]
		if ok {
			if ApplyWStateDelta(statedb, addr, wstate, wStateDelta) {
				suicideAddr = append(suicideAddr, addr)
			}
		} else {
			if ApplyWState(statedb, addr, wstate) {
				suicideAddr = append(suicideAddr, addr)
			}
		}
		wroteDetailCount += wstate.ItemCount

	}
	for _, addr := range suicideAddr {
		if abort != nil && abort() {
			return
		}
		statedb.Suicide(addr)
	}

	mixStatus.WriteDetailCount = wroteDetailCount
	mixStatus.WriteDetailTotal = wroteDetailCount
}

// ApplyWStates apply write state for each address in wstate
func ApplyWStates(statedb *state.StateDB, rw *cache.RWRecord, mixStatus *cmptypes.MixStatus, abort func() bool) {
	var suicideAddr []common.Address
	wroteDetailCount := 0

	for addr, wstate := range rw.WState {
		if abort != nil && abort() {
			return
		}
		if ApplyWState(statedb, addr, wstate) {
			suicideAddr = append(suicideAddr, addr)
		}
		wroteDetailCount += wstate.ItemCount
	}
	for _, addr := range suicideAddr {
		if abort != nil && abort() {
			return
		}
		statedb.Suicide(addr)
	}
	if mixStatus != nil {
		mixStatus.WriteDetailCount = wroteDetailCount
		mixStatus.WriteDetailTotal = wroteDetailCount
	}
}

func MixApplyObjState(statedb *state.StateDB, rw *cache.RWRecord, wobjectRefs cache.WObjectWeakRefMap,
	txPreplay *cache.TxPreplay, mixStatus *cmptypes.MixStatus, abort func() bool) {
	var suicideAddr []common.Address
	WDeltas := txPreplay.PreplayResults.DeltaWrites
	var wroteObjCount, wroteDetailCount, writeDetailTotal int
	for addr, wstate := range rw.WState {
		if abort != nil && abort() {
			return
		}
		addressWrote := false
		_, depHit := mixStatus.DepHitAddrMap[addr]
		if depHit {
			if wref, refOk := wobjectRefs.GetMatchedRef(addr, txPreplay); refOk {
				if objectHolder, hOk := txPreplay.PreplayResults.GetAndDeleteHolder(wref); hOk {
					statedb.ApplyStateObject(objectHolder.Obj)
					wroteObjCount += 1
					addressWrote = true
				}
			}
		} else {
			if mixStatus.MixHitType == cmptypes.AllDeltaHit || mixStatus.MixHitType == cmptypes.PartialDeltaHit {
				wStateDelta, ok := WDeltas[addr]
				if ok {
					if ApplyWStateDelta(statedb, addr, wstate, wStateDelta) {
						suicideAddr = append(suicideAddr, addr)
					}
					wroteDetailCount += wstate.ItemCount
					addressWrote = true
				} else {
					panic("there should be a delta state")
				}
			}
		}

		if !addressWrote {
			if ApplyWState(statedb, addr, wstate) {
				suicideAddr = append(suicideAddr, addr)
			}
			wroteDetailCount += wstate.ItemCount
		}
		writeDetailTotal += wstate.ItemCount
	}

	for _, addr := range suicideAddr {
		if abort != nil && abort() {
			return
		}
		statedb.Suicide(addr)
	}
	mixStatus.WriteDepCount = wroteObjCount
	mixStatus.WriteDetailCount = wroteDetailCount
	mixStatus.WriteDetailTotal = writeDetailTotal
}

func (reuse *Cmpreuse) mixCheck(txPreplay *cache.TxPreplay, bc *core.BlockChain, statedb *state.StateDB, header *types.Header, abort func() bool,
	blockPre *cache.BlockPre, isBlockProcess bool, cfg *vm.Config) (*cache.PreplayResult, *cmptypes.MixStatus, *cmptypes.PreplayResTrieNode,
	interface{}, bool, bool) {
	trie := txPreplay.PreplayResults.MixTree
	cmpReuseChecking := cfg.MSRAVMSettings.CmpReuseChecking
	setDelta := !cfg.MSRAVMSettings.NoTrace && !cfg.MSRAVMSettings.SingleFuture
	res, mixHitStatus, missNode, missValue, isAbort, ok := SearchMixTree(trie, statedb, bc, header, abort, false,
		isBlockProcess, txPreplay.PreplayResults.IsExternalTransfer && setDelta)
	if ok {
		//res := trieNode.Round.(*cache.PreplayResult)

		if blockPre != nil && blockPre.ListenTimeNano < res.TimestampNano {
			if isBlockProcess {
				log.Warn("mixhit fail caused by BLOCKPRE", "txhash", txPreplay.TxHash.Hex())
			}
			return nil, mixHitStatus, missNode, missValue, isAbort, false
		}
		if cmpReuseChecking {
			if isBlockProcess {
				//log.Info("checking")
			}
			if !CheckRChain(res.RWrecord, bc, header, false) {
				if isBlockProcess { // TODO: debug code
					log.Warn("depcheck unmatch due to chain info", "txhash", txPreplay.TxHash.Hex())
					CheckRChain(res.RWrecord, bc, header, true)
				}
				br, _ := json.Marshal(res)
				log.Warn("unmatch round ", "round", string(br))
				panic("depcheck unmatch due to chain info: txhash=" + txPreplay.TxHash.Hex())

			}
			noDeltaCheck := mixHitStatus.MixHitType != cmptypes.AllDeltaHit && mixHitStatus.MixHitType != cmptypes.PartialDeltaHit
			if !CheckRState(res.RWrecord, statedb, true, noDeltaCheck) {
				log.Warn("****************************************************")
				statusjs, _ := json.Marshal(mixHitStatus)
				log.Warn("unmatched status", "status", string(statusjs))
				SearchMixTree(trie, statedb, bc, header, abort, true, isBlockProcess, txPreplay.PreplayResults.IsExternalTransfer)

				br, _ := json.Marshal(res)
				dab, _ := json.Marshal(mixHitStatus.DepHitAddrMap)
				log.Warn("unmatch due to state info", "blockHash", header.Hash(), "blockprocess",
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

// Deprecated
func (reuse *Cmpreuse) depCheck(txPreplay *cache.TxPreplay, bc *core.BlockChain, statedb *state.StateDB, header *types.Header, abort func() bool,
	blockPre *cache.BlockPre, isBlockProcess bool, cfg *vm.Config) (res *cache.PreplayResult, isAbort bool, ok bool) {
	//txPreplay.Mu.Lock()
	//defer txPreplay.Mu.Unlock()

	trie := txPreplay.PreplayResults.ReadDepTree
	trieNode, isAbort, ok := SearchTree(trie, statedb, bc, header, abort, isBlockProcess, false)
	if ok {
		res := trieNode.Round.(*cache.PreplayResult)

		if blockPre != nil && blockPre.ListenTimeNano < res.TimestampNano {
			return nil, isAbort, false
		}
		if cfg.MSRAVMSettings.CmpReuseChecking {
			if !CheckRChain(res.RWrecord, bc, header, false) {
				if isBlockProcess { // TODO: debug code
					log.Warn("depcheck unmatch due to chain info", "txhash", txPreplay.TxHash.Hex())
				}
				CheckRChain(res.RWrecord, bc, header, true)
				panic("depcheck unmatch due to chain info: txhash=" + txPreplay.TxHash.Hex())
				return nil, isAbort, false
			} else if !CheckRState(res.RWrecord, statedb, true, true) {
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
					preTxResId := adv.Value.(*cmptypes.TxResID)
					if preTxResId == nil {
						log.Info("show depcheck match read dep (no tx changed)", "readaddress", addr)
					} else {
						log.Info("show depcheck match read dep", "readaddress", addr, "lastTxhash", preTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID)
						preTxPreplay := reuse.MSRACache.PeekTxPreplayInProcessForDebug(*preTxResId.Txhash)
						if preTxPreplay != nil {
							preTxPreplay.RLockRound()
							if preTxRound, okk := preTxPreplay.PeekRound(preTxResId.RoundID); okk {
								pretxbs, _ := json.Marshal(preTxRound)
								//if jerr != nil {
								log.Info("@@@@@@ pre tx info", "pretxRound", string(pretxbs))
								//}
							} else {
								log.Warn("no this tx round")
							}
							preTxPreplay.RUnlockRound()
						} else {
							log.Warn("no this tx preplay")
						}

					}
					if statedb.GetTxDepByAccount(addr) == nil {
						log.Info("show read dep is DEFAULT_TXRESID", "readaddress", addr)

					} else {
						newTxResId, ok := statedb.GetTxDepByAccount(addr).(*cmptypes.TxResID)
						if !ok {
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
						if *preTxResId.Hash() == *newTxResId.Hash() {
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

func (reuse *Cmpreuse) trieCheck(txPreplay *cache.TxPreplay, bc *core.BlockChain, statedb *state.StateDB, header *types.Header, abort func() bool,
	blockPre *cache.BlockPre, isBlockProcess bool, cfg *vm.Config) (*cache.PreplayResult, uint64, bool, bool) {
	trie := txPreplay.PreplayResults.RWRecordTrie
	trieNode, isAbort, ok := SearchTree(trie, statedb, bc, header, abort, isBlockProcess, false)

	if ok {
		res := trieNode.Round.(*cache.PreplayResult)
		if blockPre != nil && blockPre.ListenTimeNano < res.TimestampNano {
			return nil, trie.RoundCount, false, false
		}
		if cfg.MSRAVMSettings.CmpReuseChecking {
			if !CheckRChain(res.RWrecord, bc, header, false) {
				log.Warn("depcheck unmatch due to chain info", "txhash", txPreplay.TxHash.Hex())
				CheckRChain(res.RWrecord, bc, header, true)
				panic("depcheck unmatch due to chain info: txhash=" + txPreplay.TxHash.Hex())
				return nil, trie.RoundCount, isAbort, false
			} else if !CheckRState(res.RWrecord, statedb, true, true) {
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
					preTxResId := adv.Value.(*cmptypes.TxResID)
					if preTxResId == nil {
						log.Info("show depcheck match read dep (no tx changed)", "readaddress", addr)
					} else {
						log.Info("show depcheck match read dep", "readaddress", addr, "lastTxhash", preTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID)
						preTxPreplay := reuse.MSRACache.PeekTxPreplayInProcessForDebug(*preTxResId.Txhash)
						if preTxPreplay != nil {
							preTxPreplay.RLockRound()
							if preTxRound, okk := preTxPreplay.PeekRound(preTxResId.RoundID); okk {
								pretxbs, _ := json.Marshal(preTxRound)
								//if jerr != nil {
								log.Info("@@@@@@ pre tx info", "pretxRound", string(pretxbs))
								//}
							} else {
								log.Warn("no this tx round")
							}
							preTxPreplay.RUnlockRound()
						} else {
							log.Warn("no this tx preplay")
						}

					}
					newTxResId := statedb.GetTxDepByAccount(addr).(*cmptypes.TxResID)
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
					if *preTxResId.Hash() == *newTxResId.Hash() {
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

				return nil, trie.RoundCount, false, false
			}
		}

		return res, trie.RoundCount, isAbort, true
	}
	return nil, trie.RoundCount, isAbort, false
}

func (reuse *Cmpreuse) deltaCheck(txPreplay *cache.TxPreplay, bc *core.BlockChain, statedb *state.StateDB, header *types.Header, abort func() bool,
	blockPre *cache.BlockPre, isBlockProcess bool) (res *cache.PreplayResult, isAbort bool, ok bool) {
	trie := txPreplay.PreplayResults.DeltaTree
	trieNode, isAbort, ok := SearchTree(trie, statedb, bc, header, abort, isBlockProcess, false)
	if ok {
		res := trieNode.Round.(*cache.PreplayResult)
		if blockPre != nil && blockPre.ListenTimeNano < res.TimestampNano {
			return nil, false, false
		}
		return res, isAbort, true
	}
	return nil, isAbort, false
}

func (reuse *Cmpreuse) traceCheck(txPreplay *cache.TxPreplay, statedb *state.StateDB, header *types.Header, abort func() bool, isBlockProcess bool,
	getHashFunc vm.GetHashFunc, precompiles map[common.Address]vm.PrecompiledContract, cfg *vm.Config) *TraceTrieSearchResult {

	if getHashFunc == nil || precompiles == nil {
		panic("Should not be nil!")
	}

	var trie *TraceTrie
	t := txPreplay.PreplayResults.TraceTrie
	if t != nil {
		trie = t.(*TraceTrie)
	}

	if trie != nil {
		return trie.SearchTraceTrie(statedb, header, getHashFunc, precompiles, abort, cfg, isBlockProcess, reuse.MSRACache)
	}
	return nil
}

// Deprecated:  getValidRW get valid transaction from tx preplay cache
func (reuse *Cmpreuse) getValidRW(txPreplay *cache.TxPreplay, bc *core.BlockChain, statedb *state.StateDB, header *types.Header,
	blockPre *cache.BlockPre, abort func() bool) (*cache.PreplayResult, bool, bool, bool, int64) {
	//txPreplay.Mu.Lock()
	//defer txPreplay.Mu.Unlock()

	var leastOne, isAbort bool
	var cmpCnt int64
	rwrecordKeys := txPreplay.PreplayResults.RWrecords.Keys()

	// rwrecordKeys are from oldest to newest. iterate them reversely
	for i := len(rwrecordKeys) - 1; i >= 0; i-- {
		if abort != nil && abort() {
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
		if CheckRChain(rwrecord, bc, header, false) && CheckRState(rwrecord, statedb, false, true) {
			// update the priority of the correct rwrecord simply
			txPreplay.PreplayResults.RWrecords.Get(rawKey)
			return rwrecord.Round, true, leastOne, isAbort, cmpCnt
		}
	}
	return nil, false, leastOne, isAbort, cmpCnt
}

// setStateDB use RW to update stateDB, add logs, benefit miner and deduct gas from the pool
func (reuse *Cmpreuse) setStateDB(bc core.ChainContext, author *common.Address, statedb *state.StateDB, header *types.Header,
	tx *types.Transaction, txPreplay *cache.TxPreplay, round *cache.PreplayResult, status *cmptypes.ReuseStatus,
	isBlockProcess bool, cfg *vm.Config,
	sr *TraceTrieSearchResult, abort func() bool) {
	if status.BaseStatus != cmptypes.Hit {
		panic("wrong call")
	}
	if isBlockProcess && !cfg.MSRAVMSettings.ParallelizeReuse {
		if statedb.IgnoreJournalEntry != false {
			panic("Unmatched ignoreJournalEntry")
		}
		statedb.IgnoreJournalEntry = true
	}

	if !statedb.IsRWMode() {
		switch {
		case status.HitType == cmptypes.DeltaHit:
			ApplyDelta(statedb, round.RWrecord, txPreplay.PreplayResults.DeltaWrites, status.MixStatus, abort)
		case status.HitType == cmptypes.TrieHit:
			ApplyWStates(statedb, round.RWrecord, status.MixStatus, abort)
		case status.HitType == cmptypes.MixHit:
			MixApplyObjState(statedb, round.RWrecord, round.WObjectWeakRefs, txPreplay, status.MixStatus, abort)
		case status.HitType == cmptypes.TraceHit:
			isAbort, txResMap := sr._ApplyStores(txPreplay, status.TraceStatus, abort, cfg.MSRAVMSettings.NoOverMatching, isBlockProcess)
			if isAbort {
				return
			} else {
				status.TraceTrieHitAddrs = txResMap
			}
		default:
			panic("there is a unexpected hit type")
		}

	} else {
		if status.HitType == cmptypes.TraceHit {
			txPreplay.PreplayResults.TraceTrieMu.Lock()
			isAbort, txResMap := sr.ApplyStores(txPreplay, status.TraceStatus, abort, cfg.MSRAVMSettings.NoOverMatching, isBlockProcess)
			txPreplay.PreplayResults.TraceTrieMu.Unlock()
			if isAbort {
				return
			} else {
				status.TraceTrieHitAddrs = txResMap
			}
		} else if status.HitType == cmptypes.DeltaHit {
			ApplyDelta(statedb, round.RWrecord, txPreplay.PreplayResults.DeltaWrites, status.MixStatus, abort)
		} else {
			ApplyWStates(statedb, round.RWrecord, status.MixStatus, abort)
		}
	}

	if abort != nil && abort() {
		return
	}
	// Add Logs
	if status.HitType != cmptypes.TraceHit {
		for _, receiptLog := range round.Receipt.Logs {
			if abort != nil && abort() {
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

	if abort != nil && abort() {
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

	if isBlockProcess && !cfg.MSRAVMSettings.ParallelizeReuse {
		if statedb.IgnoreJournalEntry != true {
			panic("Unmatched ignoreJournalEntry")
		}
		statedb.IgnoreJournalEntry = false
	}

}

func (reuse *Cmpreuse) reuseTransaction(bc *core.BlockChain, author *common.Address, gp *core.GasPool, statedb *state.StateDB,
	header *types.Header, getHashFunc vm.GetHashFunc, precompiles map[common.Address]vm.PrecompiledContract, tx *types.Transaction,
	blockPre *cache.BlockPre, abort func() bool, isBlockProcess bool, fullPreplay bool, cfg *vm.Config) (
	status *cmptypes.ReuseStatus, round *cache.PreplayResult, d0 time.Duration, d1 time.Duration) {

	detailedTime := cfg.MSRAVMSettings.DetailTime

	var sr *TraceTrieSearchResult
	status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Undefined}
	roundCount := uint64(0)

	var t0 time.Time
	if detailedTime {
		t0 = time.Now()
	}

	var txPreplay *cache.TxPreplay
	if isBlockProcess {
		var tryFailed bool
		txPreplay, tryFailed = reuse.MSRACache.TryPeekPreplay(tx.Hash())
		if tryFailed {
			status.TryPeekFailed = true
		}
	} else {
		txPreplay = reuse.MSRACache.GetTxPreplay(tx.Hash())
	}
	if (isBlockProcess && cfg.MSRAVMSettings.NoReuse) || txPreplay == nil {
		if detailedTime {
			d0 = time.Since(t0)
		}
		//status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.NoPreplay}
		status.BaseStatus = cmptypes.NoPreplay //no cache, quit compete
		return
	}

	var lockCount int
	var tryHoldLock = func(mu *cmptypes.SimpleTryLock) (hold bool) {
		if isBlockProcess {
			if !mu.TryLock() {
				lockCount++
				return false
			}
		} else {
			mu.Lock()
		}
		return true
	}

	if fullPreplay && !isBlockProcess && cfg.MSRAVMSettings.EnableReuseTracer && !cfg.MSRAVMSettings.NoTrace {
		//if !txPreplay.PreplayResults.IsExternalTransfer {
		if tryHoldLock(txPreplay.PreplayResults.TraceTrieMu) {
			traceNotExist := txPreplay.PreplayResults.TraceTrie == nil
			if txPreplay.PreplayResults.ForcedTraceTrieGeneration {
				traceNotExist = false
			} else {
				txPreplay.PreplayResults.ForcedTraceTrieGeneration = true
			}
			txPreplay.PreplayResults.TraceTrieMu.Unlock()
			if traceNotExist {
				// force miss to run real apply if there is no trace
				// otherwise if the result is hit, no trace will be created at all
				if detailedTime {
					d0 = time.Since(t0)
				}
				//status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.NoPreplay}
				status.BaseStatus = cmptypes.NoPreplay //no cache, quit compete
				return
			}
		}
		//}
	}

	traceTrieChecked := false
	traceIsOff := cfg.MSRAVMSettings.SingleFuture || cfg.MSRAVMSettings.NoTrace

	if traceIsOff || cfg.MSRAVMSettings.AddFastPath {
		if status.BaseStatus != cmptypes.Undefined {
			panic("BaseStatus should be undefined before the first check")
		}
		if cfg.MSRAVMSettings.NoOverMatching {
			round, roundCount, d0 = reuse.tryTrieCheck(tryHoldLock, txPreplay, bc, statedb, header, abort, blockPre, isBlockProcess, cfg, t0, status)
			if status.BaseStatus == cmptypes.Unknown {
				status.RoundCount = roundCount
				return
			}
		} else {
			round, d0 = reuse.tryMixCheck(tryHoldLock, txPreplay, bc, statedb, header, abort, blockPre, isBlockProcess, cfg, status, t0)
			if status.BaseStatus == cmptypes.Miss || status.BaseStatus == cmptypes.Unknown {
				return
			}
		}
	}

	if !traceIsOff {
		if status.BaseStatus == cmptypes.Undefined {
			traceTrieChecked, sr, d0, round = reuse.tryTraceCheck(tryHoldLock, txPreplay, statedb, header, abort, isBlockProcess, getHashFunc, precompiles, cfg, status, t0)
			if status.BaseStatus == cmptypes.Miss || status.BaseStatus == cmptypes.Unknown {
				status.RoundCount = roundCount
				return
			}
		}

		if !cfg.MSRAVMSettings.AddFastPath {
			if !cfg.MSRAVMSettings.NoMemoization || !isBlockProcess {
				if status.BaseStatus == cmptypes.Undefined {
					round, roundCount, d0 = reuse.tryTrieCheck(tryHoldLock, txPreplay, bc, statedb, header, abort, blockPre, isBlockProcess, cfg, t0, status)
					if status.BaseStatus == cmptypes.Unknown {
						status.RoundCount = roundCount
						return
					}
				}
			}
		}
	}

	if status.BaseStatus == cmptypes.Undefined {
		if isBlockProcess && lockCount == 4 {
			d0 = time.Since(t0)
			//status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Unknown, AbortStage: cmptypes.TxPreplayLock}
			status.BaseStatus = cmptypes.Unknown
			status.AbortStage = cmptypes.TxPreplayLock
			return
		} else {
			// for debug purpose
			if false { //isBlockProcess {
				mixRoundCount := uint64(0)
				deltaCount := uint64(0)
				detailCount := uint64(0)
				if txPreplay.PreplayResults.MixTree != nil {
					mixRoundCount = txPreplay.PreplayResults.MixTree.RoundCount
				}
				if txPreplay.PreplayResults.DeltaTree != nil {
					deltaCount = txPreplay.PreplayResults.DeltaTree.RoundCount
				}
				if txPreplay.PreplayResults.RWRecordTrie != nil {
					detailCount = txPreplay.PreplayResults.RWRecordTrie.RoundCount
				}
				log.Info("NoMatchMiss", "tx", tx.Hash().Hex(), "gasLimit", tx.Gas(), "mix", mixRoundCount,
					"traceExist", txPreplay.PreplayResults.TraceTrie != nil, "traceChecked", traceTrieChecked,
					"delta", deltaCount,
					"detail", detailCount,
					"external", txPreplay.PreplayResults.IsExternalTransfer,
					"time", txPreplay.Timestamp.String())
			}
			d0 = time.Since(t0)
			//status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Miss, MissType: cmptypes.NoMatchMiss, MissNode: missNode, MissValue: missValue}
			status.BaseStatus = cmptypes.Miss
			status.MissType = cmptypes.NoMatchMiss
			//status.MissNode = missNode
			//status.MissValue = missValue
			return
		}
	}

	if status.BaseStatus != cmptypes.Hit {
		panic("Should be Hit!")
	}

	// Clear up the miss info in case that we have TraceTrie hit and (MixTree miss)
	status.MissNode = nil
	status.MissValue = nil

	if err := gp.SubGas(round.Receipt.GasUsed); err != nil {
		//status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Fail} // fail for gas limit reach, quit compete
		status.BaseStatus = cmptypes.Fail
		return
	}

	t1 := time.Now()
	reuse.setStateDB(bc, author, statedb, header, tx, txPreplay, round, status, isBlockProcess, cfg, sr, abort)
	d1 = time.Since(t1)

	if isBlockProcess && cfg.MSRAVMSettings.CmpReuseChecking {
		//sjs, _ := json.Marshal(status)
		//rjs,_:= json.Marshal(round)
		//log.Info("status", "tx", tx.Hash().Hex(), "status", string(sjs))
	}

	return
}

func (reuse *Cmpreuse) tryTraceCheck(tryHoldLock func(mu *cmptypes.SimpleTryLock) (hold bool),
	txPreplay *cache.TxPreplay, statedb *state.StateDB, header *types.Header, abort func() bool, isBlockProcess bool,
	getHashFunc vm.GetHashFunc, precompiles map[common.Address]vm.PrecompiledContract, cfg *vm.Config,
	status *cmptypes.ReuseStatus, t0 time.Time) (traceTrieChecked bool, sr *TraceTrieSearchResult, d0 time.Duration, round *cache.PreplayResult) {
	if tryHoldLock(txPreplay.PreplayResults.TraceTrieMu) {
		traceTrieChecked = true
		sr = reuse.traceCheck(txPreplay, statedb, header, abort, isBlockProcess, getHashFunc, precompiles, cfg)
		txPreplay.PreplayResults.TraceTrieMu.Unlock()

		if sr != nil {
			status.TraceStatus = sr.TraceStatus
			if sr.hit {
				d0 = time.Since(t0)
				//status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Hit, HitType: cmptypes.TraceHit, TraceStatus: sr.TraceStatus}
				status.BaseStatus = cmptypes.Hit
				status.HitType = cmptypes.TraceHit
				round = sr.round // sr.GetAnyRound()
			} else if sr.aborted {
				d0 = time.Since(t0)
				//status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Unknown, AbortStage: cmptypes.TraceCheck}
				status.BaseStatus = cmptypes.Unknown
				status.AbortStage = cmptypes.TraceCheck
			} else {
				d0 = time.Since(t0)
				//status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Miss, MissType: cmptypes.TraceMiss, MissNode: missNode, MissValue: missValue}
				status.BaseStatus = cmptypes.Miss
				status.MissType = cmptypes.TraceMiss
			}
		}
	} else {
		cache.LockCount[1] ++
	}
	return
}

func (reuse *Cmpreuse) tryMixCheck(tryHoldLock func(mu *cmptypes.SimpleTryLock) (hold bool),
	txPreplay *cache.TxPreplay, bc *core.BlockChain, statedb *state.StateDB, header *types.Header, abort func() bool,
	blockPre *cache.BlockPre, isBlockProcess bool, cfg *vm.Config, status *cmptypes.ReuseStatus,
	t0 time.Time) (round *cache.PreplayResult, d0 time.Duration) {
	if tryHoldLock(txPreplay.PreplayResults.MixTreeMu) {
		var missNode *cmptypes.PreplayResTrieNode
		var missValue interface{}
		var isAbort, ok bool
		var mixStatus *cmptypes.MixStatus
		round, mixStatus, missNode, missValue, isAbort, ok = reuse.mixCheck(txPreplay, bc, statedb, header, abort, blockPre, isBlockProcess,
			cfg)
		txPreplay.PreplayResults.MixTreeMu.Unlock()

		status.MixStatus = mixStatus
		status.MissNode = missNode
		status.MissValue = missValue

		if ok {
			d0 = time.Since(t0)
			//status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Hit, HitType: cmptypes.MixHit, MixHitStatus: mixStatus}
			status.BaseStatus = cmptypes.Hit
			status.HitType = cmptypes.MixHit
			status.MixStatus.HitRoundID = round.RoundID
			status.MixStatus.HitRWRecordID = uintptr(unsafe.Pointer(round.RWrecord))
		} else if isAbort {
			d0 = time.Since(t0)
			//status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Unknown, AbortStage: cmptypes.MixCheck} // abort before hit or miss
			status.BaseStatus = cmptypes.Unknown
			status.AbortStage = cmptypes.MixCheck
		}
		//else {
		//	status.BaseStatus = cmptypes.Miss
		//	status.MissType = cmptypes.MixMiss
		//}
	} else {
		cache.LockCount[0] ++
	}
	return
}

func (reuse *Cmpreuse) tryTrieCheck(tryHoldLock func(mu *cmptypes.SimpleTryLock) (hold bool),
	txPreplay *cache.TxPreplay, bc *core.BlockChain, statedb *state.StateDB, header *types.Header,
	abort func() bool, blockPre *cache.BlockPre, isBlockProcess bool, cfg *vm.Config, t0 time.Time,
	status *cmptypes.ReuseStatus) (round *cache.PreplayResult, roundCount uint64,  d0 time.Duration) {
	if tryHoldLock(txPreplay.PreplayResults.RWRecordTrieMu) {
		var isAbort, ok bool
		round, roundCount, isAbort, ok = reuse.trieCheck(txPreplay, bc, statedb, header, abort, blockPre, isBlockProcess, cfg)
		txPreplay.PreplayResults.RWRecordTrieMu.Unlock()

		if ok {
			d0 = time.Since(t0)
			//status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Hit, HitType: cmptypes.TrieHit, MixHitStatus: mixStatus}
			status.BaseStatus = cmptypes.Hit
			status.HitType = cmptypes.TrieHit
			mixStatus := &cmptypes.MixStatus{MixHitType: cmptypes.NotMixHit, HitDepNodeCount: 0, UnhitDepNodeCount: -1,
				DetailCheckedCount: len(round.RWrecord.ReadDetail.ReadDetailSeq), BasicDetailCount: len(round.RWrecord.ReadDetail.ReadDetailSeq),
				RoundCount: roundCount}

			status.MixStatus = mixStatus

			if false && isBlockProcess {
				mixbs, _ := json.Marshal(mixStatus)
				roundbs, _ := json.Marshal(round)
				log.Warn("Mixhit miss !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", "tx", txPreplay.TxHash.Hex(), "mixstatus", string(mixbs), "round", string(roundbs))
				txPreplay.PreplayResults.MixTreeMu.Lock()
				SearchMixTree(txPreplay.PreplayResults.MixTree, statedb, bc, header, func() bool { return false }, true, isBlockProcess, txPreplay.PreplayResults.IsExternalTransfer)
				txPreplay.PreplayResults.MixTreeMu.Unlock()
				log.Warn(". . . . . . . . . . . . . . . . . . . . . . . . . ")
				SearchTree(txPreplay.PreplayResults.RWRecordTrie, statedb, bc, header, func() bool { return false }, isBlockProcess, true)
				formertxsbs, _ := json.Marshal(statedb.ProcessedTxs)
				log.Warn("former tx", "former txs", string(formertxsbs))

				depMatch := true
				for _, adv := range round.ReadDepSeq {
					if adv.AddLoc.Field != cmptypes.Dependence {
						continue
					}
					addr := adv.AddLoc.Address
					preTxResId := adv.Value.(*cmptypes.TxResID)
					if preTxResId == nil {
						log.Info("show depcheck match read dep (no tx changed)", "readaddress", addr)
					} else {
						log.Info("show depcheck match read dep", "readaddress", addr, "lastTxhash", preTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID)
						preTxPreplay := reuse.MSRACache.PeekTxPreplayInProcessForDebug(*preTxResId.Txhash)
						if preTxPreplay != nil {
							preTxPreplay.RLockRound()
							if preTxRound, okk := preTxPreplay.PeekRound(preTxResId.RoundID); okk {
								pretxbs, _ := json.Marshal(preTxRound)
								//if jerr != nil {
								log.Info("@@@@@@ pre tx info", "pretxRound", string(pretxbs))
								//}
							} else {
								log.Warn("no this tx round")
							}
							preTxPreplay.RUnlockRound()
						} else {
							log.Warn("no this tx preplay")
						}

					}
					newTxResId := statedb.GetTxDepByAccount(addr).(*cmptypes.TxResID)
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
					if *preTxResId.Hash() == *newTxResId.Hash() {
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
			//status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Unknown, AbortStage: cmptypes.TrieCheck} // abort before hit or miss
			status.BaseStatus = cmptypes.Unknown
			status.AbortStage = cmptypes.TrieCheck
		}
	} else {
		cache.LockCount[3] ++
	}
	return
}

func (reuse *Cmpreuse) tryDeltaCheck(tryHoldLock func(mu *cmptypes.SimpleTryLock) (hold bool),
	txPreplay *cache.TxPreplay, bc *core.BlockChain, statedb *state.StateDB, header *types.Header, abort func() bool,
	blockPre *cache.BlockPre, t0 time.Time, status *cmptypes.ReuseStatus, isBlockProcess bool) (round *cache.PreplayResult, d0 time.Duration) {
	if tryHoldLock(txPreplay.PreplayResults.DeltaTreeMu) {
		var isAbort, ok bool
		round, isAbort, ok = reuse.deltaCheck(txPreplay, bc, statedb, header, abort, blockPre, isBlockProcess)
		txPreplay.PreplayResults.DeltaTreeMu.Unlock()

		if ok {
			d0 = time.Since(t0)
			status.BaseStatus = cmptypes.Hit
			status.HitType = cmptypes.DeltaHit

			checkedNode := 3
			if len(round.RWrecord.ReadDetail.ReadDetailSeq) == 5 {
				checkedNode = 4
			}

			mixStatus := &cmptypes.MixStatus{MixHitType: cmptypes.NotMixHit, HitDepNodeCount: 0, UnhitDepNodeCount: -1,
				DetailCheckedCount: checkedNode, BasicDetailCount: len(round.RWrecord.ReadDetail.ReadDetailSeq)}
			status.MixStatus = mixStatus
		} else if isAbort {
			d0 = time.Since(t0)
			//status = &cmptypes.ReuseStatus{BaseStatus: cmptypes.Unknown, AbortStage: cmptypes.DeltaCheck} // abort before hit or miss
			status.BaseStatus = cmptypes.Unknown
			status.AbortStage = cmptypes.DeltaCheck
		} else {
			if false && txPreplay.PreplayResults.IsExternalTransfer {
				log.Warn("zhongxin: delta miss !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", "txhash", txPreplay.TxHash.Hex())
			}
		}
	} else {
		cache.LockCount[2] ++
	}
	return
}
