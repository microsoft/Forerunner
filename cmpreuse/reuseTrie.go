package cmpreuse

import (
	"encoding/json"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
)

func InsertRecord(trie *cmptypes.PreplayResTrie, round *cache.PreplayResult, blockNumber uint64) {
	record := round.RWrecord

	if blockNumber > trie.LatestBN {
		trie.LatestBN = blockNumber
	}
	if trie.LeafCount > 1000 {
		trie.Clear()
	}

	currentNode := trie.Root
	seqRecord := record.ReadDetail.ReadDetailSeq
	for _, addrFieldValue := range seqRecord {
		currentNode = insertNode(currentNode, addrFieldValue)
	}
	if currentNode.IsLeaf {
		// there is the same RWRecord before, assert they are the same
		// TODO debug code, need be removed
		//if currentNode.RWRecord.GetHash() != record.GetHash() {
		//	panic("the rwrecord should be the same")
		//}
	} else {
		currentNode.IsLeaf = true
		currentNode.RWRecord = record
		trie.LeafCount += 1
	}
	trie.RoundIds[round.RoundID] = true
}

func SearchPreplayRes(trie *cmptypes.PreplayResTrie, db *state.StateDB, bc core.ChainContext, header *types.Header, abort func() bool, ) (*cmptypes.PreplayResTrieNode, bool, bool) {
	return SearchTree(trie, db, bc, header, abort, false)
}

// return true is this round is inserted. false for this round is a repeated round
func InsertAccDep(trie *cmptypes.PreplayResTrie, round *cache.PreplayResult, blockNumber uint64, preBlockHash *common.Hash) bool {

	if trie.LeafCount > 1000 {
		trie.Clear()
	}
	//trie.ClearOld(3)
	trie.LatestBN = blockNumber

	//var topseq []*cmptypes.AddrLocValue
	//topseq = append(topseq, &cmptypes.AddrLocValue{AddLoc: &cmptypes.AddrLocation{Field: cmptypes.Number}, Value: blockNumber})
	//topseq = append(topseq, &cmptypes.AddrLocValue{AddLoc: &cmptypes.AddrLocation{Field: cmptypes.PreBlockHash}, Value: *preBlockHash})

	currentNode := trie.Root

	for _, readDep := range round.ReadDeps {
		//for _, readDep := range append(topseq, round.ReadDeps...) {
		currentNode = insertNode(currentNode, readDep)
	}

	trie.RoundIds[round.RoundID] = true
	if currentNode.IsLeaf {
		// there is the same RWRecord before, assert they are the same
		// TODO debug code
		//if currentNode.Round.(*cache.PreplayResult).RWrecord.GetHash() != record.GetHash() {
		//	oldroundbs, _ := json.Marshal(currentNode.Round.(*cache.PreplayResult))
		//	curroundbs, _ := json.Marshal(round)
		//	log.Warn("insert account dep tree rwrecord not same", "tx", round.TxHash.Hex(),
		//		"oldRound", string(oldroundbs), "curround", string(curroundbs))
		//
		//	if currentNode.Round.(*cache.PreplayResult).RWrecord.GetRChainHash() == record.GetRChainHash() {
		//		log.Warn("insert account dep tree rwrecord not same, rchain same", "tx", round.TxHash.Hex())
		//	} else {
		//		log.Warn("insert account dep tree rwrecord not same, rchain different")
		//
		//	}
		//	//log.Warn("==============================================================")
		//	//preReadDetail, _ := json.Marshal(currentNode.Round.(*cache.PreplayResult).RWrecord.ReadDetail)
		//	//newReadDetail, _ := json.Marshal(record.ReadDetail)
		//	//log.Warn("", "preReadDetail", string(preReadDetail))
		//	//log.Warn("", "newReadDetail", string(newReadDetail))
		//	//log.Warn("==============================================================")
		//	//for _, addr := range record.ReadDetail.ReadAddress {
		//	//	preTxResId := round.ReadDeps[addr].LastTxResID
		//	//	newTxResId := currentNode.Round.(*cache.PreplayResult).ReadDeps[addr].LastTxResID
		//	//	if preTxResId == nil {
		//	//		if newTxResId == nil {
		//	//			continue
		//	//		} else {
		//	//			log.Warn("preTxResId is nil and newTxResId is not", "curHash", newTxResId.Hash(),
		//	//				"curTxhash", newTxResId.Txhash.Hex(), "curRoundID", newTxResId.RoundID)
		//	//			continue
		//	//		}
		//	//	} else {
		//	//		if newTxResId == nil {
		//	//			log.Warn("newTxResId is nil and preTxResId is not", "preHash", preTxResId.Hash(),
		//	//				"preTxhash", preTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID, )
		//	//			continue
		//	//		}
		//	//	}
		//	//
		//	//	if preTxResId.Hash().Hex() == newTxResId.Hash().Hex() {
		//	//	} else {
		//	//		if preTxResId.Txhash.Hex() == newTxResId.Txhash.Hex() && preTxResId.RoundID == newTxResId.RoundID {
		//	//			log.Warn("!! TxResID hash conflict: hash same; content diff", "preHash", preTxResId.Hash(), "curHash", newTxResId.Hash(),
		//	//				"preTxhash", preTxResId.Txhash.Hex(), "curTxhash", newTxResId.Txhash.Hex(), "preRoundID", preTxResId.RoundID, "curRoundID", newTxResId.RoundID)
		//	//		} else {
		//	//			log.Warn("!!>> read dep hash and content diff <<!!", "preTxhash", preTxResId.Txhash.Hex(), "curTxhash", newTxResId.Txhash.Hex(),
		//	//				"preRoundID", preTxResId.RoundID, "curRoundID", newTxResId.RoundID)
		//	//		}
		//	//	}
		//	//}
		//	//log.Error("the rwrecord should be the same")
		//	panic("the rwrecord in dep tree should be the same")
		//	return
		//}
		return false
	} else {
		currentNode.IsLeaf = true
		currentNode.Round = round
		trie.LeafCount += 1
		return true
	}

}

func SearchAccDep(trie *cmptypes.PreplayResTrie, db *state.StateDB, bc core.ChainContext, header *types.Header, abort func() bool) (*cmptypes.PreplayResTrieNode, bool, bool) {
	return SearchTree(trie, db, bc, header, abort, false)
}

// only some of addresses's would inserted into detail check subtree,
const FixedDepCheckCount = 4

func InsertMixTree(trie *cmptypes.PreplayResTrie, round *cache.PreplayResult, blockNumber uint64, preBlockHash *common.Hash) {
	//if len(round.ReadDeps) > 10 {
	//	return
	//}
	if trie.LeafCount > 1000 {
		trie.Clear()
	}
	//trie.ClearOld(3)
	trie.LatestBN = blockNumber

	//var topseq []*cmptypes.AddrLocValue
	//topseq = append(topseq, &cmptypes.AddrLocValue{AddLoc: &cmptypes.AddrLocation{Field: cmptypes.Number}, Value: blockNumber})
	//topseq = append(topseq, &cmptypes.AddrLocValue{AddLoc: &cmptypes.AddrLocation{Field: cmptypes.PreBlockHash}, Value: *preBlockHash})
	detailSeq := round.RWrecord.ReadDetail.ReadDetailSeq

	currentNode := trie.Root

	//for _, blockDep := range topseq {
	//	currentNode = insertNode(currentNode, blockDep)
	//}

	hitDep := make(map[interface{}]bool)
	depCheckedAddr := make(map[common.Address]bool)
	checkedButNoHit := 0
	leafCount := new(int)

	insertDep2MixTree(currentNode, 0, round.ReadDeps, 0, detailSeq, hitDep, depCheckedAddr, checkedButNoHit, round, leafCount)

	//// TODO: clear top layer' DetailChild
	//topDetail := &cmptypes.PreplayResTrieNode{Children: make(map[interface{}]*cmptypes.PreplayResTrieNode)}
	//topNode := trie.Root
	//if !round.RWrecord.ReadDetail.IsBlockNumberSensitive {
	//	topNode.DetailChild = topDetail
	//} else {
	//	topNode = topNode.Children[blockNumber]
	//	topNode.DetailChild = topDetail
	//}
	//for _, detailAlv := range detailSeq {
	//	topDetail = insertNode(topDetail, detailAlv)
	//}
	//topDetail.IsLeaf = true
	//topDetail.Round = round

	trie.LeafCount += 1
	trie.RoundIds[round.RoundID] = true

	//if *leafCount > 100 {
	//	//rbs, _ := json.Marshal(round)
	//	depCheckedAddrbs, _ := json.Marshal(depCheckedAddr)
	//	log.Warn("Big leaf count", "tx", round.TxHash.Hex(), "leafCount", *leafCount, "addrCount", len(depCheckedAddr), "depChecked", string(depCheckedAddrbs))
	//}
}

func insertDep2MixTree(currentNode *cmptypes.PreplayResTrieNode, rIndex int, readDepSeq []*cmptypes.AddrLocValue, dIndex int,
	detailSeq []*cmptypes.AddrLocValue, hitDep map[interface{}]bool, depCheckedAddr map[common.Address]bool, noHit int, round *cache.PreplayResult, leafCount *int) {

	if rIndex >= len(readDepSeq) {
		for _, detailalv := range detailSeq[dIndex:] {
			if cmptypes.IsChainField(detailalv.AddLoc.Field) || hitDep[detailalv.AddLoc.Address] {
				continue
			}
			currentNode = insertNode(currentNode, detailalv)
		}
		*leafCount = *leafCount + 1
		if currentNode.IsLeaf {
		} else {
			currentNode.IsLeaf = true
			currentNode.Round = round
		}

		return
	}
	curDep := readDepSeq[rIndex]
	curAddr := curDep.AddLoc.Address
	child := currentNode

	if cmptypes.IsChainField(curDep.AddLoc.Field) {

		hitDep[curDep.AddLoc.Field] = true
		child = insertNode(currentNode, curDep)

		insertDep2MixTree(child, rIndex+1, readDepSeq, dIndex, detailSeq, hitDep, depCheckedAddr, noHit, round, leafCount)

		hitDep[curDep.AddLoc.Field] = false
	} else {
		// assert hitDep[curAddr] == false, depCheckedAddr[curAddr]= false
		//if hitDep[curAddr] || depCheckedAddr[curAddr] {
		//	rbs, _ := json.Marshal(round)
		//	hitdepbs, _ := json.Marshal(hitDep)
		//	depCheckedAddrbs, _ := json.Marshal(depCheckedAddr)
		//	log.Warn("insert dep wrong", "tx", round.TxHash.Hex(), "rIndex", rIndex, "dIndex", dIndex,
		//		"hitdep", string(hitdepbs), "depChecked", string(depCheckedAddrbs), "round", string(rbs))
		//
		//	panic("Insert dep check wrong")
		//}

		hitDep[curAddr] = true
		depCheckedAddr[curAddr] = true
		child = insertNode(currentNode, curDep)

		//insertDep2MixTree(child, rIndex+1, readDepSeq, dIndex, detailSeq, hitDep, depCheckedAddr, noHit, round)
		insertDetail2MixTree(child, rIndex+1, readDepSeq, dIndex, detailSeq, hitDep, depCheckedAddr, noHit, round, leafCount)

		hitDep[curAddr] = false

		if currentNode.DetailChild == nil {
			currentNode.DetailChild = &cmptypes.PreplayResTrieNode{Children: make(map[interface{}]*cmptypes.PreplayResTrieNode)}
		}

		insertDetail2MixTree(currentNode.DetailChild, rIndex+1, readDepSeq, dIndex, detailSeq, hitDep, depCheckedAddr, noHit+1, round, leafCount)

		depCheckedAddr[curAddr] = false
	}
}

func insertDetail2MixTree(currentNode *cmptypes.PreplayResTrieNode, rIndex int, readDep []*cmptypes.AddrLocValue, dIndex int,
	detailSeq []*cmptypes.AddrLocValue, hitDep map[interface{}]bool, depCheckedAddr map[common.Address]bool, noHit int, round *cache.PreplayResult, leafCount *int) {

	if dIndex >= len(detailSeq) {
		*leafCount = *leafCount + 1
		if currentNode.IsLeaf {
		} else {
			currentNode.IsLeaf = true
			currentNode.Round = round
		}
		return
	}
	detailRead := detailSeq[dIndex]

	if cmptypes.IsChainField(detailRead.AddLoc.Field) {
		if !hitDep[detailRead.AddLoc.Field] {
			// assert readDep[rIndex] is chainfield
			if detailRead.AddLoc.Field != cmptypes.Number && detailRead.AddLoc.Field != cmptypes.Blockhash {
				// Debug code
				//if rIndex >= len(readDep) || readDep[rIndex].AddLoc.Field != detailRead.AddLoc.Field {
				//	rbs, _ := json.Marshal(round)
				//	hitdepbs, _ := json.Marshal(hitDep)
				//	depCheckedAddrbs, _ := json.Marshal(depCheckedAddr)
				//	log.Warn("set mix tree", "tx", round.TxHash.Hex(), "rIndex", rIndex, "dIndex", dIndex,
				//		"hitdep", string(hitdepbs), "depChecked", string(depCheckedAddrbs), "round", string(rbs))
				//
				//	log.Error("chain dep and chain detail are not aligned")
				//	panic("NOT ALIGNED")
				//}
				rIndex++
			}
			currentNode = insertNode(currentNode, detailRead)
		}
		insertDetail2MixTree(currentNode, rIndex, readDep, dIndex+1, detailSeq, hitDep, depCheckedAddr, noHit, round, leafCount)
		return
	} else {
		detailReadAddr := detailRead.AddLoc.Address
		isNewAddr := !depCheckedAddr[detailReadAddr]
		// to reduce the node number, ifnoHit <= FixedDepCheckCount, only append detail node, regardless of this addr is not checked by deb // magic number
		if isNewAddr && noHit < FixedDepCheckCount && !(noHit > 0 && noHit <= FixedDepCheckCount && rIndex > FixedDepCheckCount+3) {
			insertDep2MixTree(currentNode, rIndex, readDep, dIndex, detailSeq, hitDep, depCheckedAddr, noHit, round, leafCount)
		} else {
			if !hitDep[detailReadAddr] {
				currentNode = insertNode(currentNode, detailRead)
			}
			if isNewAddr {
				//if hitDep[detailReadAddr] {
				//	rbs, _ := json.Marshal(round)
				//	hitdepbs, _ := json.Marshal(hitDep)
				//	depCheckedAddrbs, _ := json.Marshal(depCheckedAddr)
				//	log.Warn("set mix tree", "tx", round.TxHash.Hex(), "rIndex", rIndex, "dIndex", dIndex,
				//		"hitdep", string(hitdepbs), "depChecked", string(depCheckedAddrbs), "round", string(rbs))
				//	panic("no possible to hit dep ")
				//}
				//// assert readDep[rindex] is the detailReadAddr
				//if readDep[rIndex].AddLoc.Address != detailReadAddr {
				//	rbs, _ := json.Marshal(round)
				//	hitdepbs, _ := json.Marshal(hitDep)
				//	depCheckedAddrbs, _ := json.Marshal(depCheckedAddr)
				//	log.Warn("set mix tree", "tx", round.TxHash.Hex(), "rIndex", rIndex, "dIndex", dIndex,
				//		"hitdep", string(hitdepbs), "depChecked", string(depCheckedAddrbs), "round", string(rbs))
				//	panic("addr dep and addr detail are not aligned")
				//}
				depCheckedAddr[detailReadAddr] = true
				noHit++
				rIndex++
			}
			insertDetail2MixTree(currentNode, rIndex, readDep, dIndex+1, detailSeq, hitDep, depCheckedAddr, noHit, round, leafCount)
			if isNewAddr {
				depCheckedAddr[detailReadAddr] = false
			}
		}
	}
}

func insertNode(currentNode *cmptypes.PreplayResTrieNode, alv *cmptypes.AddrLocValue) *cmptypes.PreplayResTrieNode {
	if currentNode.NodeType == nil {
		currentNode.NodeType = alv.AddLoc
	} else {
		////TODO: debug code, test well(would not be touched), can be removed
		//key := currentNode.NodeType
		//if !cmp.Equal(key, alv.AddLoc) {
		//
		//	log.Warn("nodekey", "addr", key.Address, "field", key.Field, "loc", key.Loc)
		//	log.Warn("addrfield", "addr", alv.AddLoc.Address, "filed",
		//		alv.AddLoc.Field, "loc", alv.AddLoc.Loc)
		//
		//	panic("should be the same key")
		//}
	}

	var value interface{}
	if currentNode.NodeType.Field == cmptypes.Dependence {
		value = alv.Value.(*cmptypes.ChangedBy).Hash()
	} else {
		value = alv.Value
	}
	child, ok := currentNode.Children[value]
	if !ok {
		child = &cmptypes.PreplayResTrieNode{Children: make(map[interface{}]*cmptypes.PreplayResTrieNode)}
		currentNode.Children[value] = child
	}
	return child
}

func SearchTree(trie *cmptypes.PreplayResTrie, db *state.StateDB, bc core.ChainContext, header *types.Header, abort func() bool, debug bool) (
	node *cmptypes.PreplayResTrieNode, isAbort bool, ok bool) {

	currentNode := trie.Root
	if currentNode.NodeType == nil {
		return nil, false, false
	}

	for ; !currentNode.IsLeaf; {
		if abort() {
			return nil, true, false
		}
		nodeType := currentNode.NodeType
		value := getCurrentValue(nodeType, db, bc, header)
		// assert all kinds of values in the trie are simple types, which is guaranteed in rwhook when recording the readDetail
		childNode, ok := currentNode.Children[value]
		if debug {
			albs,_:= json.Marshal(&cmptypes.AddrLocValue{nodeType, value})
			log.Warn("search node type", "ok", ok, "addr", nodeType.Address, "field", nodeType.Field,
				"loc", nodeType.Loc, "value", value, "alv", string(albs))
		}
		if ok {
			currentNode = childNode
		} else {
			return nil, false, false
		}
	}
	return currentNode, false, true
}

func SearchMixTree(trie *cmptypes.PreplayResTrie, db *state.StateDB, bc core.ChainContext, header *types.Header, abort func() bool, debug bool) (
	round *cache.PreplayResult, mixStatus *cmptypes.MixHitStatus, isAbort bool, ok bool) {
	currentNode := trie.Root

	if currentNode.NodeType == nil {
		return nil, nil, false, false
	}

	var matchedDeps []common.Address
	depMatchedMap := make(map[common.Address]bool)
	allDepMatched := true
	allDetailMatched := true

	for ; !currentNode.IsLeaf; {
		if abort() {
			return nil, nil, true, false
		}
		nodeType := currentNode.NodeType
		value := getCurrentValue(nodeType, db, bc, header)
		// assert all kinds of values in the trie are simple types, which is guaranteed in rwhook when recording the readDetail
		childNode, ok := currentNode.Children[value]
		if debug {
			albs,_:= json.Marshal(&cmptypes.AddrLocValue{nodeType, value})
			log.Warn("search node type", "ok", ok, "addr", nodeType.Address, "field", nodeType.Field,
				"loc", nodeType.Loc, "value", value, "alv", string(albs))
		}
		if ok {
			currentNode = childNode
			if nodeType.Field == cmptypes.Dependence {
				allDetailMatched = false
				matchedDeps = append(matchedDeps, nodeType.Address)
				depMatchedMap[nodeType.Address] = true
			}
		} else {
			if currentNode.DetailChild != nil {
				// assert cmptypes.IsStateField(nodeType.Field) is false
				allDepMatched = false
				currentNode = currentNode.DetailChild
				if debug {
					log.Warn("search node type-detail node", "ok", ok, "addr", nodeType.Address, "field", nodeType.Field,
						"loc", nodeType.Loc, "value", value)
				}
			} else {
				return nil, nil, false, false
			}
		}
	}
	mixHitType := cmptypes.PartialHit
	if allDepMatched {
		mixHitType = cmptypes.AllDepHit
	} else if allDetailMatched {
		mixHitType = cmptypes.AllDetailHit
	}

	mixStatus = &cmptypes.MixHitStatus{MixHitType: mixHitType, DepHitAddr: matchedDeps, DepHitAddrMap: depMatchedMap}
	return currentNode.Round.(*cache.PreplayResult), mixStatus, false, true
}

func getCurrentValue(addrLoc *cmptypes.AddrLocation, statedb *state.StateDB, bc core.ChainContext, header *types.Header) interface{} {
	addr := addrLoc.Address
	switch addrLoc.Field {
	case cmptypes.Coinbase:
		return header.Coinbase
	case cmptypes.Timestamp:
		return header.Time
	case cmptypes.Number:
		return header.Number.Uint64()
	case cmptypes.Difficulty:
		return common.BigToHash(header.Difficulty)
	case cmptypes.GasLimit:
		return header.GasLimit
	case cmptypes.Blockhash:
		number := addrLoc.Loc.(uint64)
		curBn := header.Number.Uint64()
		if curBn-number < 257 && number < curBn {
			getHashFn := core.GetHashFn(header, bc)
			return getHashFn(number)
		} else {
			return common.Hash{}
		}
	case cmptypes.PreBlockHash:
		return header.ParentHash
	case cmptypes.Exist:
		return statedb.Exist(addr)
	case cmptypes.Empty:
		return statedb.Empty(addr)
	case cmptypes.Balance:
		return common.BigToHash(statedb.GetBalance(addr)) // // convert complex type (big.Int) to simple type: bytes[32]
	case cmptypes.Nonce:
		return statedb.GetNonce(addr)
	case cmptypes.CodeHash:
		value := statedb.GetCodeHash(addr)
		if value == emptyCodeHash {
			value = nilCodeHash
		}
		return value
	case cmptypes.Storage:
		position := addrLoc.Loc.(common.Hash)
		return statedb.GetState(addr, position)
	case cmptypes.CommittedStorage:
		position := addrLoc.Loc.(common.Hash)
		return statedb.GetCommittedState(addr, position)
	case cmptypes.Dependence:
		var value interface{}
		changedBy, ok := statedb.AccountChangedBy[addr]
		if ok {
			value = changedBy.Hash()
		} else {
			value = *statedb.TrieGetSnap(addr)
		}
		return value
	default:
		log.Error("wrong field", "field ", addrLoc.Field)
	}
	return nil
}
