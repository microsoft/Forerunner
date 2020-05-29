package cmpreuse

import (
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"math/big"
)

const (
	TreeCleanThreshold    = 800
	MaxTreeCleanThreshold = 5000
)

//func isExternalTransfer(seqRecord []*cmptypes.AddrLocValue) bool {
//	// assert seqRecord len
//	if len(seqRecord) != 5 {
//		return false
//	} else {
//		//assert 'to' is a common user
//		if seqRecord[4].AddLoc.Field != cmptypes.CodeHash {
//			return false
//		} else {
//			value := seqRecord[4].Value.(common.Hash)
//			if value != EmptyCodeHash && value != EmptyCodeHash {
//				return false
//			}
//		}
//	}
//	return true
//}

// only external transfer tx can call this function
func InsertDelta(tx *types.Transaction, trie *cmptypes.PreplayResTrie, round *cache.PreplayResult, blockNumber uint64) {

	if trie.LeafCount > 0 {
		// each tx only have one kind of result now.
		return
	}
	currentNode := trie.Root
	record := round.RWrecord
	seqRecord := record.ReadDetail.ReadDetailSeq

	for index, addrFieldValue := range seqRecord {
		if addrFieldValue.AddLoc.Field == cmptypes.Balance {
			if index == 1 {
				minB := new(big.Int).Add(tx.Value(), new(big.Int).Mul(new(big.Int).SetUint64(tx.Gas()), tx.GasPrice()))
				adv := &cmptypes.AddrLocValue{
					AddLoc: &cmptypes.AddrLocation{
						Address: addrFieldValue.AddLoc.Address,
						Field:   cmptypes.MinBalance,
						Loc:     minB,
					},
					Value: true,
				}
				currentNode, _ = insertNode(currentNode, adv)
			} else { // else : the balance of `to` is unnecessary.
				cmptypes.MyAssert(index == 3)
			}
		} else {
			currentNode, _ = insertNode(currentNode, addrFieldValue)
		}
	}
	if currentNode.IsLeaf {
	} else {
		currentNode.IsLeaf = true
		//
		//// set delta
		//record.WStateDelta = make(map[common.Address]*cache.WStateDelta)
		//senderBalanceDelta := new(big.Int).Sub(record.WState[sender].Balance, record.RState[sender].Balance)
		//record.WStateDelta[sender] = &cache.WStateDelta{senderBalanceDelta}
		//
		//cmptypes.MyAssert(tx.To() != nil)
		//if _, ok := record.WState[*tx.To()]; ok {
		//	toBalanceDelta := new(big.Int).Sub(record.WState[*tx.To()].Balance, record.RState[*tx.To()].Balance)
		//	record.WStateDelta[*tx.To()] = &cache.WStateDelta{toBalanceDelta}
		//}

		currentNode.Round = round
		trie.LeafCount += 1
	}
}

func InsertRecord(trie *cmptypes.PreplayResTrie, round *cache.PreplayResult, blockNumber uint64) {

	//if blockNumber > trie.LatestBN {
	//	trie.LatestBN = blockNumber
	//	if trie.LeafCount > TreeCleanThreshold {
	//		trie.Clear()
	//	}
	//}
	//
	//if trie.LeafCount > MaxTreeCleanThreshold {
	//	trie.Clear()
	//}

	newNodeCount := uint(0)
	isNew := false
	currentNode := trie.Root
	record := round.RWrecord
	seqRecord := record.ReadDetail.ReadDetailSeq
	for _, addrFieldValue := range seqRecord {
		currentNode, isNew = insertNode(currentNode, addrFieldValue)
		if isNew {
			newNodeCount++
		}
	}
	if currentNode.IsLeaf {
		// there is the same RWRecord before, assert they are the same
		// TODO debug code, need be removed
		//if currentNode.RWRecord.GetHash() != record.GetHash() {
		//	panic("the rwrecord should be the same")
		//}
	} else {
		currentNode.IsLeaf = true
		currentNode.Round = round
		trie.LeafCount += 1
		newLeaves := make([]*cmptypes.PreplayResTrieNode, 1)
		newLeaves[0] = currentNode
		trie.TrackRoundNodes(newLeaves, newNodeCount, round)
	}
}

func SearchPreplayRes(trie *cmptypes.PreplayResTrie, db *state.StateDB, bc core.ChainContext, header *types.Header, abort func() bool, ) (*cmptypes.PreplayResTrieNode, bool, bool) {
	return SearchTree(trie, db, bc, header, abort, false)
}

// Deprecated
// return true is this round is inserted. false for this round is a repeated round
func InsertAccDep(trie *cmptypes.PreplayResTrie, round *cache.PreplayResult, blockNumber uint64, preBlockHash *common.Hash) bool {

	//if blockNumber > trie.LatestBN {
	//	trie.LatestBN = blockNumber
	//	if trie.LeafCount > TreeCleanThreshold {
	//		trie.Clear()
	//	}
	//}
	//
	//if trie.LeafCount > MaxTreeCleanThreshold {
	//	trie.Clear()
	//}

	currentNode := trie.Root

	for _, readDep := range round.ReadDepSeq {
		//for _, readDep := range append(topseq, round.ReadDepSeq...) {
		currentNode, _ = insertNode(currentNode, readDep)
	}

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
		//	//	preTxResId := round.ReadDepSeq[addr].LastTxResID
		//	//	newTxResId := currentNode.Round.(*cache.PreplayResult).ReadDepSeq[addr].LastTxResID
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

// only some of addresses's would inserted into detail check subtree,
const FixedDepCheckCount = 4

func InsertMixTree(tx *types.Transaction, trie *cmptypes.PreplayResTrie, round *cache.PreplayResult, isExternalTransfer bool, blockNumber uint64, preBlockHash *common.Hash) {
	var newLeaves []*cmptypes.PreplayResTrieNode
	newLeavesPtr := &newLeaves
	newNodeCount := new(uint)

	//if blockNumber > trie.LatestBN {
	//	trie.LatestBN = blockNumber
	//	if trie.LeafCount > TreeCleanThreshold {
	//		trie.Clear()
	//	}
	//}
	//
	//if trie.LeafCount > MaxTreeCleanThreshold {
	//	trie.Clear()
	//}

	currentNode := trie.Root

	hitDep := make(map[interface{}]bool)
	depCheckedAddr := make(map[common.Address]bool)
	checkedButNoHit := 0

	insertDep2MixTree(currentNode, 0, 0, hitDep, depCheckedAddr, checkedButNoHit, newLeavesPtr, newNodeCount, round, tx, isExternalTransfer)

	trie.LeafCount += 1

	trie.TrackRoundNodes(*newLeavesPtr, *newNodeCount, round)
}

func insertDep2MixTree(currentNode *cmptypes.PreplayResTrieNode, readDepIndex int, detailDeqIndex int,
	hitDep map[interface{}]bool, depCheckedAddr map[common.Address]bool, noHit int,
	newLeaves *[]*cmptypes.PreplayResTrieNode, newNodeCount *uint, round *cache.PreplayResult, tx *types.Transaction, isExternalTransfer bool) {
	readDepSeq := round.ReadDepSeq
	detailSeq := round.RWrecord.ReadDetail.ReadDetailSeq

	// all dep have been inserted, now, insert the rest details
	if readDepIndex >= len(readDepSeq) {
		for _, detailalv := range detailSeq[detailDeqIndex:] {
			if cmptypes.IsChainField(detailalv.AddLoc.Field) || hitDep[detailalv.AddLoc.Address] {
				continue
			}
			var isNew bool
			currentNode, isNew = insertNode(currentNode, detailalv)
			if isNew {
				*newNodeCount = *newNodeCount + 1
			}
		}
		if currentNode.IsLeaf {
		} else {
			currentNode.IsLeaf = true
			currentNode.Round = round
			*newLeaves = append(*newLeaves, currentNode)
		}

		return
	}
	curDep := readDepSeq[readDepIndex]
	curAddr := curDep.AddLoc.Address
	child := currentNode

	if cmptypes.IsChainField(curDep.AddLoc.Field) {
		hitDep[curDep.AddLoc.Field] = true
		var isNew bool
		child, isNew = insertNode(currentNode, curDep)
		if isNew {
			*newNodeCount = *newNodeCount + 1
		}
		insertDep2MixTree(child, readDepIndex+1, detailDeqIndex, hitDep, depCheckedAddr, noHit, newLeaves, newNodeCount, round, tx, isExternalTransfer)

		hitDep[curDep.AddLoc.Field] = false
	} else {
		// assert hitDep[curAddr] == false, depCheckedAddr[curAddr]= false
		//if hitDep[curAddr] || depCheckedAddr[curAddr] {
		//	rbs, _ := json.Marshal(round)
		//	hitdepbs, _ := json.Marshal(hitDep)
		//	depCheckedAddrbs, _ := json.Marshal(depCheckedAddr)
		//	log.Warn("insert dep wrong", "tx", round.TxHash.Hex(), "readDepIndex", readDepIndex, "detailDeqIndex", detailDeqIndex,
		//		"hitdep", string(hitdepbs), "depChecked", string(depCheckedAddrbs), "round", string(rbs))
		//
		//	panic("Insert dep check wrong")
		//}

		hitDep[curAddr] = true
		depCheckedAddr[curAddr] = true
		var isNew bool
		child, isNew = insertNode(currentNode, curDep)
		if isNew {
			*newNodeCount = *newNodeCount + 1
		}
		insertDetail2MixTree(child, readDepIndex+1, detailDeqIndex, hitDep, depCheckedAddr, noHit, newLeaves, newNodeCount, round, tx, isExternalTransfer)

		hitDep[curAddr] = false

		if currentNode.DetailChild == nil {
			currentNode.DetailChild = &cmptypes.PreplayResTrieNode{Parent: currentNode}
			*newNodeCount = *newNodeCount + 1
		}

		insertDetail2MixTree(currentNode.DetailChild, readDepIndex+1, detailDeqIndex, hitDep, depCheckedAddr, noHit+1, newLeaves, newNodeCount, round, tx, isExternalTransfer)

		depCheckedAddr[curAddr] = false
	}
}

func insertDetail2MixTree(currentNode *cmptypes.PreplayResTrieNode, rIndex int, dIndex int,
	hitDep map[interface{}]bool, depCheckedAddr map[common.Address]bool, noHit int,
	newLeaves *[]*cmptypes.PreplayResTrieNode, newNodeCount *uint, round *cache.PreplayResult, tx *types.Transaction, isExternalTransfer bool) {
	detailSeq := round.RWrecord.ReadDetail.ReadDetailSeq
	if dIndex >= len(detailSeq) {
		if currentNode.IsLeaf {
		} else {
			currentNode.IsLeaf = true
			currentNode.Round = round
			*newLeaves = append(*newLeaves, currentNode)
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
			var isNew bool
			currentNode, isNew = insertNode(currentNode, detailRead)
			if isNew {
				*newNodeCount = *newNodeCount + 1
			}
		}
		insertDetail2MixTree(currentNode, rIndex, dIndex+1, hitDep, depCheckedAddr, noHit, newLeaves, newNodeCount, round, tx, isExternalTransfer)
		return
	} else {
		detailReadAddr := detailRead.AddLoc.Address
		isNewAddr := !depCheckedAddr[detailReadAddr]
		// to reduce the node number, ifnoHit <= FixedDepCheckCount, only append detail node, regardless of this addr is not checked by deb // magic number
		if isNewAddr && noHit < FixedDepCheckCount && !(noHit > 0 && noHit <= FixedDepCheckCount && rIndex > FixedDepCheckCount+3) {
			insertDep2MixTree(currentNode, rIndex, dIndex, hitDep, depCheckedAddr, noHit, newLeaves, newNodeCount, round, tx, isExternalTransfer)
		} else {
			if !hitDep[detailReadAddr] {
				var isNewNode bool
				if isExternalTransfer {
					if detailRead.AddLoc.Field == cmptypes.Balance {
						cmptypes.MyAssert(dIndex == 1 || dIndex == 3)
						isSender := dIndex == 1
						if isSender {
							minB := new(big.Int).Add(tx.Value(), new(big.Int).Mul(new(big.Int).SetUint64(tx.Gas()), tx.GasPrice()))
							adv := &cmptypes.AddrLocValue{
								AddLoc: &cmptypes.AddrLocation{
									Address: detailReadAddr,
									Field:   cmptypes.MinBalance,
									Loc:     minB,
								},
								Value: true,
							}
							currentNode, isNewNode = insertNode(currentNode, adv)
						} // else : the balance of `to` is unnecessary, and it would not be inserted.
					} else {
						currentNode, isNewNode = insertNode(currentNode, detailRead)
					}
				} else {
					currentNode, isNewNode = insertNode(currentNode, detailRead)
				}

				if isNewNode {
					*newNodeCount = *newNodeCount + 1
				}
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
			insertDetail2MixTree(currentNode, rIndex, dIndex+1, hitDep, depCheckedAddr, noHit, newLeaves, newNodeCount, round, tx, isExternalTransfer)
			if isNewAddr {
				depCheckedAddr[detailReadAddr] = false
			}
		}
	}
}

//	 @return    *cmptypes.PreplayResTrieNode	"the child node(which is supposed to be inserted)"
//				bool 	"whether a new node is inserted"
func insertNode(currentNode *cmptypes.PreplayResTrieNode, alv *cmptypes.AddrLocValue) (*cmptypes.PreplayResTrieNode, bool) {
	if currentNode.NodeType == nil {
		currentNode.NodeType = alv.AddLoc
		currentNode.Children = cmptypes.NewChildren(alv.AddLoc)
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

	var child *cmptypes.PreplayResTrieNode
	var ok bool
	switch alv.AddLoc.Field {
	case cmptypes.Coinbase:
		value := alv.Value.(common.Address)
		realChild := currentNode.Children.(cmptypes.AddressChildren)
		child, ok = realChild[value]
		if !ok {
			child = &cmptypes.PreplayResTrieNode{Parent: currentNode, Value: alv.Value}
			realChild[value] = child
		}
	case cmptypes.Timestamp, cmptypes.Number, cmptypes.Difficulty, cmptypes.GasLimit, cmptypes.Nonce:
		value := alv.Value.(uint64)
		realChild := currentNode.Children.(cmptypes.UintChildren)
		child, ok = realChild[value]
		if !ok {
			child = &cmptypes.PreplayResTrieNode{Parent: currentNode, Value: alv.Value}
			realChild[value] = child
		}
	case cmptypes.Blockhash, cmptypes.Balance, cmptypes.CodeHash, cmptypes.Storage, cmptypes.CommittedStorage:
		value := alv.Value.(common.Hash)
		realChild := currentNode.Children.(cmptypes.HashChildren)
		child, ok = realChild[value]
		if !ok {
			child = &cmptypes.PreplayResTrieNode{Parent: currentNode, Value: alv.Value}
			realChild[value] = child
		}
	case cmptypes.Exist, cmptypes.Empty, cmptypes.MinBalance:
		value := alv.Value.(bool)
		realChild := currentNode.Children.(cmptypes.BoolChildren)
		child, ok = realChild[value]
		if !ok {
			child = &cmptypes.PreplayResTrieNode{Parent: currentNode, Value: alv.Value}
			realChild[value] = child
		}
	case cmptypes.Dependence:
		value := alv.Value.(*cmptypes.ChangedBy).Hash()
		realChild := currentNode.Children.(cmptypes.StringChildren)
		child, ok = realChild[value]
		if !ok {
			child = &cmptypes.PreplayResTrieNode{Parent: currentNode, Value: value}
			realChild[value] = child
		}
	default:
		panic("Wrong Field")
	}

	return child, !ok
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
		childNode, ok := getChild(currentNode, db, bc, header, false)
		//if debug {
		//  nodeType := currentNode.NodeType
		//	albs, _ := json.Marshal(&cmptypes.AddrLocValue{AddLoc: nodeType, Value: value})
		//	log.Warn("search node type", "ok", ok, "addr", nodeType.Address, "field", nodeType.Field,
		//		"loc", nodeType.Loc, "value", value, "alv", string(albs))
		//}
		if ok {
			currentNode = childNode
		} else {
			return nil, false, false
		}
	}
	return currentNode, false, true
}

func SearchMixTree(trie *cmptypes.PreplayResTrie, db *state.StateDB, bc core.ChainContext, header *types.Header, abort func() bool,
	debug bool, isBlockProcess bool, isExternalTransfer bool) (round *cache.PreplayResult, mixStatus *cmptypes.MixHitStatus,
	missNode *cmptypes.PreplayResTrieNode, missValue interface{}, isAbort bool, ok bool) {
	currentNode := trie.Root

	if currentNode.NodeType == nil {
		if isBlockProcess {
			return nil, nil, currentNode, nil, false, false
		} else {
			return nil, nil, nil, nil, false, false
		}
	}

	var (
		matchedDeps   []common.Address
		depMatchedMap = make(map[common.Address]interface{})

		allDepMatched    = true
		allDetailMatched = true
	)

	for ; !currentNode.IsLeaf; {
		if abort() {
			return nil, nil, nil, nil, true, false
		}
		childNode, ok := getChild(currentNode, db, bc, header, isBlockProcess)
		//if debug {
		//
		//	albs, _ := json.Marshal(&cmptypes.AddrLocValue{AddLoc: nodeType, Value: value})
		//	log.Warn("search node type", "ok", ok, "addr", nodeType.Address, "field", nodeType.Field,
		//		"loc", nodeType.Loc, "value", value, "alv", string(albs))
		//}
		nodeType := currentNode.NodeType
		if ok {
			currentNode = childNode
			if nodeType.Field == cmptypes.Dependence {
				allDetailMatched = false
				matchedDeps = append(matchedDeps, nodeType.Address)
				depMatchedMap[nodeType.Address] = true
			}
		} else {
			if currentNode.DetailChild != nil {
				// Note: for external transfer, mixcheck is disabled.
				if isExternalTransfer && !isBlockProcess {
					mixStatus = &cmptypes.MixHitStatus{MixHitType: cmptypes.NotMixHit}
					return nil, mixStatus, nil, nil, false, false
				}
				// assert cmptypes.IsStateField(nodeType.Field) is false
				allDepMatched = false
				currentNode = currentNode.DetailChild

			} else {
				mixStatus = &cmptypes.MixHitStatus{MixHitType: cmptypes.NotMixHit, DepHitAddr: matchedDeps, DepHitAddrMap: depMatchedMap}

				if isBlockProcess {
					// to reduce the cost of converting interfaces, mute the miss Value
					return nil, mixStatus, currentNode, nil, false, false
				} else {
					return nil, mixStatus, nil, nil, false, false
				}
			}
		}
	}
	mixHitType := cmptypes.PartialHit
	if allDepMatched {
		mixHitType = cmptypes.AllDepHit
	} else if allDetailMatched {
		if isExternalTransfer {
			mixHitType = cmptypes.AllDeltaHit
		} else {
			mixHitType = cmptypes.AllDetailHit
		}
	} else if isExternalTransfer {
		mixHitType = cmptypes.PartialDeltaHit
	}

	mixStatus = &cmptypes.MixHitStatus{MixHitType: mixHitType, DepHitAddr: matchedDeps, DepHitAddrMap: depMatchedMap}
	return currentNode.Round.(*cache.PreplayResult), mixStatus, nil, nil, false, true
}

func copyNode(node *cmptypes.PreplayResTrieNode) *cmptypes.PreplayResTrieNode {
	nodeCpy := &cmptypes.PreplayResTrieNode{}
	if node.NodeType != nil {
		nodeCpy.NodeType = &cmptypes.AddrLocation{
			Address: node.NodeType.Address,
			Field:   node.NodeType.Field,
			Loc:     node.NodeType.Loc,
		}
	}
	nodeCpy.Children = node.Children.CopyKey()
	return nodeCpy
}

func getChild(currentNode *cmptypes.PreplayResTrieNode, statedb *state.StateDB, bc core.ChainContext, header *types.Header, isBlockProcess bool) (*cmptypes.PreplayResTrieNode, bool) {

	addr := currentNode.NodeType.Address
	switch currentNode.NodeType.Field {
	case cmptypes.Coinbase:
		child, ok := currentNode.Children.(cmptypes.AddressChildren)[header.Coinbase]
		return child, ok
	case cmptypes.Timestamp:
		child, ok := currentNode.Children.(cmptypes.UintChildren)[header.Time]
		return child, ok
	case cmptypes.Number:
		child, ok := currentNode.Children.(cmptypes.UintChildren)[header.Number.Uint64()]
		return child, ok
	case cmptypes.Difficulty:
		child, ok := currentNode.Children.(cmptypes.UintChildren)[header.Difficulty.Uint64()]
		return child, ok
	case cmptypes.GasLimit:
		child, ok := currentNode.Children.(cmptypes.UintChildren)[header.GasLimit]
		return child, ok
	case cmptypes.Blockhash:
		number := currentNode.NodeType.Loc.(uint64)
		curBn := header.Number.Uint64()
		value := common.Hash{}
		if curBn-number < 257 && number < curBn {
			getHashFn := core.GetHashFn(header, bc)
			value = getHashFn(number)
		}
		child, ok := currentNode.Children.(cmptypes.HashChildren)[value]
		return child, ok
	case cmptypes.Exist:
		child, ok := currentNode.Children.(cmptypes.BoolChildren)[statedb.Exist(addr)    ]
		return child, ok
	case cmptypes.Empty:
		child, ok := currentNode.Children.(cmptypes.BoolChildren)[statedb.Empty(addr)]
		return child, ok
	case cmptypes.Balance:
		value := common.BigToHash(statedb.GetBalance(addr)) // // convert complex type (big.Int) to simple type: bytes[32]
		child, ok := currentNode.Children.(cmptypes.HashChildren)[value]
		return child, ok
	case cmptypes.Nonce:
		child, ok := currentNode.Children.(cmptypes.UintChildren)[statedb.GetNonce(addr)]
		return child, ok
	case cmptypes.CodeHash:
		value := statedb.GetCodeHash(addr)
		if value == cmptypes.EmptyCodeHash {
			value = cmptypes.NilCodeHash
		}
		child, ok := currentNode.Children.(cmptypes.HashChildren)[value]
		return child, ok
	case cmptypes.Storage:
		position := currentNode.NodeType.Loc.(common.Hash)
		value := statedb.GetState(addr, position)
		child, ok := currentNode.Children.(cmptypes.HashChildren)[value]
		return child, ok
	case cmptypes.CommittedStorage:
		position := currentNode.NodeType.Loc.(common.Hash)
		value := statedb.GetCommittedState(addr, position)
		child, ok := currentNode.Children.(cmptypes.HashChildren)[value]
		return child, ok
	case cmptypes.Dependence:
		value := statedb.GetAccountSnapOrChangedBy(addr)

		child, ok := currentNode.Children.(cmptypes.StringChildren)[value]
		return child, ok
	case cmptypes.MinBalance:
		value := statedb.GetBalance(addr).Cmp(currentNode.NodeType.Loc.(*big.Int)) > 0
		child, ok := currentNode.Children.(cmptypes.BoolChildren)[value]
		return child, ok
	default:
		log.Error("wrong field", "field ", currentNode.NodeType.Field)
	}
	return nil, false
}

// return : child node  &&  whether new node is inserted
func getChildOrInsert(currentNode *cmptypes.PreplayResTrieNode, alv *cmptypes.AddrLocValue) (*cmptypes.PreplayResTrieNode, bool) {
	var child *cmptypes.PreplayResTrieNode
	var ok bool
	switch alv.AddLoc.Field {
	case cmptypes.Coinbase:
		value := alv.Value.(common.Address)
		realChild := currentNode.Children.(cmptypes.AddressChildren)
		child, ok = realChild[value]
		if !ok {
			child = &cmptypes.PreplayResTrieNode{Parent: currentNode, Value: alv.Value}
			realChild[value] = child
		}
	case cmptypes.Timestamp, cmptypes.Number, cmptypes.Difficulty, cmptypes.GasLimit, cmptypes.Nonce:
		value := alv.Value.(uint64)
		realChild := currentNode.Children.(cmptypes.UintChildren)
		child, ok = realChild[value]
		if !ok {
			child = &cmptypes.PreplayResTrieNode{Parent: currentNode, Value: alv.Value}
			realChild[value] = child
		}
	case cmptypes.Blockhash, cmptypes.Balance, cmptypes.CodeHash, cmptypes.Storage, cmptypes.CommittedStorage:
		value := alv.Value.(common.Hash)
		realChild := currentNode.Children.(cmptypes.HashChildren)
		child, ok = realChild[value]
		if !ok {
			child = &cmptypes.PreplayResTrieNode{Parent: currentNode, Value: alv.Value}
			realChild[value] = child
		}
	case cmptypes.Exist, cmptypes.Empty, cmptypes.MinBalance:
		value := alv.Value.(bool)
		realChild := currentNode.Children.(cmptypes.BoolChildren)
		child, ok = realChild[value]
		if !ok {
			child = &cmptypes.PreplayResTrieNode{Parent: currentNode, Value: alv.Value}
			realChild[value] = child
		}
	case cmptypes.Dependence:
		value := alv.Value.(*cmptypes.ChangedBy).Hash()
		realChild := currentNode.Children.(cmptypes.StringChildren)
		child, ok = realChild[value]
		if !ok {
			child = &cmptypes.PreplayResTrieNode{Parent: currentNode, Value: alv.Value}
			realChild[value] = child
		}
	default:
		panic("Wrong Field")
	}

	return child, !ok
}
