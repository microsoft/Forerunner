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
	"math/big"
)

const (
	TreeCleanThreshold    = 800
	MaxTreeCleanThreshold = 5000
)

func InsertDelta(tx *types.Transaction, trie *cmptypes.PreplayResTrie, round *cache.PreplayResult, blockNumber uint64) {
	if blockNumber > trie.LatestBN {
		trie.LatestBN = blockNumber
	}
	if trie.LeafCount > 0 {
		// each tx only have one kind of result now.
		return
	}
	currentNode := trie.Root
	record := round.RWrecord
	seqRecord := record.ReadDetail.ReadDetailSeq
	// assert seqRecord len
	if len(seqRecord) != 5 {
		return
	} else {
		//assert to is a common user
		if seqRecord[4].AddLoc.Field != cmptypes.CodeHash {
			return
		} else {
			value := seqRecord[4].Value.(common.Hash)
			if value != nilCodeHash && value != emptyCodeHash {
				return
			}
		}
	}
	var sender common.Address
	isSender := true
	for _, addrFieldValue := range seqRecord {
		if addrFieldValue.AddLoc.Field == cmptypes.Balance {
			if isSender {
				sender = addrFieldValue.AddLoc.Address
				isSender = false
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
			}
		} else {
			currentNode, _ = insertNode(currentNode, addrFieldValue)
		}
	}
	if currentNode.IsLeaf {
	} else {
		currentNode.IsLeaf = true

		// set delta
		record.WStateDelta = make(map[common.Address]*cache.WStateDelta)

		senderBalanceDelta := new(big.Int).Sub(record.WState[sender].Balance, record.RState[sender].Balance)
		record.WStateDelta[sender] = &cache.WStateDelta{senderBalanceDelta}
		if tx.To() != nil {
			if _, ok := record.WState[*tx.To()]; ok {
				toBalanceDelta := new(big.Int).Sub(record.WState[*tx.To()].Balance, record.RState[*tx.To()].Balance)
				record.WStateDelta[*tx.To()] = &cache.WStateDelta{toBalanceDelta}
			}
		}

		currentNode.Round = round
		trie.LeafCount += 1
	}
	trie.RoundIds[round.RoundID] = true
}

func InsertRecord(trie *cmptypes.PreplayResTrie, round *cache.PreplayResult, blockNumber uint64) {

	if blockNumber > trie.LatestBN {
		trie.LatestBN = blockNumber
		if trie.LeafCount > TreeCleanThreshold {
			trie.Clear()
		}
	}

	if trie.LeafCount > MaxTreeCleanThreshold {
		trie.Clear()
	}

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
		newLeaves := make([]cmptypes.ISRefCountNode, 1)
		newLeaves[0] = currentNode
		trie.TrackRoundRefNodes(newLeaves, newNodeCount, round)
	}
	trie.RoundIds[round.RoundID] = true
}

func SearchPreplayRes(trie *cmptypes.PreplayResTrie, db *state.StateDB, bc core.ChainContext, header *types.Header, abort func() bool, ) (*cmptypes.PreplayResTrieNode, bool, bool) {
	return SearchTree(trie, db, bc, header, abort, false)
}

// Deprecated
// return true is this round is inserted. false for this round is a repeated round
func InsertAccDep(trie *cmptypes.PreplayResTrie, round *cache.PreplayResult, blockNumber uint64, preBlockHash *common.Hash) bool {

	if blockNumber > trie.LatestBN {
		trie.LatestBN = blockNumber
		if trie.LeafCount > TreeCleanThreshold {
			trie.Clear()
		}
	}

	if trie.LeafCount > MaxTreeCleanThreshold {
		trie.Clear()
	}

	currentNode := trie.Root

	for _, readDep := range round.ReadDepSeq {
		//for _, readDep := range append(topseq, round.ReadDepSeq...) {
		currentNode, _ = insertNode(currentNode, readDep)
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

func SearchAccDep(trie *cmptypes.PreplayResTrie, db *state.StateDB, bc core.ChainContext, header *types.Header, abort func() bool) (*cmptypes.PreplayResTrieNode, bool, bool) {
	return SearchTree(trie, db, bc, header, abort, false)
}

// only some of addresses's would inserted into detail check subtree,
const FixedDepCheckCount = 4

func InsertMixTree(trie *cmptypes.PreplayResTrie, round *cache.PreplayResult, blockNumber uint64, preBlockHash *common.Hash) {
	var newLeaves []cmptypes.ISRefCountNode
	newLeavesPtr := &newLeaves
	newNodeCount := new(uint)

	if blockNumber > trie.LatestBN {
		trie.LatestBN = blockNumber
		if trie.LeafCount > TreeCleanThreshold {
			trie.Clear()
		}
	}

	if trie.LeafCount > MaxTreeCleanThreshold {
		trie.Clear()
	}

	detailSeq := round.RWrecord.ReadDetail.ReadDetailSeq

	currentNode := trie.Root

	hitDep := make(map[interface{}]bool)
	depCheckedAddr := make(map[common.Address]bool)
	checkedButNoHit := 0
	//leafCount := new(int)

	insertDep2MixTree(currentNode, 0, round.ReadDepSeq, 0, detailSeq, hitDep, depCheckedAddr, checkedButNoHit, round, newLeavesPtr, newNodeCount)

	trie.LeafCount += 1
	trie.RoundIds[round.RoundID] = true

	trie.TrackRoundRefNodes(*newLeavesPtr, *newNodeCount, round)
}

func insertDep2MixTree(currentNode *cmptypes.PreplayResTrieNode, rIndex int, readDepSeq []*cmptypes.AddrLocValue, dIndex int,
	detailSeq []*cmptypes.AddrLocValue, hitDep map[interface{}]bool, depCheckedAddr map[common.Address]bool, noHit int,
	round *cache.PreplayResult, newLeaves *[]cmptypes.ISRefCountNode, newNodeCount *uint) {

	if rIndex >= len(readDepSeq) {
		for _, detailalv := range detailSeq[dIndex:] {
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
	curDep := readDepSeq[rIndex]
	curAddr := curDep.AddLoc.Address
	child := currentNode

	if cmptypes.IsChainField(curDep.AddLoc.Field) {
		hitDep[curDep.AddLoc.Field] = true
		var isNew bool
		child, isNew = insertNode(currentNode, curDep)
		if isNew {
			*newNodeCount = *newNodeCount + 1
		}
		insertDep2MixTree(child, rIndex+1, readDepSeq, dIndex, detailSeq, hitDep, depCheckedAddr, noHit, round, newLeaves, newNodeCount)

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
		var isNew bool
		child, isNew = insertNode(currentNode, curDep)
		if isNew {
			*newNodeCount = *newNodeCount + 1
		}
		insertDetail2MixTree(child, rIndex+1, readDepSeq, dIndex, detailSeq, hitDep, depCheckedAddr, noHit, round, newLeaves, newNodeCount)

		hitDep[curAddr] = false

		if currentNode.DetailChild == nil {
			currentNode.DetailChild = &cmptypes.PreplayResTrieNode{Children: make(map[interface{}]*cmptypes.PreplayResTrieNode), Parent: currentNode}
		}

		insertDetail2MixTree(currentNode.DetailChild, rIndex+1, readDepSeq, dIndex, detailSeq, hitDep, depCheckedAddr, noHit+1, round, newLeaves, newNodeCount)

		depCheckedAddr[curAddr] = false
	}
}

func insertDetail2MixTree(currentNode *cmptypes.PreplayResTrieNode, rIndex int, readDep []*cmptypes.AddrLocValue, dIndex int,
	detailSeq []*cmptypes.AddrLocValue, hitDep map[interface{}]bool, depCheckedAddr map[common.Address]bool, noHit int,
	round *cache.PreplayResult, newLeaves *[]cmptypes.ISRefCountNode, newNodeCount *uint) {

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
		insertDetail2MixTree(currentNode, rIndex, readDep, dIndex+1, detailSeq, hitDep, depCheckedAddr, noHit, round, newLeaves, newNodeCount)
		return
	} else {
		detailReadAddr := detailRead.AddLoc.Address
		isNewAddr := !depCheckedAddr[detailReadAddr]
		// to reduce the node number, ifnoHit <= FixedDepCheckCount, only append detail node, regardless of this addr is not checked by deb // magic number
		if isNewAddr && noHit < FixedDepCheckCount && !(noHit > 0 && noHit <= FixedDepCheckCount && rIndex > FixedDepCheckCount+3) {
			insertDep2MixTree(currentNode, rIndex, readDep, dIndex, detailSeq, hitDep, depCheckedAddr, noHit, round, newLeaves, newNodeCount)
		} else {
			if !hitDep[detailReadAddr] {
				var isNew bool
				currentNode, isNew = insertNode(currentNode, detailRead)
				if isNew{
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
			insertDetail2MixTree(currentNode, rIndex, readDep, dIndex+1, detailSeq, hitDep, depCheckedAddr, noHit, round, newLeaves, newNodeCount)
			if isNewAddr {
				depCheckedAddr[detailReadAddr] = false
			}
		}
	}
}

//	 @return    *cmptypes.PreplayResTrieNode	"the child node(which is supposed to be insert)"
//				bool 	"whether a new node is inserted"
func insertNode(currentNode *cmptypes.PreplayResTrieNode, alv *cmptypes.AddrLocValue) (*cmptypes.PreplayResTrieNode, bool) {
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
		child = &cmptypes.PreplayResTrieNode{Children: make(map[interface{}]*cmptypes.PreplayResTrieNode), Parent: currentNode, Value: value}
		currentNode.Children[value] = child
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
		nodeType := currentNode.NodeType
		value := getCurrentValue(nodeType, db, bc, header)
		// assert all kinds of values in the trie are simple types, which is guaranteed in rwhook when recording the readDetail
		childNode, ok := currentNode.Children[value]
		if debug {
			albs, _ := json.Marshal(&cmptypes.AddrLocValue{AddLoc: nodeType, Value: value})
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

func SearchMixTree(trie *cmptypes.PreplayResTrie, db *state.StateDB, bc core.ChainContext, header *types.Header, abort func() bool,
	debug bool, isBlockProcess bool) (round *cache.PreplayResult, mixStatus *cmptypes.MixHitStatus,
	missNode *cmptypes.PreplayResTrieNode, missValue interface{}, isAbort bool, ok bool) {
	currentNode := trie.Root

	if currentNode.NodeType == nil {
		if isBlockProcess {
			return nil, nil, copyNode(currentNode), nil, false, false
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
		nodeType := currentNode.NodeType
		value := getCurrentValue(nodeType, db, bc, header)
		// assert all kinds of values in the trie are simple types, which is guaranteed in rwhook when recording the readDetail
		childNode, ok := currentNode.Children[value]
		if debug {
			albs, _ := json.Marshal(&cmptypes.AddrLocValue{AddLoc: nodeType, Value: value})
			log.Warn("search node type", "ok", ok, "addr", nodeType.Address, "field", nodeType.Field,
				"loc", nodeType.Loc, "value", value, "alv", string(albs))
		}
		if ok {
			currentNode = childNode
			if nodeType.Field == cmptypes.Dependence {
				allDetailMatched = false
				matchedDeps = append(matchedDeps, nodeType.Address)
				depMatchedMap[nodeType.Address] = value
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
				mixStatus = &cmptypes.MixHitStatus{MixHitType: cmptypes.NotMixHit, DepHitAddr: matchedDeps, DepHitAddrMap: depMatchedMap}

				if isBlockProcess {
					return nil, mixStatus, copyNode(currentNode), value, false, false
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
		mixHitType = cmptypes.AllDetailHit
	}

	mixStatus = &cmptypes.MixHitStatus{MixHitType: mixHitType, DepHitAddr: matchedDeps, DepHitAddrMap: depMatchedMap}
	return currentNode.Round.(*cache.PreplayResult), mixStatus, nil, nil, false, true
}

func copyNode(node *cmptypes.PreplayResTrieNode) *cmptypes.PreplayResTrieNode {
	nodeCpy := &cmptypes.PreplayResTrieNode{
		Children: make(map[interface{}]*cmptypes.PreplayResTrieNode, len(node.Children)),
	}
	if node.NodeType != nil {
		nodeCpy.NodeType = &cmptypes.AddrLocation{
			Address: node.NodeType.Address,
			Field:   node.NodeType.Field,
			Loc:     node.NodeType.Loc,
		}
	}
	for child := range node.Children {
		nodeCpy.Children[child] = nil
	}
	return nodeCpy
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
			value = *statedb.GetAccountSnap(addr)
		}
		return value
	case cmptypes.MinBalance:
		return statedb.GetBalance(addr).Cmp(addrLoc.Loc.(*big.Int)) > 0
	default:
		log.Error("wrong field", "field ", addrLoc.Field)
	}
	return nil
}
