package cmpreuse

import (
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

	if record.ReadDetail.IsBlockSensitive && !trie.IsBlockSensitive {
		trie.IsBlockSensitive = true
	}
	if blockNumber > trie.LatestBN {
		trie.LatestBN = blockNumber
	}
	if trie.LeafCount > 1000 {
		trie.Clear()
	}

	currentNode := trie.Root
	seqRecord := record.ReadDetail.ReadDetailSeq
	for _, addrFieldValue := range seqRecord {
		if currentNode.NodeType == nil {
			currentNode.NodeType = addrFieldValue.AddLoc

		} else {
			// TODO: debug code, test well(would not be touched), can be removed
			//key := currentNode.NodeType
			//if !cmp.Equal(key, addrFieldValue.AddLoc) {
			//
			//	log.Warn("nodekey", "addr", key.Address, "field", key.Field, "loc", key.Loc)
			//	log.Warn("addrfield", "addr", addrFieldValue.AddLoc.Address, "filed",
			//		addrFieldValue.AddLoc.Field, "loc", addrFieldValue.AddLoc.Loc)
			//
			//	panic("should be the same key")
			//}
		}
		if currentNode.Children == nil {
			currentNode.Children = make(map[interface{}]*cmptypes.PreplayResTrieNode)
		}
		child, ok := currentNode.Children[addrFieldValue.Value]
		if ok {
			currentNode = child
			continue
		} else {
			child = &cmptypes.PreplayResTrieNode{Children: make(map[interface{}]*cmptypes.PreplayResTrieNode)}
			currentNode.Children[addrFieldValue.Value] = child
			currentNode = child
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
		currentNode.RWRecord = record
		trie.LeafCount += 1
	}
	trie.RoundIds[round.RoundID] = true
}

func SearchPreplayRes(trie *cmptypes.PreplayResTrie, db *state.StateDB, bc core.ChainContext, header *types.Header) (*cmptypes.PreplayResTrieNode, bool) {

	currentNode := trie.Root
	if currentNode.NodeType == nil {
		return nil, false
	}
	for ; !currentNode.IsLeaf; {
		nodeType := currentNode.NodeType
		value := getCurrentValue(nodeType, db, bc, header)
		// assert all kinds of values in the trie are simple types, which is guaranteed in rwhook when recording the readDetail
		childNode, ok := currentNode.Children[value]
		if ok {
			currentNode = childNode
		} else {
			// TODO might search another path

			return nil, false
		}
	}
	return currentNode, true
}

func InsertAccDep(trie *cmptypes.PreplayResTrie, round *cache.PreplayResult, blockNumber uint64, preBlockHash *common.Hash) {
	// TODO optimize the trie clear
	if blockNumber > trie.LatestBN + 2 {
		trie.Clear()
		trie.LatestBN = blockNumber
	}

	var topseq []*cmptypes.AddrLocValue
	topseq = append(topseq, &cmptypes.AddrLocValue{AddLoc: &cmptypes.AddrLocation{Field: cmptypes.Number}, Value: blockNumber})
	topseq = append(topseq, &cmptypes.AddrLocValue{AddLoc: &cmptypes.AddrLocation{Field: cmptypes.PreBlockHash}, Value: *preBlockHash})

	currentNode := trie.Root

	for _, readDep := range append(topseq, round.ReadDeps...) {

		if currentNode.NodeType == nil {
			currentNode.NodeType = readDep.AddLoc
		} else {
			//// TODO: debug code
			//if currentNode.NodeType.Address != readDep.AddLoc.Address || currentNode.NodeType.Field != readDep.AddLoc.Field {
			//	readdetailbs, _ := json.Marshal(record.ReadDetail)
			//	log.Warn("curTx", "txhash", round.TxHash)
			//	log.Warn("readdetail", string(readdetailbs))
			//	log.Warn("currentNode vs new Node", "curNodeAddress", currentNode.NodeType.Address,
			//		"newAddress", readDep.AddLoc.Address, "field", currentNode.NodeType.Field)
			//	panic("there should be the same key in the dep tree")
			//}
		}

		if currentNode.Children == nil {
			currentNode.Children = make(map[interface{}]*cmptypes.PreplayResTrieNode)
		}

		var value interface{}
		if currentNode.NodeType.Field == cmptypes.Dependence{
			value = readDep.Value.(*cmptypes.ChangedBy).Hash()
		}else{
			value = readDep.Value
		}
		child, ok := currentNode.Children[value]

		if ok {
			currentNode = child
			continue
		} else {
			child = &cmptypes.PreplayResTrieNode{Children: make(map[interface{}]*cmptypes.PreplayResTrieNode)}
			currentNode.Children[value] = child
			currentNode = child

		}
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
	} else {
		currentNode.IsLeaf = true
		currentNode.Round = round
		trie.LeafCount += 1
	}
	trie.RoundIds[round.RoundID] = true
}

func SearchAccDep(trie *cmptypes.PreplayResTrie, db *state.StateDB, bc core.ChainContext, header *types.Header) (*cmptypes.PreplayResTrieNode, bool) {

	currentNode := trie.Root
	if currentNode.NodeType == nil {
		return nil, false
	}

	for ; !currentNode.IsLeaf; {
		nodeType := currentNode.NodeType

		value := getCurrentValue(nodeType, db, bc, header)
		// assert all kinds of values in the trie are simple types, which is guaranteed in rwhook when recording the readDetail
		childNode, ok := currentNode.Children[value]
		if ok {
			currentNode = childNode
		} else {
			// TODO might search another path

			return nil, false
		}
	}
	return currentNode, true
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
		getHashFn := core.GetHashFn(header, bc)
		number := addrLoc.Loc.(uint64)
		return getHashFn(number)
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
		return statedb.GetCodeHash(addr)
	case cmptypes.Storage:
		position := addrLoc.Loc.(common.Hash)
		return statedb.GetState(addr, position)
	case cmptypes.CommittedStorage:
		position := addrLoc.Loc.(common.Hash)
		return statedb.GetCommittedState(addr, position)
	case cmptypes.Dependence:
		var value common.Hash
		changedBy, ok := statedb.AccountChangedBy[addr]
		if ok {
			value = changedBy.Hash()
		} else {
			value = common.Hash{}
		}
		return value
	default:
		log.Error("wrong field", "field ", addrLoc.Field)
	}
	return nil
}

// Deprecated
//func GetRChainField(loc *cmptypes.AddrLocation, bc core.ChainContext, header *types.Header) interface{} {
//	// assert loc.isChain is true
//	switch loc.Field {
//	case cmptypes.Coinbase:
//		return header.Coinbase
//	case cmptypes.Timestamp:
//		return header.Time
//	case cmptypes.Number:
//		return header.Number.Uint64()
//	case cmptypes.Difficulty:
//		return common.BigToHash(header.Difficulty)
//	case cmptypes.GasLimit:
//		return header.GasLimit
//	case cmptypes.Blockhash:
//		getHashFn := core.GetHashFn(header, bc)
//		number := loc.Loc.(uint64)
//		return getHashFn(number)
//	case cmptypes.PreBlockHash:
//		return header.ParentHash
//	default:
//		log.Error("wrong chain field", "field ", loc.Field)
//	}
//	return nil
//}

// Deprecated
//func GetRStateValue(addrLoc *cmptypes.AddrLocation, statedb *state.StateDB) interface{} {
//	addr := addrLoc.Address
//	switch addrLoc.Field {
//	case cmptypes.Exist:
//		return statedb.Exist(addr)
//	case cmptypes.Empty:
//		return statedb.Empty(addr)
//	case cmptypes.Balance:
//		return common.BigToHash(statedb.GetBalance(addr)) // // convert complex type (big.Int) to simple type: bytes[32]
//	case cmptypes.Nonce:
//		return statedb.GetNonce(addr)
//	case cmptypes.CodeHash:
//		return statedb.GetCodeHash(addr)
//	case cmptypes.Storage:
//		position := addrLoc.Loc.(common.Hash)
//		return statedb.GetState(addr, position)
//	case cmptypes.CommittedStorage:
//		position := addrLoc.Loc.(common.Hash)
//		return statedb.GetCommittedState(addr, position)
//	default:
//		log.Error("wrong state field", "field ", addrLoc.Field)
//
//	}
//	return nil
//}
