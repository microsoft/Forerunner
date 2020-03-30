package optipreplayer

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"time"
)

var upperLimit = new(big.Int).SetUint64(math.MaxUint64)

type MissReporter struct {
	block         *types.Block
	parent        *types.Block
	txnsIndexMap  map[common.Hash]int
	txnsSenderMap map[common.Hash]common.Address
	txnsMap       map[common.Hash]*types.Transaction
	groundGroups  map[common.Hash]*TxnGroup
	getRWRecord   func(txn common.Hash) *RWRecord

	nilNodeTypeCount int
	rChainMissCount  [4]int
	rStateMissCount  [2]int
	groupMissCount   [2]int
	sortMissCount    [2]int
	chainMissCount   [2]int
	snapMissCount    [2]int
	orderMissTotal   int
	orderMissCount   [7]int
	minerMissCount   [2]int
	rootMissCount    [2]int
	bugMissCount     [2]int

	preplayer *Preplayer
}

func NewMissReporter() *MissReporter {
	return new(MissReporter)
}

func (r *MissReporter) SetBlock(block *types.Block) {
	r.block = block
	r.txnsIndexMap = make(map[common.Hash]int)
	r.txnsSenderMap = make(map[common.Hash]common.Address)
	r.txnsMap = make(map[common.Hash]*types.Transaction)
	for index, txn := range block.Transactions() {
		r.txnsIndexMap[txn.Hash()] = index
		sender, _ := types.Sender(r.preplayer.taskBuilder.signer, txn)
		r.txnsSenderMap[txn.Hash()] = sender
		r.txnsMap[txn.Hash()] = txn
	}
	r.parent, r.groundGroups, r.getRWRecord = r.preplayer.taskBuilder.getGroundGroup(block, r.txnsIndexMap, r.txnsSenderMap)
}

func (r *MissReporter) SetMissTxn(txn *types.Transaction, node *cmptypes.PreplayResTrieNode, value interface{}) {
	if node.NodeType == nil {
		r.nilNodeTypeCount++
		return
	}
	nodeType := node.NodeType
	if cmptypes.IsChainField(nodeType.Field) {
		switch nodeType.Field {
		case cmptypes.Coinbase:
			r.rChainMissCount[0]++
		case cmptypes.Timestamp:
			r.rChainMissCount[1]++
		case cmptypes.GasLimit:
			r.rChainMissCount[2]++
		default:
			r.rChainMissCount[3]++
		}
		return
	}
	isStorageMiss := nodeType.Field == cmptypes.Storage || nodeType.Field == cmptypes.CommittedStorage
	if isStorageMiss {
		r.rStateMissCount[1]++
	} else {
		r.rStateMissCount[0]++
	}
	index := r.txnsIndexMap[txn.Hash()]
	sender := r.txnsSenderMap[txn.Hash()]
	groundGroup, groundOrder := r.searchGroundGroup(txn, sender)
	poolBefore, orderBefore := r.getTxnBefore(txn, groundOrder)

	groupHitGroup, _ := r.searchGroupHitGroup(txn, sender, poolBefore)
	if len(groupHitGroup) == 0 {
		if isStorageMiss {
			r.groupMissCount[1]++
		} else {
			r.groupMissCount[0]++
		}
		//log.Info("Report reuse miss: missing txns", "tx", txn.Hash(), "index", index,
		//	"relative->hit", fmt.Sprintf("%d->0", relativeSize))
		return
	}

	if !r.isSortHit(poolBefore.copy()) {
		if isStorageMiss {
			r.sortMissCount[1]++
		} else {
			r.sortMissCount[0]++
		}
		//log.Info("Report reuse miss: N&P violation", "tx", txn.Hash(), "index", index,
		//	"poolBeforeSize", poolBefore.size(), "orderBeforeSize", len(orderBefore),
		//	"poolBefore", poolBefore, "orderBefore", orderBefore)
		return
	}

	chainHitGroup := r.searchChainHitGroup(groupHitGroup)
	if len(chainHitGroup) == 0 {
		if isStorageMiss {
			r.chainMissCount[1]++
		} else {
			r.chainMissCount[0]++
		}
		//pickGroup := pickOneGroup(groupHitGroup)
		//context := []interface{}{"tx", txn.Hash(), "index", index, "groupHitGroupNum", len(groupHitGroup),
		//	"isDep", fmt.Sprintf("%v:%v", pickGroup.isCoinbaseDep(), pickGroup.isTimestampDep())}
		//if pickGroup.isCoinbaseDep() {
		//	var minerStr string
		//	for index, miner := range r.preplayer.taskBuilder.minerList.top5Active {
		//		if index > 0 {
		//			minerStr += ","
		//		}
		//		minerStr += miner.Hex()
		//	}
		//	context = append(context, "groundCoinbase", r.block.Header().Coinbase.Hex(), "activeMiner", minerStr)
		//}
		//if pickGroup.isTimestampDep() {
		//	context = append(context, "groundTimestamp", r.block.Header().Time, "forecastTimestamp", pickGroup.header.time)
		//}
		//log.Info("Report reuse miss: chain miss", context...)
		return
	}

	snapHitGroup, orderHitGroup := r.searchSnapAndOrderHitGroup(chainHitGroup, groundOrder, orderBefore)
	if len(snapHitGroup) == 0 {
		if isStorageMiss {
			r.snapMissCount[1]++
		} else {
			r.snapMissCount[0]++
		}
		//pickGroup := pickOneGroup(chainHitGroup)
		//log.Info("Report reuse miss: extra txns", "tx", txn.Hash(), "index", index, "chainHitGroupNum", len(chainHitGroup),
		//	"pickGroupSize", pickGroup.txnCount, "pickGroupSubSize", len(pickGroup.subpoolList),
		//	"group.txns", pickGroup.txns, "groundOrder", groundOrder)
		return
	}

	if len(orderHitGroup) == 0 {
		var (
			preplayRate = -1.0
			pickGroup   = pickOneGroup(snapHitGroup)
		)
		for _, group := range snapHitGroup {
			var nowRate float64
			if group.orderCount.Cmp(upperLimit) <= 0 {
				nowRate = float64(group.preplayCount) / float64(group.orderCount.Uint64())
			}
			if nowRate > preplayRate {
				preplayRate = nowRate
				pickGroup = group
			}
		}
		r.orderMissTotal++
		if pickGroup.orderCount.Cmp(upperLimit) > 0 {
			r.orderMissCount[6]++
		} else {
			orderCount := pickGroup.orderCount.Uint64()
			switch {
			case orderCount <= 1:
				r.orderMissCount[0]++
			case orderCount <= 2:
				r.orderMissCount[1]++
			case orderCount <= 6:
				r.orderMissCount[2]++
			case orderCount <= 24:
				r.orderMissCount[3]++
			case orderCount <= 120:
				r.orderMissCount[4]++
			case orderCount <= 720:
				r.orderMissCount[5]++
			default:
				r.orderMissCount[6]++
			}
		}
		//log.Info("Report reuse miss: insufficient execution", "tx", txn.Hash(), "index", index, "snapHitGroupNum", len(snapHitGroup),
		//	"pickGroupSize", pickGroup.txnCount, "pickGroupSubSize", len(pickGroup.subpoolList),
		//	"group.txns", pickGroup.txns, "groundOrder", groundOrder,
		//	"pickPreplayMost", fmt.Sprintf("%d(%.2f%%)->%s", pickGroup.preplayCount, preplayRate*100, pickGroup.orderCount.String()))
		return
	}

	if sender == r.block.Coinbase() {
		if isStorageMiss {
			r.minerMissCount[1]++
		} else {
			r.minerMissCount[0]++
		}
		//log.Info("Report reuse miss: miner miss", "tx", txn.Hash(), "index", index, "orderHitGroupNum", len(orderHitGroup),
		//	"sender", sender, "coinbase", r.block.Coinbase())
		return
	}

	rootHitGroup := r.searchRootHitGroup(orderHitGroup)
	if len(rootHitGroup) == 0 {
		if isStorageMiss {
			r.rootMissCount[1]++
		} else {
			r.rootMissCount[0]++
		}
		pickGroup := pickOneGroup(orderHitGroup)
		log.Info("Report reuse miss: root miss", "tx", txn.Hash(), "index", index, "orderHitGroupNum", len(orderHitGroup),
			"block.parent.root", r.parent.Root(), "group.parent.root", pickGroup.parent.Root())
		return
	}

	if isStorageMiss {
		r.bugMissCount[1]++
	} else {
		r.bugMissCount[0]++
	}
	pickGroup := pickOneGroup(rootHitGroup)
	nodeTypeStr := fmt.Sprintf("%s.%v", nodeType.Address.Hex(), nodeType.Field.String())
	if nodeType.Field == cmptypes.CommittedStorage || nodeType.Field == cmptypes.Storage {
		nodeTypeStr += "." + nodeType.Loc.(common.Hash).Hex()
	}
	nodeChildStr := fmt.Sprintf("%d:", len(node.Children))
	var first = true
	for value := range node.Children {
		if first {
			nodeChildStr += "{"
			first = false
		} else {
			nodeChildStr += ","
		}
		nodeChildStr += getInterfaceValue(value)
	}
	if len(node.Children) > 0 {
		nodeChildStr += "}"
	}
	var groupTxns = "TooLarge"
	if pickGroup.txnCount <= 20 {
		groupTxns = pickGroup.txns.String()
	}
	log.Info("Report reuse miss: bug", "tx", txn.Hash().Hex(), "index", index,
		"nodeType", nodeTypeStr, "nodeChild", nodeChildStr, "missValue", getInterfaceValue(value),
		"rootHitGroupNum", len(rootHitGroup), "pickGroupSize", pickGroup.txnCount, "pickGroupSubSize", len(pickGroup.subpoolList),
		"pickPreplayMost", fmt.Sprintf("%d->%s", pickGroup.preplayCount, pickGroup.orderCount.String()),
		"preplayedOrder", pickGroup.preplayHistory[0],
		"group.txns", groupTxns, "orderBefore", orderBefore,
	)
	group := r.groundGroups[groundGroup.hash]
	log.Info("Ground group info", "hash", group.hash, "txnCount", group.txnCount,
		"txns", group.txns, "order", groundOrder)
	for _, txns := range group.txns {
		for _, txn := range txns {
			log.Info("Transaction info", "tx", txn.Hash().Hex(), "index", r.txnsIndexMap[txn.Hash()],
				"sender", r.txnsSenderMap[txn.Hash()], "rwrecord", r.getRWRecord(txn.Hash()).String())
		}
	}
}

func (r *MissReporter) ReportMiss() {
	context := []interface{}{
		"nilNodeType", r.nilNodeTypeCount,
		"rChainMiss", fmt.Sprintf("%d(%d:%d:%d:%d)", r.rChainMissCount[0]+r.rChainMissCount[1]+r.rChainMissCount[2]+r.rChainMissCount[3],
			r.rChainMissCount[0], r.rChainMissCount[1], r.rChainMissCount[2], r.rChainMissCount[3]),
		"rStateMiss", fmt.Sprintf("%d(%d:%d)", r.rStateMissCount[0]+r.rStateMissCount[1], r.rStateMissCount[0], r.rStateMissCount[1]),
	}
	groupMissCount := r.groupMissCount[0] + r.groupMissCount[1]
	if groupMissCount > 0 {
		context = append(context, "missing txns", fmt.Sprintf("%d(%d:%d)", groupMissCount, r.groupMissCount[0], r.groupMissCount[1]))
	}
	sortMissCount := r.sortMissCount[0] + r.sortMissCount[1]
	if sortMissCount > 0 {
		context = append(context, "N&P violation", fmt.Sprintf("%d(%d:%d)", sortMissCount, r.sortMissCount[0], r.sortMissCount[1]))
	}
	chainMissCount := r.chainMissCount[0] + r.chainMissCount[1]
	if chainMissCount > 0 {
		context = append(context, "chainMiss", fmt.Sprintf("%d(%d:%d)", chainMissCount, r.chainMissCount[0], r.chainMissCount[1]))
	}
	snapMissCount := r.snapMissCount[0] + r.snapMissCount[1]
	if snapMissCount > 0 {
		context = append(context, "extra txns", fmt.Sprintf("%d(%d:%d)", snapMissCount, r.snapMissCount[0], r.snapMissCount[1]))
	}
	context = append(context,
		"insufficient execution", fmt.Sprintf("%d:[1|2|6|24|120|720 >720]-%v", r.orderMissTotal, r.orderMissCount),
	)
	minerMissCount := r.minerMissCount[0] + r.minerMissCount[1]
	if minerMissCount > 0 {
		context = append(context, "minerMiss", fmt.Sprintf("%d(%d:%d)", minerMissCount, r.minerMissCount[0], r.minerMissCount[1]))
	}
	rootMissCount := r.rootMissCount[0] + r.rootMissCount[1]
	if rootMissCount > 0 {
		context = append(context, "rootMiss", fmt.Sprintf("%d(%d:%d)", rootMissCount, r.rootMissCount[0], r.rootMissCount[1]))
	}
	bugMissCount := r.bugMissCount[0] + r.bugMissCount[1]
	if bugMissCount > 0 {
		context = append(context, "bugMiss", fmt.Sprintf("%d(%d:%d)", bugMissCount, r.bugMissCount[0], r.bugMissCount[1]))
	}
	log.Info("Cumulative miss statistics", context...)
}

func (b *TaskBuilder) getGroundGroup(block *types.Block, txnsIndexMap map[common.Hash]int, txnsSenderMap map[common.Hash]common.Address) (*types.Block,
	map[common.Hash]*TxnGroup, func(txn common.Hash) *RWRecord) {
	parentHash := block.ParentHash()
	parent := b.chain.GetBlockByHash(parentHash)

	pendingTxn := make(TransactionPool)
	pendingList := types.Transactions{}
	for _, txn := range block.Transactions() {
		sender := txnsSenderMap[txn.Hash()]
		pendingTxn[sender] = append(pendingTxn[sender], txn)
		pendingList = append(pendingList, txn)
		if b.globalCache.GetTxPreplay(txn.Hash()) == nil {
			b.globalCache.CommitTxPreplay(cache.NewTxPreplay(txn))
		}
	}

	totalDifficulty := b.chain.GetTd(parentHash, parent.NumberU64())
	if totalDifficulty == nil {
		totalDifficulty = new(big.Int)
	}
	currentState := &cache.CurrentState{
		PreplayName:       b.trigger.Name,
		Number:            parent.NumberU64(),
		Hash:              parentHash.Hex(),
		RawHash:           parentHash,
		Txs:               parent.Transactions(),
		TotalDifficulty:   totalDifficulty.String(),
		SnapshotTimestamp: time.Now().UnixNano() / 1000000,
	}

	executor := NewExecutor("0", b.config, b.engine, b.chain, b.eth.ChainDb(), txnsIndexMap,
		pendingTxn, pendingList, currentState, b.trigger, nil, false)

	executor.RoundID = b.globalCache.NewRoundID()

	// Execute, use pending for preplay
	executor.commit(block.Coinbase(), parent, block.Header(), pendingTxn)

	rwrecords := make(map[common.Hash]*RWRecord)
	for _, txns := range pendingTxn {
		for _, txn := range txns {
			txnHash := txn.Hash()
			txPreplay := b.globalCache.GetTxPreplay(txnHash)
			if txPreplay != nil {
				if round, ok := txPreplay.PeekRound(executor.RoundID); ok {
					rwrecords[txnHash] = NewRWRecord(round.RWrecord)
					continue
				}
			}
			rwrecords[txnHash] = NewRWRecord(nil)
		}
	}

	getRWRecord := func(txn common.Hash) *RWRecord {
		if rwrecord, ok := rwrecords[txn]; ok {
			return rwrecord
		} else {
			rwrecords[txn] = NewRWRecord(nil)
			return rwrecords[txn]
		}
	}
	isSameGroup := func(sender common.Address, txn common.Hash, group *TxnGroup) bool {
		if _, ok := group.txns[sender]; ok {
			return true
		}
		rwrecord := getRWRecord(txn)
		return rwrecord.isRWOverlap(group.RWRecord) || group.isRWOverlap(rwrecord)
	}

	nowGroups := make(map[common.Hash]*TxnGroup)
	for from, txns := range pendingTxn {
		relativeGroup := make(map[common.Hash]struct{})
		for _, txn := range txns {
			for groupHash, group := range nowGroups {
				if _, ok := relativeGroup[groupHash]; ok {
					continue
				}
				if isSameGroup(from, txn.Hash(), group) {
					relativeGroup[groupHash] = struct{}{}
				}
			}
		}

		newGroup := make(TransactionPool)
		rwrecord := NewRWRecord(nil)
		var res [][]byte
		for groupHash := range relativeGroup {
			addGroup := nowGroups[groupHash]
			rwrecord.merge(addGroup.RWRecord)
			for addr, txns := range addGroup.txns {
				newGroup[addr] = append(newGroup[addr], txns...)
				for _, txn := range txns {
					res = append(res, txn.Hash().Bytes())
				}
			}
			delete(nowGroups, groupHash)
		}
		newGroup[from] = append(newGroup[from], txns...)
		for _, txn := range txns {
			rwrecord.merge(getRWRecord(txn.Hash()))
			res = append(res, txn.Hash().Bytes())
		}
		sort.Slice(res, func(i, j int) bool {
			return bytes.Compare(res[i], res[j]) == -1
		})
		groupHash := crypto.Keccak256Hash(res...)
		nowGroups[groupHash] = &TxnGroup{
			hash:     groupHash,
			txns:     newGroup,
			RWRecord: rwrecord,
		}
	}
	for _, group := range nowGroups {
		group.txnCount = group.txns.size()
	}
	return parent, nowGroups, getRWRecord
}

func (r *MissReporter) searchGroundGroup(currentTxn *types.Transaction, sender common.Address) (*TxnGroup, TxnOrder) {
	var (
		groundGroup *TxnGroup
		groundOrder TxnOrder
		sum         int
	)
	for _, group := range r.groundGroups {
		sum += group.txns.size()
		for _, txn := range group.txns[sender] {
			if txn.Hash() == currentTxn.Hash() {
				if groundGroup != nil {
					panic("Find duplicate ground group for tx:" + currentTxn.Hash().Hex() + ",sum:" + strconv.Itoa(sum))
				}
				groundGroup = group
			}
		}
	}
	if groundGroup == nil {
		for _, group := range r.groundGroups {
			for from, txns := range group.txns {
				for _, txn := range txns {
					if txn.Hash() == currentTxn.Hash() {
						groundGroup = group
						log.Error(fmt.Sprintf("Find ground group in sender:%s, actual sender:%s", from.Hex(), sender.Hex()))
					}
				}
			}
		}
		if groundGroup == nil {
			panic("Can not find ground group for tx:" + currentTxn.Hash().Hex() + ",sum:" + strconv.Itoa(sum))
		}
	}
	groundOrder = make([]common.Hash, 0, groundGroup.txnCount)
	for _, txns := range groundGroup.txns {
		for _, txn := range txns {
			groundOrder = append(groundOrder, txn.Hash())
		}
	}
	sort.Slice(groundOrder, func(i, j int) bool {
		return r.txnsIndexMap[groundOrder[i]] < r.txnsIndexMap[groundOrder[j]]
	})
	return groundGroup, groundOrder
}

func (r *MissReporter) getTxnBefore(currentTxn *types.Transaction, order TxnOrder) (TransactionPool, TxnOrder) {
	beforeTxnPool := make(TransactionPool)
	beforeTxnOrder := make([]common.Hash, 0)
	for _, hash := range order {
		sender := r.txnsSenderMap[hash]
		beforeTxnPool[sender] = append(beforeTxnPool[sender], r.txnsMap[hash])
		beforeTxnOrder = append(beforeTxnOrder, hash)
		if hash == currentTxn.Hash() {
			break
		}
	}
	return beforeTxnPool, beforeTxnOrder
}

func (r *MissReporter) searchGroupHitGroup(currentTxn *types.Transaction, sender common.Address, poolBefore TransactionPool) (map[common.Hash]*TxnGroup, int) {
	var relativeGroup = make(map[common.Hash]*TxnGroup)
	r.preplayer.taskBuilder.pastGroupsMu.RLock()
	for groupHash, group := range r.preplayer.taskBuilder.pastGroups {
		for _, txn := range group.txns[sender] {
			if txn.Hash() == currentTxn.Hash() {
				relativeGroup[groupHash] = group
				break
			}
		}
	}
	r.preplayer.taskBuilder.pastGroupsMu.RUnlock()
	hitGroup := make(map[common.Hash]*TxnGroup)
	for groupHash, group := range relativeGroup {
		if !poolBefore.isTxnsPoolLarge(group.txns) {
			hitGroup[groupHash] = group
		}
	}
	return hitGroup, len(relativeGroup)
}

func (r *MissReporter) isSortHit(groundPool TransactionPool) bool {
	txnHeap := types.NewTransactionsByPriceAndNonce(r.preplayer.taskBuilder.signer, groundPool, nil, true)
	var nowPrice = txnHeap.Peek().GasPrice()
	var (
		lastIndex = -1
		nowIndex  = r.txnsIndexMap[txnHeap.Peek().Hash()]
	)
	for txnHeap.Peek() != nil {
		txn := txnHeap.Peek()
		txnPrice := txn.GasPrice()
		txnIndex := r.txnsIndexMap[txn.Hash()]
		if txnPrice.Cmp(nowPrice) < 0 {
			nowPrice = txnPrice
			lastIndex = nowIndex
		} else {
			if txnIndex < lastIndex {
				return false
			}
			if nowIndex < txnIndex {
				nowIndex = txnIndex
			}
		}
		txnHeap.Shift()
	}
	return true
}

func (r *MissReporter) searchChainHitGroup(hitGroup map[common.Hash]*TxnGroup) map[common.Hash]*TxnGroup {
	chainHitGroup := make(map[common.Hash]*TxnGroup)
	for groupHash, group := range hitGroup {
		if group.isCoinbaseDep() {
			miss := true
			for _, miner := range r.preplayer.taskBuilder.minerList.top5Active {
				if r.block.Header().Coinbase == miner {
					miss = false
					break
				}
			}
			if miss {
				continue
			}
		}
		if group.isTimestampDep() {
			miss := true
			for _, shift := range timeShift {
				if r.block.Header().Time == uint64(int(group.header.time)+shift) {
					miss = false
					break
				}
			}
			if miss {
				continue
			}
		}
		chainHitGroup[groupHash] = group
	}
	return chainHitGroup
}

func (r *MissReporter) searchSnapAndOrderHitGroup(chainHitGroup map[common.Hash]*TxnGroup, groundOrder, orderBefore TxnOrder) (map[common.Hash]*TxnGroup, map[common.Hash]*TxnGroup) {
	snapHitGroup := make(map[common.Hash]*TxnGroup)
	orderHitGroup := make(map[common.Hash]*TxnGroup)
	for hash, group := range chainHitGroup {
		if snapHit, orderHit := group.evaluatePreplay(groundOrder, orderBefore); snapHit {
			snapHitGroup[hash] = group
			if orderHit {
				orderHitGroup[hash] = group
			}
		}
	}
	return snapHitGroup, orderHitGroup
}

func (r *MissReporter) searchRootHitGroup(orderHitGroup map[common.Hash]*TxnGroup) map[common.Hash]*TxnGroup {
	rootHitGroup := make(map[common.Hash]*TxnGroup)
	for hash, group := range orderHitGroup {
		if r.parent.Root() == group.parent.Root() {
			rootHitGroup[hash] = group
		}
	}
	return rootHitGroup
}

//func (r *MissReporter) getRanking(beforeOrder TxnOrder, group *TxnGroup, debug bool) (int64, bool) {
//	start := time.Now()
//	var isPrint bool
//	groundOrderMap := make(map[common.Hash]int, len(beforeOrder))
//	for i, hash := range beforeOrder {
//		groundOrderMap[hash] = i
//	}
//	nextInList := make([]map[common.Address]int, len(group.subpoolList))
//	for i := range nextInList {
//		nextInList[i] = make(map[common.Address]int, group.subpoolList[i].size())
//		for from := range group.subpoolList[i] {
//			nextInList[i][from] = 0
//		}
//	}
//	isSubInOrder := func(subpoolIndex int) bool {
//		subPool := group.subpoolList[subpoolIndex]
//		nextIn := nextInList[subpoolIndex]
//		for from, txns := range subPool {
//			if nextIn[from] < len(txns) {
//				return false
//			}
//		}
//		return true
//	}
//	rank := int64(-1)
//	overflow := false
//	rankCount := uint64(1)
//	finish := false
//	var walkTxnsPool func(subpoolLoc int, txnLoc int, isMatch bool, lastIndex int)
//	walkTxnsPool = func(subpoolLoc int, txnLoc int, isMatch bool, lastIndex int) {
//		if time.Since(start) > time.Minute && !isPrint {
//			log.Info("Detect long getRanking", "beforeOrder", beforeOrder.String(), "group", group.txns.String(),
//				"rank", rank, "rankCount", rankCount, "overflow", overflow, "finish", finish)
//			debug = true
//			isPrint = true
//		}
//		if time.Since(start) > 2*time.Minute && isPrint {
//			debug = false
//		}
//		if debug {
//			log.Info("In walkTxnsPool", "subpoolLoc", subpoolLoc, "txnLoc", txnLoc, "isMatch", isMatch, "lastIndex", lastIndex)
//		}
//		if finish {
//			return
//		}
//		if rankCount > uint64(group.preplayCount) && rankCount >= 1000 {
//			finish = true
//			rank = int64(rankCount)
//			overflow = true
//			return
//		}
//		if isSubInOrder(subpoolLoc) {
//			if subpoolLoc == 0 {
//				if isMatch {
//					if group.isChainDep() {
//						var (
//							coinbaseTryCount  = 1
//							timestampTryCount = 1
//						)
//						if group.isCoinbaseDep() {
//							coinbaseTryCount = len(r.preplayer.taskBuilder.minerList.top5Active)
//						}
//						if group.isTimestampDep() {
//							timestampTryCount = len(timeShift)
//						}
//						for i := 0; i < coinbaseTryCount; i++ {
//							for j := 0; j < timestampTryCount; j++ {
//								if !(group.isCoinbaseDep() && r.block.Header().Coinbase != r.preplayer.taskBuilder.minerList.top5Active[i] ||
//									group.isTimestampDep() && r.block.Header().Time != uint64(int(group.header.time)+timeShift[j])) {
//									rank = int64(rankCount)
//									finish = true
//									return
//								}
//								rankCount++
//							}
//						}
//						if debug {
//							log.Info("Reach impossible", "ground", r.block.Header().Time, "forecast", group.header.time)
//						}
//					} else {
//						rank = int64(rankCount)
//						finish = true
//					}
//				} else {
//					rankCount += uint64(group.chainFactor)
//				}
//				return
//			} else {
//				if isMatch {
//					subpoolLoc--
//					txnLoc = group.startList[subpoolLoc]
//					lastIndex = -1
//				} else {
//					count := uint64(group.chainFactor)
//					for i := 0; i < subpoolLoc; i++ {
//						count *= group.orderCountList[i].Uint64()
//					}
//					rankCount += count
//					return
//				}
//			}
//		}
//
//		subpool := group.subpoolList[subpoolLoc]
//		nextIn := nextInList[subpoolLoc]
//		var (
//			maxPriceFrom = make([]common.Address, 0)
//			maxPrice     = new(big.Int)
//		)
//		for from, txns := range subpool {
//			nextIndex := nextIn[from]
//			if nextIndex >= len(txns) {
//				continue
//			}
//			headTxn := txns[nextIndex]
//			headGasPrice := headTxn.GasPrice()
//			cmp := headGasPrice.Cmp(maxPrice)
//			switch {
//			case cmp > 0:
//				maxPrice = headGasPrice
//				maxPriceFrom = []common.Address{from}
//			case cmp == 0:
//				maxPriceFrom = append(maxPriceFrom, from)
//			}
//		}
//		sort.Slice(maxPriceFrom, func(i, j int) bool {
//			return bytes.Compare(maxPriceFrom[i].Bytes(), maxPriceFrom[j].Bytes()) < 0
//		})
//		for _, from := range maxPriceFrom {
//			txn := subpool[from][nextIn[from]]
//			nextIn[from]++
//			if debug {
//				log.Info("Pick", "txn", txn.Hash(), "txnLoc", txnLoc, "from count", len(maxPriceFrom))
//			}
//			if isMatch {
//				if index, ok := groundOrderMap[txn.Hash()]; ok {
//					if lastIndex < index {
//						walkTxnsPool(subpoolLoc, txnLoc+1, true, index)
//					} else {
//						walkTxnsPool(subpoolLoc, txnLoc+1, false, index)
//					}
//				} else {
//					if len(beforeOrder) <= txnLoc {
//						walkTxnsPool(subpoolLoc, txnLoc+1, true, lastIndex)
//					} else {
//						walkTxnsPool(subpoolLoc, txnLoc+1, false, lastIndex)
//					}
//				}
//			} else {
//				walkTxnsPool(subpoolLoc, txnLoc+1, false, lastIndex)
//			}
//			nextIn[from]--
//		}
//	}
//	lastSubpoolIndex := len(group.subpoolList) - 1
//	if debug {
//		log.Info("Start walk", "lastSubpoolIndex", lastSubpoolIndex)
//	}
//	if len(group.subpoolList) == 0 {
//		log.Info("Detect subpool zero", "txn", group.txns, "count", group.txnCount)
//	} else {
//		walkTxnsPool(lastSubpoolIndex, group.startList[lastSubpoolIndex], true, -1)
//	}
//	return rank, overflow
//}

func pickOneGroup(groupMap map[common.Hash]*TxnGroup) *TxnGroup {
	for _, group := range groupMap {
		return group
	}
	return nil
}

func getInterfaceValue(value interface{}) string {
	val := reflect.ValueOf(value)
	typ := reflect.Indirect(val).Type()
	switch v := value.(type) {
	case uint64:
		return fmt.Sprintf("%s:%d", typ.String(), v)
	case common.Hash:
		return fmt.Sprintf("%s:%s:%s", typ.String(), v.Hex(), v.Big().String())
	default:
		return fmt.Sprintf("%s:%v", typ.String(), v)
	}
}
