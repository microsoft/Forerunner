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

	noPreplay      int
	noGroupPreplay int
	noExecPreplay  int

	miss    int
	txnType [3]int

	groupMissCount   int
	execMissCount    int
	nilNodeTypeCount int
	rChainMissTotal  int
	rChainMissCount  [6]int
	noTimeDepCount   int
	timeMissMap      map[[2]int]int

	rStateMissCount [2]int
	txnMissCount    [2]int
	sortMissCount   [2]int
	chainMissCount  [2]int
	snapMissCount   [2]int
	orderMissTotal  int
	orderMissCount  [7]int
	minerMissCount  [2]int
	rootMissCount   [2]int
	bugMissCount    [2]int

	signer           types.Signer
	reportMissDetail bool
	preplayer        *Preplayer
}

func NewMissReporter(chainID *big.Int, reportMissDetail bool) *MissReporter {
	r := new(MissReporter)
	r.signer = types.NewEIP155Signer(chainID)
	r.reportMissDetail = reportMissDetail
	r.timeMissMap = make(map[[2]int]int)
	return r
}

func (r *MissReporter) SetBlock(block *types.Block) {
	r.block = block
	r.txnsIndexMap = make(map[common.Hash]int)
	r.txnsSenderMap = make(map[common.Hash]common.Address)
	r.txnsMap = make(map[common.Hash]*types.Transaction)
	for index, txn := range block.Transactions() {
		r.txnsIndexMap[txn.Hash()] = index
		sender, _ := types.Sender(r.signer, txn)
		r.txnsSenderMap[txn.Hash()] = sender
		r.txnsMap[txn.Hash()] = txn
	}
	if r.reportMissDetail {
		r.parent, r.groundGroups, r.getRWRecord = r.preplayer.getGroundGroup(block, r.txnsIndexMap, r.txnsSenderMap)
	}
}

func (r *MissReporter) SetNoPreplayTxn(txn *types.Transaction, enqueue uint64) {
	index := r.txnsIndexMap[txn.Hash()]
	sender := r.txnsSenderMap[txn.Hash()]
	enqueueTime := time.Unix(int64(enqueue/1e9), int64(enqueue%1e9))
	enqueueStr := enqueueTime.Format("2006-01-02 15:04:05")
	duration := common.PrettyDuration(time.Since(enqueueTime))
	groupHitGroup := r.preplayer.preplayLog.searchGroupHitGroup(txn, sender)
	if len(groupHitGroup) == 0 {
		r.noGroupPreplay++
		log.Info("Report reuse no preplay: NoGroup", "tx", txn.Hash().Hex(), "index", index, "enqueue", enqueueStr, "duration", duration)
		return
	}
	execHitGroup := r.searchExecHitGroup(txn, groupHitGroup)
	if len(execHitGroup) == 0 {
		r.noExecPreplay++
		pickGroup := pickOneGroup(groupHitGroup)
		log.Info("Report reuse no preplay: NoExec", "tx", txn.Hash().Hex(), "index", index, "enqueue", enqueueStr, "duration", duration,
			"reason", pickGroup.getTxnFailReason(txn.Hash()))
		return
	}
	log.Info("Report reuse no preplay: bug", "tx", txn.Hash().Hex(), "index", index, "enqueue", enqueueStr, "duration", duration)
}

func (r *MissReporter) SetMissTxn(txn *types.Transaction, node *cmptypes.PreplayResTrieNode, value interface{}, txnType int) {
	r.miss++
	r.txnType[txnType]++

	index := r.txnsIndexMap[txn.Hash()]
	sender := r.txnsSenderMap[txn.Hash()]

	groupHitGroup := r.preplayer.preplayLog.searchGroupHitGroup(txn, sender)
	if len(groupHitGroup) == 0 {
		r.groupMissCount++
		if isSample(txn) {
			log.Info("Report reuse miss: NoGroup", "tx", txn.Hash().Hex(), "index", index)
		}
		return
	}

	execHitGroup := r.searchExecHitGroup(txn, groupHitGroup)
	if len(execHitGroup) == 0 {
		r.execMissCount++
		if isSample(txn) {
			log.Info("Report reuse miss: NoExec", "tx", txn.Hash().Hex(), "index", index)
		}
		return
	}

	if node.NodeType == nil {
		r.nilNodeTypeCount++
		if isSample(txn) {
			log.Info("Report reuse miss: nodeType nil", "tx", txn.Hash().Hex(), "index", index)
		}
		return
	}

	nodeType := node.NodeType
	if cmptypes.IsChainField(nodeType.Field) {
		r.rChainMissTotal++
		r.rChainMissCount[nodeType.Field-cmptypes.Blockhash]++
		//if nodeType.Field == cmptypes.Timestamp {
		//	var haveTimeDep = make(map[common.Hash]*TxnGroup)
		//	for groupHash, group := range execHitGroup {
		//		if group.isTimestampDep() {
		//			haveTimeDep[groupHash] = group
		//		}
		//	}
		//	if len(haveTimeDep) == 0 {
		//		r.noTimeDepCount++
		//	} else {
		//		var preplayMost int
		//		for _, group := range haveTimeDep {
		//			if preplayMost < group.getPreplayCount() {
		//				preplayMost = group.getPreplayCount()
		//			}
		//		}
		//		var forecastTime uint64
		//		if forecastTime = r.preplayer.nowHeader.time; forecastTime != 0 {
		//			if preplayMost >= 5 {
		//				preplayMost = 5
		//			}
		//			diff := int(forecastTime - r.block.Time())
		//			if diff >= 3 {
		//				diff = 3
		//			}
		//			if diff <= -3 {
		//				diff = -3
		//			}
		//			r.timeMissMap[[2]int{preplayMost, diff}]++
		//		}
		//		pickGroup := pickOneGroup(haveTimeDep)
		//		nodeChildStr := fmt.Sprintf("%d:", node.Children.Size())
		//		var first = true
		//		for value := range node.Children.GetKeys() {
		//			if first {
		//				nodeChildStr += "{"
		//				first = false
		//			} else {
		//				nodeChildStr += ","
		//			}
		//			nodeChildStr += getInterfaceValue(value)
		//		}
		//		if node.Children.Size() > 0 {
		//			nodeChildStr += "}"
		//		}
		//		var groupTxns = "TooLarge"
		//		if pickGroup.txnCount <= 20 {
		//			groupTxns = pickGroup.txnPool.String()
		//		}
		//		log.Info("Report reuse miss: timestamp", "tx", txn.Hash().Hex(), "index", index,
		//			"nodeChild", nodeChildStr, "missValue", getInterfaceValue(value),
		//			"forecastTime", forecastTime, "block.Time", r.block.Time(),
		//			"haveTimeDepGroupNum", len(haveTimeDep), "pickGroupSize", pickGroup.txnCount,
		//			"isTimeDep", pickGroup.isTimestampDep(), "preplayedTimestamp", pickGroup.getTimestampOrderLog(),
		//			"pickGroupPreplay", pickGroup.getPreplayCount(), "group.txnPool", groupTxns,
		//		)
		//	}
		//}
		return
	}

	isStorageMiss := nodeType.Field == cmptypes.Storage || nodeType.Field == cmptypes.CommittedStorage
	if isStorageMiss {
		r.rStateMissCount[1]++
	} else {
		r.rStateMissCount[0]++
	}

	if !r.reportMissDetail {
		return
	}

	groundGroup, groundOrder := r.searchGroundGroup(txn, sender)
	poolBefore, orderBefore := r.getTxnBefore(txn, groundOrder)

	txnHitGroup := r.searchTxnHitGroup(execHitGroup, poolBefore)
	if len(txnHitGroup) == 0 {
		if isStorageMiss {
			r.txnMissCount[1]++
		} else {
			r.txnMissCount[0]++
		}
		if isSample(txn) {
			log.Info("Report reuse miss: missing txns", "tx", txn.Hash().Hex(), "index", index,
				"execHit->txnHit", fmt.Sprintf("%d->0", len(execHitGroup)))
		}
		return
	}

	if !r.isSortHit(poolBefore.copy()) {
		if isStorageMiss {
			r.sortMissCount[1]++
		} else {
			r.sortMissCount[0]++
		}
		if isSample(txn) {
			log.Info("Report reuse miss: N&P violation", "tx", txn.Hash().Hex(), "index", index,
				"poolBeforeSize", poolBefore.size(), "orderBeforeSize", len(orderBefore),
				"poolBefore", poolBefore, "orderBefore", orderBefore)
		}
		return
	}

	chainHitGroup := r.searchChainHitGroup(txnHitGroup)
	if len(chainHitGroup) == 0 {
		if isStorageMiss {
			r.chainMissCount[1]++
		} else {
			r.chainMissCount[0]++
		}
		if isSample(txn) {
			pickGroup := pickOneGroup(txnHitGroup)
			context := []interface{}{"tx", txn.Hash().Hex(), "index", index, "txnHitGroupNum", len(txnHitGroup),
				"isDep", fmt.Sprintf("%v:%v", pickGroup.isCoinbaseDep(), pickGroup.isTimestampDep())}
			if pickGroup.isCoinbaseDep() {
				var minerStr string
				for index, miner := range r.preplayer.minerList.topActive {
					if index > 0 {
						minerStr += ","
					}
					minerStr += miner.Hex()
				}
				context = append(context, "groundCoinbase", r.block.Header().Coinbase.Hex(), "activeMiner", minerStr)
			}
			if pickGroup.isTimestampDep() {
				context = append(context, "groundTimestamp", r.block.Header().Time, "forecastTimestamp", r.preplayer.nowHeader.time)
			}
			log.Info("Report reuse miss: chain miss", context...)
		}
		return
	}

	snapHitGroup, orderHitGroup := r.searchSnapAndOrderHitGroup(chainHitGroup, groundOrder, orderBefore)
	if len(snapHitGroup) == 0 {
		if isStorageMiss {
			r.snapMissCount[1]++
		} else {
			r.snapMissCount[0]++
		}
		if isSample(txn) {
			pickGroup := pickOneGroup(chainHitGroup)
			log.Info("Report reuse miss: extra txns", "tx", txn.Hash().Hex(), "index", index, "chainHitGroupNum", len(chainHitGroup),
				"pickGroupSize", pickGroup.txnCount, "pickGroupSubSize", len(pickGroup.subpoolList),
				"group.txnPool", pickGroup.txnPool, "groundOrder", groundOrder)
		}
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
				nowRate = float64(group.getPreplayCount()) / float64(group.orderCount.Uint64())
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
		if isSample(txn) {
			log.Info("Report reuse miss: insufficient execution", "tx", txn.Hash().Hex(), "index", index, "snapHitGroupNum", len(snapHitGroup),
				"pickGroupSize", pickGroup.txnCount, "pickGroupSubSize", len(pickGroup.subpoolList),
				"group.txnPool", pickGroup.txnPool, "groundOrder", groundOrder,
				"pickPreplayMost", fmt.Sprintf("%d(%.2f%%)->%s", pickGroup.getPreplayCount(), preplayRate*100, pickGroup.orderCount.String()))
		}
		return
	}

	if sender == r.block.Coinbase() {
		if isStorageMiss {
			r.minerMissCount[1]++
		} else {
			r.minerMissCount[0]++
		}
		if isSample(txn) {
			log.Info("Report reuse miss: miner miss", "tx", txn.Hash().Hex(), "index", index, "orderHitGroupNum", len(orderHitGroup),
				"sender", sender, "coinbase", r.block.Coinbase())
		}
		return
	}

	rootHitGroup := r.searchRootHitGroup(orderHitGroup)
	if len(rootHitGroup) == 0 {
		if isStorageMiss {
			r.rootMissCount[1]++
		} else {
			r.rootMissCount[0]++
		}
		if isSample(txn) {
			pickGroup := pickOneGroup(orderHitGroup)
			log.Info("Report reuse miss: root miss", "tx", txn.Hash().Hex(), "index", index, "orderHitGroupNum", len(orderHitGroup),
				"block.parent.root", r.parent.Root(), "group.parent.root", pickGroup.parent.Root())
		}
		return
	}

	if isStorageMiss {
		r.bugMissCount[1]++
	} else {
		r.bugMissCount[0]++
	}
	if isSample(txn) {
		pickGroup := pickOneGroup(rootHitGroup)
		nodeTypeStr := fmt.Sprintf("%s.%v", nodeType.Address.Hex(), nodeType.Field.String())
		if nodeType.Field == cmptypes.CommittedStorage || nodeType.Field == cmptypes.Storage {
			nodeTypeStr += "." + nodeType.Loc.(common.Hash).Hex()
		}
		nodeChildStr := fmt.Sprintf("%d:", node.Children.Size())
		var first = true
		for _, value := range node.Children.GetKeys() {
			if first {
				nodeChildStr += "{"
				first = false
			} else {
				nodeChildStr += ","
			}
			nodeChildStr += getInterfaceValue(value)
		}
		if node.Children.Size() > 0 {
			nodeChildStr += "}"
		}
		var groupTxnPool = "TooLarge"
		if pickGroup.txnCount <= 20 {
			groupTxnPool = pickGroup.txnPool.String()
		}
		log.Info("Report reuse miss: bug", "tx", txn.Hash().Hex(), "index", index,
			"nodeType", nodeTypeStr, "nodeChild", nodeChildStr, "missValue", getInterfaceValue(value),
			"rootHitGroupNum", len(rootHitGroup), "pickGroupSize", pickGroup.txnCount, "pickGroupSubSize", len(pickGroup.subpoolList),
			"pickPreplayMost", fmt.Sprintf("%d->%s", pickGroup.getPreplayCount(), pickGroup.orderCount.String()),
			"preplayedOrder", pickGroup.getFirstPreplayOrder(),
			"group.txnPool", groupTxnPool, "orderBefore", orderBefore,
		)
		group := r.groundGroups[groundGroup.hash]
		log.Info("Ground group info", "hash", group.hash, "txnCount", group.txnCount,
			"txnPool", group.txnPool, "order", groundOrder)
		for _, txns := range group.txnPool {
			for _, txn := range txns {
				log.Info("Transaction info", "tx", txn.Hash().Hex(), "index", r.txnsIndexMap[txn.Hash()],
					"sender", r.txnsSenderMap[txn.Hash()], "rwrecord", r.getRWRecord(txn.Hash()).String())
			}
		}
	}
}

func (r *MissReporter) ReportMiss(noListen, noListenAndEthermine, noEnpool, noEnpending, noPackage, noEnqueue, noPreplay uint64) {
	context := []interface{}{
		"NoListen", fmt.Sprintf("%d(%d)", noListen, noListenAndEthermine),
		"NoEnpool-NoEnpending", fmt.Sprintf("%d-%d", noEnpool, noEnpending),
		"NoPackage", noPackage, "NoEnqueue", noEnqueue,
		"NoPreplay", fmt.Sprintf("%d(%d-%d)", noPreplay, r.noGroupPreplay, r.noExecPreplay),
		"miss", fmt.Sprintf("%d(%d:%d:%d)", r.miss, r.txnType[0], r.txnType[1], r.txnType[2]),
	}
	if r.groupMissCount > 0 {
		context = append(context, "NoGroup", r.groupMissCount)
	}
	if r.execMissCount > 0 {
		context = append(context, "NoExec", r.execMissCount)
	}
	if r.nilNodeTypeCount > 0 {
		context = append(context, "nilNodeType", r.nilNodeTypeCount)
	}
	context = append(context, "rChainMiss", fmt.Sprintf("%d[%d:%d:%d(%.2f):%d:%d:%d]",
		r.rChainMissTotal, r.rChainMissCount[0], r.rChainMissCount[1],
		r.rChainMissCount[2], float64(r.rChainMissCount[2])/float64(r.miss),
		r.rChainMissCount[3], r.rChainMissCount[4], r.rChainMissCount[5]),
		"rStateMiss", fmt.Sprintf("%d[%d:%d]", r.rStateMissCount[0]+r.rStateMissCount[1], r.rStateMissCount[0], r.rStateMissCount[1]),
	)
	txnMissCount := r.txnMissCount[0] + r.txnMissCount[1]
	if txnMissCount > 0 {
		context = append(context, "missing txns", fmt.Sprintf("%d(%d:%d)", txnMissCount, r.txnMissCount[0], r.txnMissCount[1]))
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
	if r.orderMissTotal > 0 {
		context = append(context,
			"insufficient execution", fmt.Sprintf("%d[1|2|6|24|120|720|>720]-[%d:%d:%d:%d:%d:%d:%d]",
				r.orderMissTotal, r.orderMissCount[0], r.orderMissCount[1], r.orderMissCount[2], r.orderMissCount[3], r.orderMissCount[4],
				r.orderMissCount[5], r.orderMissCount[6]),
		)
	}
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
	//context = append(context, "timeMissMap", r.timeMissMap, "noTimeDepCount", r.noTimeDepCount)
	log.Info("Cumulative miss statistics", context...)
}

func (p *Preplayer) getGroundGroup(block *types.Block, txnsIndexMap map[common.Hash]int, txnsSenderMap map[common.Hash]common.Address) (*types.Block,
	map[common.Hash]*TxnGroup, func(txn common.Hash) *RWRecord) {
	parentHash := block.ParentHash()
	parent := p.chain.GetBlockByHash(parentHash)

	pendingTxn := make(TransactionPool)
	for _, txn := range block.Transactions() {
		sender := txnsSenderMap[txn.Hash()]
		pendingTxn[sender] = append(pendingTxn[sender], txn)
	}

	totalDifficulty := p.chain.GetTd(parentHash, parent.NumberU64())
	if totalDifficulty == nil {
		totalDifficulty = new(big.Int)
	}
	currentState := &cache.CurrentState{
		PreplayName:       p.trigger.Name,
		Number:            parent.NumberU64(),
		Hash:              parentHash.Hex(),
		RawHash:           parentHash,
		Txs:               parent.Transactions(),
		TotalDifficulty:   totalDifficulty.String(),
		SnapshotTimestamp: time.Now().UnixNano() / 1000000,
	}

	executor := NewExecutor("0", p.config, p.engine, p.chain, p.eth.ChainDb(), txnsIndexMap, pendingTxn, currentState,
		p.trigger, nil, false, false, false, nil)

	executor.RoundID = p.globalCache.NewRoundID()

	// Execute, use pending for preplay
	executor.commit(block.Coinbase(), parent, block.Header(), pendingTxn)

	rwrecords := make(map[common.Hash]*RWRecord)
	for _, txns := range pendingTxn {
		for _, txn := range txns {
			txnHash := txn.Hash()
			txPreplay := p.globalCache.PeekTxPreplay(txnHash)
			if txPreplay != nil {
				txPreplay.RLockRound()
				if round, ok := txPreplay.PeekRound(executor.RoundID); ok {
					rwrecords[txnHash] = NewRWRecord(round.RWrecord)
					txPreplay.RUnlockRound()
					continue
				}
				txPreplay.RUnlockRound()
			}
			rwrecords[txnHash] = NewRWRecord(nil)
			log.Error("Detect nil rwrecord in miss reporter", "txPreplay != nil", txPreplay != nil,
				"result.status", executor.resultMap[txnHash].Status, "result.reason", executor.resultMap[txnHash].Reason)
		}
	}

	selectRWRecord := func(txn common.Hash) *RWRecord {
		if rwrecord, ok := rwrecords[txn]; ok {
			return rwrecord
		} else {
			rwrecords[txn] = NewRWRecord(nil)
			return rwrecords[txn]
		}
	}
	isSameGroup := func(sender common.Address, txn common.Hash, group *TxnGroup) bool {
		if _, ok := group.txnPool[sender]; ok {
			return true
		}
		rwrecord := selectRWRecord(txn)
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

		txnPool := make(TransactionPool)
		txnList := make([][]byte, 0)
		rwrecord := NewRWRecord(nil)
		for groupHash := range relativeGroup {
			addGroup := nowGroups[groupHash]
			delete(nowGroups, groupHash)
			for addr, txns := range addGroup.txnPool {
				txnPool[addr] = append(txnPool[addr], txns...)
			}
			txnList = append(txnList, addGroup.txnList...)
			rwrecord.merge(addGroup.RWRecord)
		}
		txnPool[from] = append(txnPool[from], txns...)
		for _, txn := range txns {
			txnList = append(txnList, txn.Hash().Bytes())
			rwrecord.merge(selectRWRecord(txn.Hash()))
		}
		sort.Slice(txnList, func(i, j int) bool {
			return bytes.Compare(txnList[i], txnList[j]) == -1
		})
		groupHash := crypto.Keccak256Hash(txnList...)
		nowGroups[groupHash] = &TxnGroup{
			hash:     groupHash,
			txnPool:  txnPool,
			txnList:  txnList,
			RWRecord: rwrecord,
		}
	}
	for _, group := range nowGroups {
		group.txnCount = group.txnPool.size()
	}
	return parent, nowGroups, selectRWRecord
}

func (r *MissReporter) searchGroundGroup(currentTxn *types.Transaction, sender common.Address) (*TxnGroup, TxnOrder) {
	var (
		groundGroup *TxnGroup
		groundOrder TxnOrder
		sum         int
	)
	for _, group := range r.groundGroups {
		sum += group.txnPool.size()
		for _, txn := range group.txnPool[sender] {
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
			for from, txns := range group.txnPool {
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
	for _, txns := range groundGroup.txnPool {
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

func (r *MissReporter) searchExecHitGroup(currentTxn *types.Transaction, groupHitGroup map[common.Hash]*TxnGroup) map[common.Hash]*TxnGroup {
	execHitGroup := make(map[common.Hash]*TxnGroup)
	for groupHash, group := range groupHitGroup {
		if group.haveTxnFinished(currentTxn.Hash()) {
			execHitGroup[groupHash] = group
		}
	}
	return execHitGroup
}

func (r *MissReporter) searchTxnHitGroup(groupExecGroup map[common.Hash]*TxnGroup, poolBefore TransactionPool) map[common.Hash]*TxnGroup {
	txnHitGroup := make(map[common.Hash]*TxnGroup)
	for groupHash, group := range groupExecGroup {
		if !poolBefore.isTxnsPoolLarge(group.txnPool) {
			txnHitGroup[groupHash] = group
		}
	}
	return txnHitGroup
}

func (r *MissReporter) isSortHit(groundPool TransactionPool) bool {
	txnHeap := types.NewTransactionsByPriceAndNonce(r.signer, groundPool, nil, true)
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
			for _, miner := range r.preplayer.minerList.topActive {
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
			diff := r.block.Header().Time - r.preplayer.nowHeader.time
			for _, shift := range timeShift {
				if diff == shift {
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

func isSample(txn *types.Transaction) bool {
	return txn.Hash()[0] == 0
}

func pickOneGroup(groupMap map[common.Hash]*TxnGroup) *TxnGroup {
	for _, group := range groupMap {
		return group
	}
	return nil
}

func getInterfaceValue(value interface{}) string {
	if value == nil { // miss_value is null
		return ""
	}
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
