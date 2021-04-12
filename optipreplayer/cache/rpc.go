package cache

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

var (
	featureSize = uint64(90)
	featureDim  = 8
)

// RPCPrelpayResult define the struct of preplay result
type RPCPrelpayResult struct {
	Status           []string            `json:"status"`
	PreplayResult    *TxPreplay          `json:"preplayResult"`
	DistributionList *TxDistributionList `json:"distributionList"`
}

// TxDistributionItem define the item of distribution list
type TxDistributionItem struct {
	GasUsed  uint64   `json:"gasUsed"`
	GasCount uint64   `json:"gasCount"`
	GasPrice *big.Int `json:"gasPrice"`
}

// TxDistributionList define distribution list
type TxDistributionList struct {
	List []*TxDistributionItem `json:"resultList"`
}

// GetDistributionList RPC for get distribution list
func (r *GlobalCache) GetDistributionList(txHash common.Hash) *TxDistributionList {
	// default round ID
	roundID := uint64(0)
	result := &TxDistributionList{}

	queryTx := r.PeekTxPreplayInNonProcess(txHash)
	if queryTx == nil {
		return result
	}

	//var err error
	//rawPending, err := r.eth.TxPool().Pending()
	//if err != nil {
	//	return result
	//}
	rawPending := map[common.Address]types.Transactions{}

	calList := map[uint64]*TxDistributionItem{}

	for acc := range rawPending {
		for _, tx := range rawPending[acc] {
			txHash := tx.Hash()

			txPreplay := r.PeekTxPreplayInNonProcess(txHash)
			if txPreplay == nil {
				continue
			}

			txPreplay.RLockRound()
			round, _ := txPreplay.PeekRound(roundID)
			// Dont consider will not in & already in
			if round.ExtraResult.Status != "will in" {
				txPreplay.RUnlockRound()
				continue
			}

			if txPreplay.GasPrice.Cmp(queryTx.GasPrice) >= 0 {
				txPrice := txPreplay.GasPrice.Uint64()
				if dItem, ok := calList[txPrice]; ok {
					dItem.GasCount = dItem.GasCount + 1
					dItem.GasUsed = dItem.GasUsed + round.Receipt.GasUsed
				} else {
					calList[txPrice] = &TxDistributionItem{
						GasCount: 1,
						GasUsed:  round.Receipt.GasUsed,
						GasPrice: txPreplay.GasPrice,
					}
				}
			}
			txPreplay.RUnlockRound()
		}
	}

	for _, item := range calList {
		result.List = append(result.List, item)
	}
	return result
}

// RPCFeatureResult define the struct of feature result
type RPCFeatureResult struct {
	Status              []string       `json:"status"`
	TxListenTimestamp   []uint64       `json:"txListenTimestamp"`
	BloConfirmTimestamp []uint64       `json:"blockConfirmTimestamp"`
	BloListenTimestamp  []uint64       `json:"blockListenTimestamp"`
	NewTimeFeatureList  *FeatureList   `json:"newTimeFeature,omitempty"`
	OldTimeFeatureList  *FeatureList   `json:"oldTimeFeature,omitempty"`
	TrainingLists       []*FeatureList `json:"training,omitempty"`
	AverageGasUsed      uint64         `json:"averageGasUsed,omitempty"`
}

// FeatureList define feature list
type FeatureList struct {
	Lists [][]uint64 `json:"lists"`
}

// RoundTimeToMinute round a timestamp to minute
func (r *GlobalCache) RoundTimeToMinute(timestamp uint64) uint64 {
	t := time.Unix((int64)(timestamp), 0)
	RoundTime := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	return (uint64)(RoundTime.Unix())
}

// GetFeatureList RPC for feature list
func (r *GlobalCache) GetFeatureList(gasPrice uint64, timestamp uint64) *FeatureList {

	// Super RLock
	r.BlockMu.RLock()
	r.TxMu.RLock()
	defer func() {
		r.TxMu.RUnlock()
		r.BlockMu.RUnlock()
	}()

	timestamp = r.RoundTimeToMinute(timestamp)
	featureList := &FeatureList{
		Lists: [][]uint64{},
	}
	for i := 0; i < featureDim; i++ {
		newList := []uint64{}
		featureList.Lists = append(featureList.Lists, newList)
	}

	lastTime := timestamp

	blockKeys := r.BlockCache.Keys()
	bIndex := len(blockKeys) - 1

	txKeys := r.TxListenCache.Keys()
	tIndex := len(txKeys) - 1

	for i := 1; (uint64)(i) <= featureSize; i++ {

		// split block
		blockBucket := BlockListens{}

		for {
			if bIndex < 0 {
				break
			}

			blockNum, _ := blockKeys[bIndex].(uint64)
			bIndex = bIndex - 1

			block := r.GetBlockListen(blockNum)
			if block == nil {
				continue
			}

			// Invalid block
			if block.Confirm.Valid == false {
				continue
			}

			// too early
			if block.Confirm.ConfirmTime > lastTime {
				continue
			}

			// last >= time >= last-59
			if block.Confirm.ConfirmTime > lastTime-60 {
				blockBucket = append(blockBucket, block)
				continue
			}

			// not valid
			bIndex = bIndex + 1
			break
		}

		// 1. Acpt
		acpt := uint64(0)
		for _, block := range blockBucket {
			if gasPrice > block.Confirm.MinPrice.Uint64() {
				acpt = uint64(1)
			}
		}
		featureList.Lists[0] = append(featureList.Lists[0], acpt)

		// 2. AcptBlockCnt
		acptCnt := uint64(0)
		for _, block := range blockBucket {
			if gasPrice > block.Confirm.MinPrice.Uint64() {
				acptCnt = acptCnt + 1
			}
		}
		featureList.Lists[1] = append(featureList.Lists[1], (uint64)(acptCnt))

		// 3. BlockTxGasUsedLow
		// 4. blockTxNumLow
		blockTxNumLow := uint64(0)
		blockTxGasUsedLow := uint64(0)

		for _, block := range blockBucket {
			for txIndex, tx := range block.Confirm.ValidTxs {
				txPrice := tx.GasPrice().Uint64()
				txGasUsed := block.Confirm.ReceiptTxs[txIndex].GasUsed
				if txPrice < gasPrice {
					blockTxNumLow = blockTxNumLow + 1
					blockTxGasUsedLow = blockTxGasUsedLow + txGasUsed
				}
			}
		}
		featureList.Lists[2] = append(featureList.Lists[2], blockTxGasUsedLow)
		featureList.Lists[3] = append(featureList.Lists[3], blockTxNumLow)

		// split tx
		txBucket := TxListens{}

		for {
			if tIndex < 0 {
				break
			}

			txHash, _ := txKeys[tIndex].(common.Hash)
			tIndex = tIndex - 1

			tx := r.GetTxListen(txHash)
			if tx == nil {
				continue
			}

			// too early
			if tx.ListenTime > lastTime {
				continue
			}

			// last >= time >= last-59
			if tx.ListenTime > lastTime-60 {
				txBucket = append(txBucket, tx)
				continue
			}

			// not valid
			tIndex = tIndex + 1
			break
		}

		// 5. ArrTxGasLimitHigh
		// 6. ArrTxGasLimitSame
		// 7. ArrTxNumHigh
		// 8. ArrTxNumeSame
		ArrTxGasLimitHigh := uint64(0)
		ArrTxGasLimitSame := uint64(0)
		ArrTxNumHigh := uint64(0)
		ArrTxNumeSame := uint64(0)

		for _, tx := range txBucket {
			txPrice := tx.Tx.GasPrice().Uint64()
			txGasLimit := tx.Tx.Gas()
			if txPrice > gasPrice {
				ArrTxGasLimitHigh = ArrTxGasLimitHigh + txGasLimit
				ArrTxNumHigh = ArrTxNumHigh + 1
			}
			if txPrice == gasPrice {
				ArrTxGasLimitSame = ArrTxGasLimitSame + txGasLimit
				ArrTxNumeSame = ArrTxNumeSame + 1
			}
		}
		featureList.Lists[4] = append(featureList.Lists[4], ArrTxGasLimitHigh)
		featureList.Lists[5] = append(featureList.Lists[5], ArrTxGasLimitSame)
		featureList.Lists[6] = append(featureList.Lists[6], ArrTxNumHigh)
		featureList.Lists[7] = append(featureList.Lists[7], ArrTxNumeSame)

		lastTime = lastTime - 60
	}

	return featureList
}

// GetQueryTxByHash returns tx, listenTime
func (r *GlobalCache) GetQueryTxByHash(hash common.Hash) *TxListen {

	// Super Lock
	r.BlockMu.RLock()
	r.TxMu.RLock()
	defer func() {
		r.TxMu.RUnlock()
		r.BlockMu.RUnlock()
	}()

	resultTx := r.GetTxListen(hash)

	if resultTx == nil {
		return nil
	}

	// Try to update confirm time
	if resultTx.ConfirmTime == 0 {
		blockKeys := r.BlockCache.Keys()
		for i := len(blockKeys) - 1; i >= 0; i-- {

			// Finish find confirm
			if resultTx.ConfirmTime != 0 {
				break
			}

			blockNum, _ := blockKeys[i].(uint64)
			block := r.GetBlockListen(blockNum)
			if block == nil {
				continue
			}

			// should find confirmation through all txs
			for _, tx := range block.Confirm.Block.Body().Transactions {
				txHash := tx.Hash()
				if txHash.String() == hash.String() {
					resultTx.ConfirmTime = block.Confirm.ConfirmTime
					resultTx.ConfirmListenTime = block.BlockPre.ListenTime
					break
				}
			}
		}
	}

	return resultTx
}

// GetNewTimeFeatureList return new time list for RPC
func (r *GlobalCache) GetNewTimeFeatureList(txHash common.Hash, timestamp uint64) (*FeatureList, string, uint64, uint64, uint64) {

	tx := r.GetQueryTxByHash(txHash)
	if tx == nil {
		return nil, "unkown tx", 0, 0, 0
	}

	// -60 to abondon the incomplete one mintue
	return r.GetFeatureList(tx.Tx.GasPrice().Uint64(), timestamp), "success", tx.ListenTime, tx.ConfirmTime, tx.ConfirmListenTime
}

// GetOldTimeFeatureList return old time list for RPC
func (r *GlobalCache) GetOldTimeFeatureList(txHash common.Hash) (*FeatureList, string, uint64, uint64, uint64) {

	tx := r.GetQueryTxByHash(txHash)
	if tx == nil {
		return nil, "unkown tx", 0, 0, 0
	}

	// include current minute
	return r.GetFeatureList(tx.Tx.GasPrice().Uint64(), tx.ListenTime), "success", tx.ListenTime, tx.ConfirmTime, tx.ConfirmListenTime

}

// GetTrainingLists returns the training dataset, timestamp is observe time
func (r *GlobalCache) GetTrainingLists(timestamp uint64) ([]*FeatureList, string, uint64) {
	timestamp = r.RoundTimeToMinute(timestamp) - 60

	// bucket := r.GetBucket(timestamp)
	// if bucket == nil {
	// 	return nil, "no cache found", 0
	// }
	// trainingLists :=

	// for _, block := range bucket.ArrBlocks {
	// 	for _, tx := range block.ValidTxs {
	// 		txHash := tx.Hash()
	//         trainingList, status, predictTime, listenTime := GetParentDirectory
	// 	}
	// }
	return nil, "not implemented", 0
}

// GetAverageGasUsed return average gas used for the past minute minutes
func (r *GlobalCache) GetAverageGasUsed(minute uint64) (uint64, string, uint64) {

	r.BlockMu.RLock()
	defer r.BlockMu.RUnlock()

	timestamp := (uint64)(time.Now().Unix())

	blockKeys := r.BlockCache.Keys()
	totBlockGasUsed := uint64(0)

	for i := len(blockKeys) - 1; i >= 0; i-- {

		blockNum, _ := blockKeys[i].(uint64)
		block := r.GetBlockListen(blockNum)
		if block == nil {
			break
		}

		if block.Confirm.ConfirmTime > timestamp {
			continue
		}
		if block.Confirm.ConfirmTime <= timestamp-uint64(60)*minute {
			break
		}

		for _, receipt := range block.Confirm.ReceiptTxs {
			txGasUsed := receipt.GasUsed
			totBlockGasUsed = totBlockGasUsed + txGasUsed
		}
	}

	return (totBlockGasUsed / minute), "success", timestamp

}
