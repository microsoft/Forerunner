// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package cache

import (
	"encoding/json"
	"github.com/ethereum/go-ethereum/crypto"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	lru "github.com/hashicorp/golang-lru"
)

var (
	logDir string
)

// GlobalCache create a global cache structure to maintain cache data
type GlobalCache struct {
	BlockPreCache *lru.Cache

	// Listen
	BlockMu    sync.RWMutex
	BlockCache *lru.Cache

	TxMu             sync.RWMutex
	TxListenCache    *lru.Cache
	TxEnpoolCache    *lru.Cache
	TxEnpendingCache *lru.Cache
	TxPackageCache   *lru.Cache
	TxEnqueueCache   *lru.Cache

	// Preplay result
	PreplayCache     *lru.Cache // Result Cache
	PreplayCacheSize int
	PreplayRoundID   uint64
	PreplayRoundIDMu sync.RWMutex
	PreplayTimestamp uint64 // Last time stamp

	reduplicatedNonceTxnMu sync.RWMutex
	reduplicatedNonceTxn   map[common.Address]types.TxByNonce

	// Gas used cache
	PrimaryGasUsedCache   *lru.Cache
	SecondaryGasUsedCache *lru.Cache
	TertiaryGasUsedCache  *lru.Cache

	TimestampMu    sync.RWMutex
	TimestampField int64

	// Real result
	GroundMu    sync.RWMutex
	GroundCache *lru.Cache

	// Cmp result, No use
	// FoundMu    sync.RWMutex
	// FoundCache *lru.Cache

	// Enable Flag
	PreplayFlag bool
	FeatureFlag bool

	WhiteListAddr *lru.Cache

	CreateTimeStamp time.Time
	// Deprecated

	// BlockCnt   uint64
	// BucketMu    sync.RWMutex // No need for global MU?
	// BucketCache *lru.Cache // Feature Cache
	pause int32

	Synced    func() bool
	SyncStart uint64
}

// NewGlobalCache create new global cache structure
func NewGlobalCache(bSize int, tSize int, pSize int, logRoot string) *GlobalCache {

	g := &GlobalCache{}

	g.BlockPreCache, _ = lru.New(bSize)
	g.BlockCache, _ = lru.New(bSize)

	g.TxListenCache, _ = lru.New(tSize * 10)
	g.TxEnpoolCache, _ = lru.New(tSize * 5)
	g.TxEnpendingCache, _ = lru.New(tSize * 5)
	g.TxPackageCache, _ = lru.New(tSize)
	g.TxEnqueueCache, _ = lru.New(tSize)

	g.PreplayCache, _ = lru.New(pSize)
	g.PreplayCacheSize = pSize
	g.PreplayRoundID = 1
	g.PreplayTimestamp = uint64(time.Now().Unix())
	g.TimestampField = -2

	g.reduplicatedNonceTxn = make(map[common.Address]types.TxByNonce)

	g.PrimaryGasUsedCache, _ = lru.New(pSize)
	g.SecondaryGasUsedCache, _ = lru.New(pSize)
	g.TertiaryGasUsedCache, _ = lru.New(pSize)

	g.GroundCache, _ = lru.New(pSize)
	// g.FoundCache, _ = lru.New(pSize)

	g.WhiteListAddr, _ = lru.New(20)
	g.WhiteListAddr.Add(common.HexToAddress("0xEA674fdDe714fd979de3EdF0F56AA9716B898ec8"), struct{}{})
	g.WhiteListAddr.Add(common.HexToAddress("0x52bc44d5378309EE2abF1539BF71dE1b7d7bE3b5"), struct{}{})

	g.CreateTimeStamp = time.Now()
	// g.BucketCache, _ = lru.New(bSize)

	logDir = filepath.Join(logRoot, g.CreateTimeStamp.Format("2006_01_02_15_04_05")+"_"+strconv.FormatInt(g.CreateTimeStamp.Unix(), 10))
	_, err := os.Stat(logDir)
	if err != nil {
		os.MkdirAll(logDir, os.ModePerm)
	}
	return g
}

var aBigInt = crypto.Keccak256Hash(common.Hex2Bytes("abignumber")).Big()

func (r *GlobalCache) GetTrieAndWObjectSizes() (cachedTxCount int, cachedTxWithTraceCount int,
	maxTrieNodeCount int64, totalTrieNodeCount int64, totalMixTrieNodeCount int64, totalRWTrieNodeCount int64,
	wobjectCount uint64, wobjectStorageSize uint64) {
	txHashes := r.KeysOfTxPreplay()
	cachedTxCount = len(txHashes)
	cachedTxWithTraceCount = 0
	maxTrieNodeCount = 0
	totalTrieNodeCount = 0
	for _, key := range txHashes {
		if txPreplay := r.PeekTxPreplay(key); txPreplay != nil {
			objectSize, storageItemCount := txPreplay.PreplayResults.GetWObjectSize()
			wobjectCount += objectSize
			wobjectStorageSize += storageItemCount
			if txPreplay.PreplayResults.MixTree != nil {
				totalMixTrieNodeCount += txPreplay.PreplayResults.MixTree.GetNodeCount()
			}
			if txPreplay.PreplayResults.TraceTrie != nil {
				cachedTxWithTraceCount++
				nodeCount := txPreplay.PreplayResults.TraceTrie.GetNodeCount()
				totalTrieNodeCount += nodeCount
				if nodeCount > maxTrieNodeCount {
					maxTrieNodeCount = nodeCount
				}
			}
			if txPreplay.PreplayResults.RWRecordTrie != nil {
				totalRWTrieNodeCount += txPreplay.PreplayResults.RWRecordTrie.GetNodeCount()
			}
		}
	}
	return

}

func (r *GlobalCache) GCWObjects() () {
	txHashes := r.KeysOfTxPreplay()
	for _, key := range txHashes {
		if txPreplay := r.PeekTxPreplay(key); txPreplay != nil {
			txPreplay.PreplayResults.GCWObjects()
		}
	}
}

// ResetGlobalCache reset the global cache size
func (r *GlobalCache) ResetGlobalCache(bSize int, tSize int, pSize int) bool {

	r.BlockMu.Lock()
	r.PreplayRoundIDMu.Lock()
	r.TxMu.Lock()
	defer func() {
		r.TxMu.Unlock()
		r.PreplayRoundIDMu.Unlock()
		r.BlockMu.Unlock()
	}()

	if bSize != 0 {
		r.BlockPreCache, _ = lru.New(bSize)
		r.BlockCache, _ = lru.New(bSize)
	}

	if tSize != 0 {
		r.TxListenCache, _ = lru.New(tSize * 10)
		r.TxEnpoolCache, _ = lru.New(tSize * 5)
		r.TxEnpendingCache, _ = lru.New(tSize * 5)
		r.TxPackageCache, _ = lru.New(tSize)
		r.TxEnqueueCache, _ = lru.New(tSize)
	}

	if pSize != 0 {
		r.PreplayCache, _ = lru.New(pSize)
		r.PreplayRoundID = 1

		r.PrimaryGasUsedCache, _ = lru.New(pSize)
		r.SecondaryGasUsedCache, _ = lru.New(pSize)
		r.TertiaryGasUsedCache, _ = lru.New(pSize)

		r.GroundCache, _ = lru.New(pSize)
		// r.FoundCache, _ = lru.New(pSize)
	}

	r.WhiteListAddr, _ = lru.New(20)

	r.reduplicatedNonceTxn = make(map[common.Address]types.TxByNonce)

	r.CreateTimeStamp = time.Now()

	return true
}

// BucketPrint print minute by minute
func (r *GlobalCache) BucketPrint(timestamp uint64) {

	// Super RLock
	r.BlockMu.RLock()
	r.TxMu.RLock()
	defer func() {
		r.TxMu.RUnlock()
		r.BlockMu.RUnlock()
	}()

	blockKeys := r.BlockCache.Keys()
	txKeys := r.TxListenCache.Keys()

	txCnt := 0
	blockCnt := 0
	totBlockGasUsed := uint64(0)
	totTxGasLimit := uint64(0)

	for i := len(blockKeys) - 1; i >= 0; i-- {

		blockNum, _ := blockKeys[i].(uint64)
		block := r.GetBlockListen(blockNum)
		if block == nil {
			break
		}

		if r.RoundTimeToMinute(block.Confirm.ConfirmTime) != timestamp {
			break
		}

		blockCnt++
		for _, receipt := range block.Confirm.ReceiptTxs {
			txGasUsed := receipt.GasUsed
			totBlockGasUsed += txGasUsed
		}
	}

	for i := len(txKeys) - 1; i >= 0; i-- {

		txHash, _ := txKeys[i].(common.Hash)
		tx := r.GetTxListen(txHash)
		if tx == nil {
			break
		}

		if r.RoundTimeToMinute(tx.ListenTime) != timestamp {
			break
		}

		txCnt = txCnt + 1
		totTxGasLimit = totTxGasLimit + tx.Tx.Gas()
	}

	context := []interface{}{
		"blocks", blockCnt, "blockkeys", len(blockKeys), "txs", txCnt, "txkeys", len(txKeys),
		"blocksmgas", float64(totBlockGasUsed) / 1000000, "txsmgas", float64(totTxGasLimit) / 1000000,
	}
	log.Debug("Collector blocks", context...)

	for i := len(blockKeys) - 1; i >= 0; i-- {

		blockNum, _ := blockKeys[i].(uint64)
		block := r.GetBlockListen(blockNum)
		if block == nil {
			break
		}

		if r.RoundTimeToMinute(block.Confirm.ConfirmTime) != timestamp {
			break
		}

		context := []interface{}{
			"time", timestamp, "number", block.Confirm.BlockNum, "minprice", block.Confirm.MinPrice.Uint64(),
			"maxprice", block.Confirm.MaxPrice.Uint64(), "validtxs", len(block.Confirm.ValidTxs),
		}
		log.Debug("Collector block", context...)
	}
}

// Deprecated:
// RealTimePrint print block by block
func (r *GlobalCache) RealTimePrint(block *types.Block) {

	// Super RLock
	// r.TxMu.RLock()
	// defer func() {
	// 	r.TxMu.RUnlock()
	// }()

	// totTxCnt := len(block.Body().Transactions)
	// listenTxCnt := uint64(0)
	// preplayTxCnt := uint64(0)
	// inTxCnt := uint64(0)

	// for _, tx := range block.Body().Transactions {

	// 	txHash := tx.Hash()

	// 	if r.GetTxListen(txHash) != nil {
	// 		listenTxCnt++
	// 	}

	// 	preplayResult := r.GetTxPreplay(txHash)
	// 	if preplayResult != nil && len(preplayResult.PreplayResults.Rounds) >= 1 {
	// 		preplayTxCnt++
	// 		if preplayResult.FlagStatus == true {
	// 			inTxCnt++
	// 		}
	// 	}
	// }

	// listenRate := 0.0
	// preplayRate := 0.0

	// if totTxCnt != 0 {
	// 	listenRate = float64((float64)(listenTxCnt) / (float64)(totTxCnt))
	// 	preplayRate = float64((float64)(preplayTxCnt) / (float64)(totTxCnt))
	// }

	// log.Info("Collector Real Time",
	// 	"Block Num", block.Number().Uint64(),
	// 	"L/T", fmt.Sprintf("%03d/%03d(%.2f)", listenTxCnt, totTxCnt, listenRate),
	// 	"P/T", fmt.Sprintf("%03d/%03d(%.2f)-%03d", preplayTxCnt, totTxCnt, preplayRate, inTxCnt),
	// )
}

func (r *GlobalCache) Pause() {
	atomic.StoreInt32(&r.pause, 1)
}

func (r *GlobalCache) Continue() {
	atomic.StoreInt32(&r.pause, 0)
}

func (r *GlobalCache) PauseForProcess() {
	for atomic.LoadInt32(&r.pause) == 1 {
		time.Sleep(2 * time.Millisecond)
	}
}

type SimpleResult struct {
	TxHash          common.Hash        `json:"txHash"`
	Tx              *types.Transaction `json:"tx"`
	Receipt         *types.Receipt     `json:"receipt"`
	RWrecord        *RWRecord          `json:"rwrecord"`
	RWTimeStamp     uint64             `json:"timestamp"`
	RWTimeStampNano uint64             `json:"timestampNano"`
}

// CommitGround commit tx ground truth
func (r *GlobalCache) CommitGround(groundTruth *SimpleResult) {

	r.GroundCache.Add(groundTruth.TxHash, groundTruth)
}

func (r *GlobalCache) PrintGround(ground *SimpleResult) {
	bytes, e := json.Marshal(&LogRWrecord{
		TxHash:        ground.TxHash,
		RoundID:       0,
		Receipt:       ground.Receipt,
		RWrecord:      ground.RWrecord,
		Timestamp:     ground.RWTimeStamp,
		TimestampNano: ground.RWTimeStampNano,
		Filled:        -1,
	})
	if e == nil {
		log.Info("😋 " + string(bytes))
	} else {
		log.Info("😋", "ground", e)
	}
}

func (r *GlobalCache) GetGround(hash common.Hash) *SimpleResult {
	value, ok := r.GroundCache.Get(hash)
	if !ok {
		return nil
	}

	ground, ok := value.(*SimpleResult)
	if !ok {
		return nil
	}

	return ground
}

func (r *GlobalCache) GetWhiteList() []common.Address {
	keys := r.WhiteListAddr.Keys()
	whiteList := make([]common.Address, len(keys))
	for _, rawKey := range keys {
		whiteList = append(whiteList, rawKey.(common.Address))
	}
	return whiteList
}

func (r *GlobalCache) IsInWhiteList(address common.Address) bool {
	return r != nil && r.WhiteListAddr.Contains(address)
}

func (r *GlobalCache) AddInWhiteList(address common.Address) {
	r.WhiteListAddr.Add(address, struct{}{})
}

func (r *GlobalCache) UpdateInWhiteList(address common.Address) {
	if r.IsInWhiteList(address) {
		r.WhiteListAddr.Peek(address)
	}
}

type WObjectWeakReference struct {
	TxHash    common.Hash    `json:"txHash"`
	Address   common.Address `json:"address"`
	Timestamp time.Time      `json:"timestamp"`
	ObjectID  uintptr        `json:"objectID"`
	RoundID   uint64         `json:"roundID"`
}

func NewWObjectWeakReference(txHash common.Hash, address common.Address,
	timestamp time.Time, objId uintptr, roundID uint64) *WObjectWeakReference {
	return &WObjectWeakReference{
		TxHash:    txHash,
		Address:   address,
		Timestamp: timestamp,
		ObjectID:  objId,
		RoundID:   roundID,
	}
}

type WObjectWeakRefMap map[common.Address]*WObjectWeakReference

func (wrm WObjectWeakRefMap) GetMatchedRef(addr common.Address, txPreplay *TxPreplay) (*WObjectWeakReference, bool) {
	if ref, rok := wrm[addr]; rok && ref.Timestamp == txPreplay.Timestamp {
		return ref, rok
	}
	return nil, false
}

const stateObjectLen = 100

type WObjectWeakRefList []*WObjectWeakReference
type WObjectWeakRefListMap map[common.Address]WObjectWeakRefList

type WObjectWeakRefPool struct {
	objMap map[common.Address]*lru.Cache
}

func NewWObjectWeakRefPool() WObjectWeakRefPool {
	return WObjectWeakRefPool{
		objMap: make(map[common.Address]*lru.Cache),
	}
}

func (m WObjectWeakRefPool) GetWObjectWeakRefList(addr common.Address) WObjectWeakRefList {
	list := make([]*WObjectWeakReference, 0, stateObjectLen)
	if cache, ok := m.objMap[addr]; ok {
		for _, rawObj := range cache.Keys() {
			list = append(list, rawObj.(*WObjectWeakReference))
		}
	}
	return list
}

func (m WObjectWeakRefPool) IsObjectRefInPool(addr common.Address, object *WObjectWeakReference) bool {
	if cache, ok := m.objMap[addr]; ok {
		return cache.Contains(object)
	} else {
		return false
	}
}

func (m WObjectWeakRefPool) AddWObjectWeakRefList(addr common.Address, newObjList WObjectWeakRefList) {
	if _, ok := m.objMap[addr]; !ok {
		m.objMap[addr], _ = lru.New(stateObjectLen)
	}
	cache := m.objMap[addr]
	for _, obj := range newObjList {
		if cache.Contains(obj) {
			cache.Get(obj)
			continue
		}
		cache.Add(obj, struct{}{})
	}
}
