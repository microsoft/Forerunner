// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

package cache

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

// BlockPre record block before process
type BlockPre struct {
	Block          *types.Block
	BlockHash      common.Hash
	BlockNum       uint64
	ListenTime     uint64 // Before process; use to set timestamp of the next block; In Emulate mode, `ListenTime` is the logged listen time
	ListenTimeNano uint64 // whether it is in Emulate Mode or not, ListenTimeNano is always the current time (time.Now)
}

// NewBlockPre creat new block pre
func NewBlockPre(block *types.Block) *BlockPre {
	curTime := time.Now()
	return &BlockPre{
		Block:          block,
		BlockNum:       block.Header().Number.Uint64(),
		BlockHash:      block.Hash(),
		ListenTime:     uint64(curTime.Unix()),
		ListenTimeNano: uint64(curTime.UnixNano()),
	}
}

// NewBlockPre creat new block pre
func NewBlockPreWithListenTime(block *types.Block, listenTime time.Time) *BlockPre {

	return &BlockPre{
		Block:          block,
		BlockNum:       block.Header().Number.Uint64(),
		BlockHash:      block.Hash(),
		ListenTime:     uint64(listenTime.Unix()),
		ListenTimeNano: uint64(time.Now().UnixNano()),
	}
}

// PeekBlockPre Get Block Pre without updating the priority
func (r *GlobalCache) PeekBlockPre(hash common.Hash) *BlockPre {

	result, ok := r.BlockPreCache.Peek(hash)
	if !ok {
		return nil
	}

	// unmarsh
	blockPre, ok := result.(*BlockPre)
	if !ok {
		return nil
	}

	return blockPre
}

// CommitBlockPre commit block pre
func (r *GlobalCache) CommitBlockPre(b *BlockPre) {

	r.BlockPreCache.Add(b.BlockHash, b)
	r.PreplayTimestamp = b.ListenTime

	return
}

// BlockListen extended block structure
type BlockListen struct {
	BlockPre *BlockPre
	Confirm  *BlockConform
}

// BlockConform contain information after block is confirmed and broadcast
type BlockConform struct {
	Block     *types.Block
	BlockNum  uint64
	BlockHash common.Hash

	MinPrice *big.Int
	MaxPrice *big.Int

	ValidTxs   types.Transactions // sorted by gas price
	ReceiptTxs types.Receipts

	ConfirmTime   uint64
	ChainHeadTime uint64

	ConfirmTimeNano   uint64
	ChainHeadTimeNano uint64

	Valid bool
}

// BlockListens extended blocks list
type BlockListens []*BlockListen

// RemoveBlock remove certain block
func (r *GlobalCache) RemoveBlock(blockNum uint64) bool {

	r.BlockMu.Lock()
	defer r.BlockMu.Unlock()

	r.BlockCache.Remove(blockNum)

	return true
}

// GetBlockListen Get Block Listen
func (r *GlobalCache) GetBlockListen(blockNum uint64) *BlockListen {

	result, ok := r.BlockCache.Peek(blockNum)
	if !ok {
		return nil
	}

	// unmarsh
	exBlock, ok := result.(*BlockListen)
	if !ok {
		return nil
	}

	return exBlock
}

// CommitBlockListen commit confirm info
func (r *GlobalCache) CommitBlockListen(b *BlockConform) bool {

	r.BlockMu.Lock()
	defer r.BlockMu.Unlock()

	blockPre := r.PeekBlockPre(b.BlockHash)
	if blockPre == nil {
		log.Info("[ListenCache] CommitBlockListen Error", "block", b.BlockNum)
		return false
	}

	blockListen := &BlockListen{
		BlockPre: blockPre,
		Confirm:  b,
	}

	r.BlockCache.Add(b.BlockNum, blockListen)

	return true
}

// TxListen extended transaction structure
type TxListen struct {
	Tx *types.Transaction

	From common.Address

	ListenTime        uint64
	ListenTimeNano    uint64
	ConfirmTime       uint64
	ConfirmListenTime uint64
	ConfirmBlockNum   uint64
}

// TxListens extended transactions list
type TxListens []*TxListen

// GetTxListen Get Tx Listen
func (r *GlobalCache) GetTxListen(hash common.Hash) *TxListen {

	r.TxMu.RLock()
	result, ok := r.TxListenCache.Peek(hash)
	r.TxMu.RUnlock()
	if !ok {
		return nil
	}

	// unmarsh
	exTx, ok := result.(*TxListen)
	if !ok {
		return nil
	}

	return exTx
}

func (r *GlobalCache) GetTxEnpool(hash common.Hash) uint64 {
	if result, ok := r.TxEnpoolCache.Peek(hash); ok {
		return result.(uint64)
	} else {
		return 0
	}
}

func (r *GlobalCache) GetTxEnpending(hash common.Hash) uint64 {
	if result, ok := r.TxEnpendingCache.Peek(hash); ok {
		return result.(uint64)
	} else {
		return 0
	}
}

func (r *GlobalCache) GetTxPackage(hash common.Hash) uint64 {
	if result, ok := r.TxPackageCache.Peek(hash); ok {
		return result.(uint64)
	} else {
		return 0
	}
}

func (r *GlobalCache) GetTxEnqueue(hash common.Hash) [2]uint64 {
	if result, ok := r.TxEnqueueCache.Peek(hash); ok {
		return result.([2]uint64)
	} else {
		return [2]uint64{}
	}
}

// CommitTxListen commit tx
func (r *GlobalCache) CommitTxListen(tx *TxListen) {
	r.TxMu.Lock()
	defer r.TxMu.Unlock()

	// Duplicate ?
	// for _, iTx := range bucket.ArrTxs {
	// 	if iTx.Hash().String() == tx.Hash().String() {
	// 		return true
	// 	}
	// }

	// Re-in
	// tmpTx := r.GetTxListen(tx.Tx.Hash())
	// if tmpTx != nil {
	// 	return
	// }

	r.TxListenCache.ContainsOrAdd(tx.Tx.Hash(), tx)
}

func (r *GlobalCache) CommitTxEnpool(tx common.Hash, txEnpool uint64) {
	r.TxEnpoolCache.ContainsOrAdd(tx, txEnpool)
}

func (r *GlobalCache) CommitTxEnpending(tx common.Hash, txEnpending uint64) {
	r.TxEnpendingCache.ContainsOrAdd(tx, txEnpending)
}

func (r *GlobalCache) CommitTxPackage(tx common.Hash, txPackage uint64) {
	r.TxPackageCache.ContainsOrAdd(tx, txPackage)
}

func (r *GlobalCache) CommitTxEnqueue(tx common.Hash, txEnqueue uint64) {
	if rawEnqueue, ok := r.TxEnqueueCache.Peek(tx); ok {
		enqueue := rawEnqueue.([2]uint64)
		enqueue[1] = txEnqueue
		r.TxEnqueueCache.Add(tx, enqueue)
	} else {
		r.TxEnqueueCache.Add(tx, [2]uint64{txEnqueue, txEnqueue})
	}
}

// func (r *GlobalCache) GetListenTxCnt(block *types.Block) uint64 {

// 	// Super RLock
// 	r.TxMu.RLock()
// 	defer func() {
// 		r.TxMu.RUnlock()
// 	}()

// 	listenTxCnt := uint64(0)

// 	for _, tx := range block.Body().Transactions {

// 		txHash := tx.Hash()

// 		if r.GetTxFromCache(txHash) != nil {
// 			listenTxCnt++
// 		}
// 	}

// 	return listenTxCnt
// }
