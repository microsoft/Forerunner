// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

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

package optipreplayer

import (
	"math/big"
	"sort"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/params"
)

var (
	chanSize             = 10
	validBlockPriceLimit = new(big.Int).SetInt64(1000000000)
	validTxPriceLimit    = new(big.Int).SetInt64(100000000)
)

// Collector collects the arrvial information of blocks and txs
type Collector struct {
	// nodeID string
	// config *params.ChainConfig
	// engine consensus.Engine
	eth     Backend
	config  *params.ChainConfig
	chainDb ethdb.Database
	chain   *core.BlockChain

	signer types.Signer

	// ChainHead Subscriptions
	chainHeadCh  chan core.ChainHeadEvent
	chainHeadSub event.Subscription

	// NewTxs Subscription
	newTxsCh  chan core.NewTxsEvent
	newTxsSub event.Subscription

	running int32

	startCh chan struct{}
	exitCh  chan struct{}

	// Cache
	globalCache *cache.GlobalCache
}

func NewCollector(eth Backend, config *params.ChainConfig) *Collector {
	collector := &Collector{
		eth:         eth,
		config:      config,
		chain:       eth.BlockChain(),
		chainDb:     eth.ChainDb(),
		exitCh:      make(chan struct{}),
		chainHeadCh: make(chan core.ChainHeadEvent, chanSize),
		newTxsCh:    make(chan core.NewTxsEvent, chanSize),
		running:     0,
	}

	collector.signer = types.NewEIP155Signer(collector.config.ChainID)

	collector.chainHeadSub = eth.BlockChain().SubscribeChainHeadEvent(collector.chainHeadCh)
	collector.newTxsSub = eth.TxPool().SubscribeNewTxsEvent(collector.newTxsCh)

	go collector.blockLoop()
	go collector.txLoop()
	go collector.printerLoop()
	collector.start()

	return collector
}

func (c *Collector) blockLoop() {
	defer c.chainHeadSub.Unsubscribe()

	log.Info("Block loop start!")
	for {
		select {
		case req := <-c.chainHeadCh:
			c.commitNewBlock(req.Block)
			// if c.globalCache.PreplayFlag {
			// 	c.globalCache.RealTimePrint(req.Block)
			// }

			// c.commit(req.timestamp)
		case <-c.exitCh:
			return
		}
	}
}

func (c *Collector) commitNewBlock(block *types.Block) bool {

	if !c.isRunning() {
		return false
	}

	if block == nil {
		return false
	}

	// log.Info("new Block", fmt.Sprintf("%s-%s", block.Number(), block.Header().Time),
	// 	len([]int{}))

	blockNum := block.Number().Uint64()
	confirmTime := block.Header().Time
	nowTime := time.Now()

	// when new block comes, old one should always be removed
	// c.globalCache.RemoveBlock(blockNum)

	exBlock := &cache.BlockConform{
		Block:             block,
		BlockNum:          blockNum,
		BlockHash:         block.Hash(),
		MaxPrice:          big.NewInt(0),
		MinPrice:          big.NewInt(0),
		ValidTxs:          types.Transactions{},
		ReceiptTxs:        types.Receipts{},
		ConfirmTime:       confirmTime,
		ChainHeadTime:     uint64(nowTime.Unix()),
		ConfirmTimeNano:   confirmTime * 1000000000,
		ChainHeadTimeNano: uint64(nowTime.UnixNano()),
		Valid:             true,
	}

	// 0.Empty block
	if len(block.Body().Transactions) == 0 {
		exBlock.Valid = false
		return false
	}

	// 1. Validate Block

	minPrice := block.Body().Transactions[0].GasPrice()
	maxPrice := block.Body().Transactions[0].GasPrice()

	// Calculate Max and Min for checking block
	for _, tx := range block.Body().Transactions {
		txPrice := tx.GasPrice()
		if txPrice.Cmp(minPrice) < 0 {
			minPrice = txPrice
		}
		if txPrice.Cmp(maxPrice) > 0 {
			maxPrice = txPrice
		}
	}

	// The block is invalid
	if maxPrice.Cmp(validBlockPriceLimit) < 0 {
		exBlock.Valid = false
		return false
	}

	// 2. Validate Tx

	coinbase := block.Header().Coinbase
	for _, tx := range block.Body().Transactions {
		from, _ := types.Sender(c.signer, tx)
		flag := uint64(1)

		// coinbase check, same address
		if from.String() == coinbase.String() {
			flag = uint64(0)
		}

		// gasprice check, too small
		if tx.GasPrice().Cmp(validTxPriceLimit) < 0 {
			flag = uint64(0)
		}

		receipt, _ := c.getTransactionReceipt(tx.Hash())
		if (receipt) == nil {
			flag = uint64(0)
		}

		if flag == uint64(1) {
			exBlock.ValidTxs = append(exBlock.ValidTxs, tx)
			exBlock.ReceiptTxs = append(exBlock.ReceiptTxs, receipt)
		}
	}

	// All invalid
	if len(exBlock.ValidTxs) == 0 {
		exBlock.Valid = false
		return false
	}

	// Sort Valid Txs
	sort.Slice(exBlock.ValidTxs, func(i, j int) bool {
		return exBlock.ValidTxs[i].GasPrice().Cmp(exBlock.ValidTxs[j].GasPrice()) < 0
	})

	// 3. Calculate Min Max for extended block
	exBlock.MinPrice = exBlock.ValidTxs[0].GasPrice()
	exBlock.MaxPrice = exBlock.ValidTxs[0].GasPrice()
	for _, tx := range exBlock.ValidTxs {
		txPrice := tx.GasPrice()
		if txPrice.Cmp(exBlock.MinPrice) < 0 {
			exBlock.MinPrice = txPrice
		}
		if txPrice.Cmp(exBlock.MaxPrice) > 0 {
			exBlock.MaxPrice = txPrice
		}
	}

	// 4. Finish Validation and commit
	return c.globalCache.CommitBlockListen(exBlock)
}

func (c *Collector) txLoop() {
	defer c.newTxsSub.Unsubscribe()

	log.Info("Tx loop start!")
	for {
		select {
		case req := <-c.newTxsCh:
			// for _, tx := range req.Txs {
			// 	log.Info("new TX", tx.Hash().String(), len([]int{}))
			// }
			// log.Info("listen new tx")
			c.commitNewTxs(req.Txs)
		case <-c.exitCh:
			return
		}
	}
}

func (c *Collector) commitNewTxs(txs types.Transactions) bool {

	if !c.isRunning() {
		return false
	}

	if len(txs) == 0 {
		return false
	}

	nowTime := time.Now()

	// TODO: Duplicate Broadcase ?
	for _, tx := range txs {
		from, _ := types.Sender(c.signer, tx)
		exTx := &cache.TxListen{
			Tx:              tx,
			From:            from,
			ListenTime:      uint64(nowTime.Unix()),
			ListenTimeNano:  uint64(nowTime.UnixNano()),
			ConfirmTime:     0,
			ConfirmBlockNum: 0,
		}
		c.globalCache.CommitTxListen(exTx)
	}

	return true
}

func (c *Collector) getTransactionReceipt(hash common.Hash) (*types.Receipt, uint64) {
	tx, blockHash, blockNumber, index := rawdb.ReadTransaction(c.chainDb, hash)
	if tx == nil {
		return nil, 0
	}
	receipts := c.chain.GetReceiptsByHash(blockHash)

	if len(receipts) <= int(index) {
		return nil, 0
	}
	receipt := receipts[index]

	return receipt, blockNumber
}

func (c *Collector) setGlobalCache(globalCache *cache.GlobalCache) {

	c.globalCache = globalCache

}

// start sets the running Status as 1.
func (c *Collector) start() {
	log.Info("Collector start!")
	log.Info("Collector real time start!")
	log.Info("BlockReuse start!")

	atomic.StoreInt32(&c.running, 1)
}

// stop sets the running Status as 0.
func (c *Collector) stop() {
	atomic.StoreInt32(&c.running, 0)
}

// isRunning returns an indicator whether preplayers is running or not.
func (c *Collector) isRunning() bool {
	return atomic.LoadInt32(&c.running) == 1
}

// close
func (c *Collector) close() {
	close(c.exitCh)
}

func (c *Collector) printerLoop() {
	log.Info("Printer start!")

	last := c.globalCache.RoundTimeToMinute((uint64)(time.Now().Unix()))
	time.Sleep(time.Second * (time.Duration)(59-time.Now().Second()))
	for {
		select {

		case <-c.exitCh:
			return

		default:
			if time.Now().Second() == 0 && c.globalCache != nil {
				c.globalCache.BucketPrint(last)
				last = c.globalCache.RoundTimeToMinute((uint64)(time.Now().Unix()))
				time.Sleep(time.Second * 59)
			}
		}

	}
}
