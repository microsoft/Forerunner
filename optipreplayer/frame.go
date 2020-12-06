// Copyright 2014 The go-ethereum Authors
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

// Package miner implements Ethereum block creation and mining.
package optipreplayer

import (
	"fmt"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/params"
)

const (
	//executorNum             = 30
	defaultBlockCacheSize   = 6 * 300
	defaultTxCacheSize      = 3000 * 300
	defaultPreplayCacheSize = 20000
)

// Backend wraps all methods required for mining.
type Backend interface {
	BlockChain() *core.BlockChain
	TxPool() *core.TxPool
	ChainDb() ethdb.Database // Block chain database
}

// Frame creates blocks and searches for proof-of-work values.
type Frame struct {
	GlobalCache *cache.GlobalCache
	preplayers  Preplayers
	collector   *Collector
	listener    *Listener

	preplayFlag bool
	featureFlag bool

	mux      *event.TypeMux
	coinbase common.Address
	eth      Backend
	engine   consensus.Engine
	exitCh   chan struct{}

	canStart    int32 // can start indicates whether we can start the mining operation
	shouldStart int32 // should start indicates whether we should start after sync
}

// NewFrame create a new frame
func NewFrame(eth Backend, config *params.ChainConfig, mux *event.TypeMux, engine consensus.Engine, gasFloor, gasCeil uint64, singleFuture bool) *Frame {
	listener := NewListener(eth)
	frame := &Frame{
		eth:         eth,
		preplayFlag: false,
		featureFlag: false,
		mux:         mux,
		engine:      engine,
		exitCh:      make(chan struct{}),
		preplayers:  NewPreplayers(eth, config, engine, gasFloor, gasCeil, listener, singleFuture),
		collector:   NewCollector(eth, config),
		listener:    listener,
		canStart:    1,
	}
	// frame.SetGlobalCache(cache.NewGlobalCache(defaultBlockCacheSize, defaultTxCacheSize, defaultPreplayCacheSize))

	// frame.preplayers = NewPreplayers(eth, config, engine, gasFloor, gasCeil)
	// frame.collector = NewCollector(eth, config)

	// go frame.update()

	return frame
}

// update keeps track of the downloader events. Please be aware that this is a one shot type of update loop.
// It's entered once and as soon as `Done` or `Failed` has been broadcasted the events are unregistered and
// the loop is exited. This to prevent a major security vuln where external parties can DOS you with blocks
// and halt your mining operation for as long as the DOS continues.
func (f *Frame) update() {
	events := f.mux.Subscribe(downloader.StartEvent{}, downloader.DoneEvent{}, downloader.FailedEvent{})
	defer events.Unsubscribe()

	for {
		select {
		case ev := <-events.Chan():
			if ev == nil {
				return
			}
			switch ev.Data.(type) {
			case downloader.StartEvent:
				atomic.StoreInt32(&f.canStart, 0)
				if f.Mining() {
					f.Stop()
					atomic.StoreInt32(&f.shouldStart, 1)
				}
			case downloader.DoneEvent, downloader.FailedEvent:
				// log.Info("Download End!!!!!!!!!!!!!!!!!!!!!!!!!!!")
				shouldStart := atomic.LoadInt32(&f.shouldStart) == 1

				atomic.StoreInt32(&f.canStart, 1)
				atomic.StoreInt32(&f.shouldStart, 0)
				if shouldStart {
					f.Start(f.coinbase)
				}
				// stop immediately and ignore all further pending events
				return
			}
		case <-f.exitCh:
			return
		}
	}
}

func (f *Frame) Start(coinbase common.Address) {
	atomic.StoreInt32(&f.shouldStart, 1)
	f.SetEtherbase(coinbase)

	f.preplayers.start()
	f.collector.start()

	if atomic.LoadInt32(&f.canStart) == 0 {
		log.Info("Network syncing, will start miner afterwards")
		return
	}

}

func (f *Frame) Stop() {
	f.preplayers.stop()
	atomic.StoreInt32(&f.shouldStart, 0)
}

func (f *Frame) Close() {
	f.preplayers.close()
	close(f.exitCh)
}

func (f *Frame) Mining() bool {
	return f.preplayers.isRunning()
}

func (f *Frame) HashRate() uint64 {
	if pow, ok := f.engine.(consensus.PoW); ok {
		return uint64(pow.Hashrate())
	}
	return 0
}

func (f *Frame) SetExtra(extra []byte) error {
	if uint64(len(extra)) > params.MaximumExtraDataSize {
		return fmt.Errorf("Extra exceeds max length. %d > %v", len(extra), params.MaximumExtraDataSize)
	}
	f.preplayers.setExtra(extra)
	return nil
}

func (f *Frame) SetEtherbase(addr common.Address) {
	f.coinbase = addr
}

func (f *Frame) SetNodeID(nodeID string) {
	f.preplayers.setNodeID(nodeID)
}

func (f *Frame) SetGlobalCache(globalCache *cache.GlobalCache) {
	f.GlobalCache = globalCache

	f.preplayers.setGlobalCache(globalCache)
	f.collector.setGlobalCache(globalCache)
	f.listener.setGlobalCache(globalCache)
}

func (f *Frame) GetPreplayer(pID uint64) *Preplayer {
	return f.preplayers[pID]
}

func (f *Frame) GetMissReporter() *MissReporter {
	for _, preplayer := range f.preplayers {
		return preplayer.missReporter
	}
	return nil
}

func (f *Frame) GetGlobalCache() *cache.GlobalCache {
	return f.GlobalCache
}

// SetPreplayFlag set preplay flag
func (f *Frame) SetPreplayFlag(preplayFlag bool) {
	f.preplayFlag = preplayFlag
	f.GlobalCache.PreplayFlag = preplayFlag
	if !f.preplayFlag {
		f.preplayers.close()
	}

	log.Info(fmt.Sprintf("Set preplay flag: %t", f.preplayFlag))

	// if f.preplayFlag && f.featureFlag {
	// 	f.GlobalCache.ResetGlobalCache(60*6, 60*3000, 0)
	// }
}

// SetFeatureFlag set feature flag
func (f *Frame) SetFeatureFlag(featureFlag bool) {
	f.featureFlag = featureFlag
	f.GlobalCache.FeatureFlag = featureFlag
	if !f.featureFlag {
		f.collector.close()
	}

	log.Info(fmt.Sprintf("Set feature flag: %t", f.featureFlag))

	// if f.preplayFlag && f.featureFlag {
	// 	f.GlobalCache.ResetGlobalCache(60*6, 60*3000, 0)
	// }
}
