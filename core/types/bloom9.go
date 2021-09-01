// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

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

package types

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"math/big"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

type bytesBacked interface {
	Bytes() []byte
}

const (
	// BloomByteLength represents the number of bytes used in a header log bloom.
	BloomByteLength = 256

	// BloomBitLength represents the number of bits used in a header log bloom.
	BloomBitLength = 8 * BloomByteLength
)

// Bloom represents a 2048 bit bloom filter.
type Bloom [BloomByteLength]byte

// BytesToBloom converts a byte slice to a bloom filter.
// It panics if b is not of suitable size.
func BytesToBloom(b []byte) Bloom {
	var bloom Bloom
	bloom.SetBytes(b)
	return bloom
}

// SetBytes sets the content of b to the given bytes.
// It panics if d is not of suitable size.
func (b *Bloom) SetBytes(d []byte) {
	if len(b) < len(d) {
		panic(fmt.Sprintf("bloom bytes too big %d %d", len(b), len(d)))
	}
	copy(b[BloomByteLength-len(d):], d)
}

// Add adds d to the filter. Future calls of Test(d) will return true.
func (b *Bloom) Add(d *big.Int) {
	bin := new(big.Int).SetBytes(b[:])
	bin.Or(bin, bloom9(d.Bytes()))
	b.SetBytes(bin.Bytes())
}

// Big converts b to a big integer.
func (b Bloom) Big() *big.Int {
	return new(big.Int).SetBytes(b[:])
}

func (b Bloom) Bytes() []byte {
	return b[:]
}

func (b Bloom) Test(test *big.Int) bool {
	return BloomLookup(b, test)
}

func (b Bloom) TestBytes(test []byte) bool {
	return b.Test(new(big.Int).SetBytes(test))

}

// MarshalText encodes b as a hex string with 0x prefix.
func (b Bloom) MarshalText() ([]byte, error) {
	return hexutil.Bytes(b[:]).MarshalText()
}

// UnmarshalText b as a hex string with 0x prefix.
func (b *Bloom) UnmarshalText(input []byte) error {
	return hexutil.UnmarshalFixedText("Bloom", input, b[:])
}

// Pipelined bloom creation assuming that only a single process will call it
//var bloomPool = grpool.NewPool(1, 1000)
//var bloomMutex sync.Mutex

type SingleThreadSpinningAsyncProcessor struct {
	theJob   unsafe.Pointer
	stopFlag int64
	wg       sync.WaitGroup
	mutex    sync.Mutex
}

func NewSingleThreadAsyncProcessor() *SingleThreadSpinningAsyncProcessor {
	sp := &SingleThreadSpinningAsyncProcessor{}
	sp.Pause()
	go sp.loop()
	return sp
}

func (sp *SingleThreadSpinningAsyncProcessor) loop() {
	for atomic.LoadInt64(&sp.stopFlag) == 0 {
		sp.mutex.Lock() // use mutex to make sure that Pause will never be called within Wait
		sp.wg.Wait()
		sp.mutex.Unlock()
		if jobPointer := atomic.LoadPointer(&sp.theJob); jobPointer != nil {
			theJob := *((*func())(jobPointer))
			theJob()
			atomic.StorePointer(&sp.theJob, nil)
		}
	}
}

// this can only be called from a single thread
func (sp *SingleThreadSpinningAsyncProcessor) RunJob(job func()) {
	theJob := unsafe.Pointer(&job)
	for atomic.CompareAndSwapPointer(&sp.theJob, nil, theJob) != true {
		runtime.Gosched()
	}
}

func (sp *SingleThreadSpinningAsyncProcessor) Stop() {
	atomic.StoreInt64(&sp.stopFlag, 1)
}

func (sp *SingleThreadSpinningAsyncProcessor) Pause() {
	sp.mutex.Lock()
	sp.wg.Add(1)
	sp.mutex.Unlock()
}

func (sp *SingleThreadSpinningAsyncProcessor) Start() {
	sp.wg.Done()
}

type ParallelBloomProcessor struct {
	bloomWg       sync.WaitGroup
	blockBloomBin *big.Int
	receiptChan   chan *Receipt
	stopped       bool
}

func NewParallelBloomProcessor() *ParallelBloomProcessor {
	bp := &ParallelBloomProcessor{
		blockBloomBin: new(big.Int),
		receiptChan:   make(chan *Receipt, 1000),
	}
	go bp.bloomWorker()
	return bp
}

func (bp *ParallelBloomProcessor) bloomWorker() {
	for {
		select {
		case receipt := <-bp.receiptChan:
			{
				if receipt == nil {
					return
				}
				bin := LogsBloom(receipt.Logs)
				receipt.Bloom = BytesToBloom(bin.Bytes())
				bp.blockBloomBin.Or(bp.blockBloomBin, bin)
				bp.bloomWg.Done()
			}
		}
	}
}

func (bp *ParallelBloomProcessor) Stop() {
	if !bp.stopped {
		bp.stopped = true
		close(bp.receiptChan)
	}
}

func (bp *ParallelBloomProcessor) CreateBloomForTransaction(receipt *Receipt) {
	if len(receipt.Logs) == 0 {
		receipt.Bloom = CreateBloom(Receipts{receipt})
		return
	}

	bp.bloomWg.Add(1)
	bp.receiptChan <- receipt
	return
}

func (bp *ParallelBloomProcessor) GetBloomForCurrentBlock() Bloom {
	bp.bloomWg.Wait()
	blockBloom := BytesToBloom(bp.blockBloomBin.Bytes())
	return blockBloom
}

func CreateBloom(receipts Receipts) Bloom {
	bin := new(big.Int)
	for _, receipt := range receipts {
		bin.Or(bin, LogsBloom(receipt.Logs))
	}

	return BytesToBloom(bin.Bytes())
}

func LogsBloom(logs []*Log) *big.Int {
	bin := new(big.Int)
	for _, log := range logs {
		bin.Or(bin, bloom9(log.Address.Bytes()))
		for _, b := range log.Topics {
			bin.Or(bin, bloom9(b[:]))
		}
	}

	return bin
}

func bloom9(b []byte) *big.Int {
	b = crypto.Keccak256(b)

	r := new(big.Int)

	for i := 0; i < 6; i += 2 {
		t := big.NewInt(1)
		b := (uint(b[i+1]) + (uint(b[i]) << 8)) & 2047
		r.Or(r, t.Lsh(t, b))
	}

	return r
}

var Bloom9 = bloom9

func BloomLookup(bin Bloom, topic bytesBacked) bool {
	bloom := bin.Big()
	cmp := bloom9(topic.Bytes())

	return bloom.And(bloom, cmp).Cmp(cmp) == 0
}
