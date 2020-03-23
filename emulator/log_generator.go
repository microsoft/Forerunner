package emulator

import (
	"fmt"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"math/rand"
	"sort"
	"time"
)

/**
 * !!! IMPORTANT NOTICE: DO NOT REMOVE THIS INTERFACE !!!
 *
 * GenLogWriter is responsible for translating log semantic to concrete log, and write them.
 *
 * This interface existed to separate log generation logic completely from log recording logic.
 * Current implementation uses shared implementation, but this doesn't mean this interface is redundant.
 */
type GenLogWriter interface {
	WriteInsertChain(time.Time, types.Blocks)
	WriteTxPoolSnapshot(_ time.Time, pendingTxs, queueTxs []*types.Transaction)
	WriteAddRemotes(time.Time, []*types.Transaction)
}

type GenLogWriterImpl struct {
	Recorder *GethRecorder
}

func (w *GenLogWriterImpl) WriteInsertChain(time time.Time, chain types.Blocks) {
	w.Recorder.RecordInsertChain(time, chain)
}

func (w *GenLogWriterImpl) WriteTxPoolSnapshot(time time.Time, pendingTxs, queueTxs []*types.Transaction) {
	w.Recorder.RecordTxPoolSnapshot(time, pendingTxs, queueTxs)
}

func (w *GenLogWriterImpl) WriteAddRemotes(time time.Time, txs []*types.Transaction) {
	w.Recorder.RecordAddRemotes(time, txs)
}

type LogGenerator struct {
	Writer     GenLogWriter
	BlockChain *core.BlockChain
}

func (l *LogGenerator) addBlock(time time.Time, block *types.Block) {
	var blocks types.Blocks
	blocks = append(blocks, block)
	l.Writer.WriteInsertChain(time, blocks)
}

type timedTx struct {
	time time.Time
	tx   *types.Transaction
}

func (l *LogGenerator) Generate(startBlockInclusive, endBlockExclusive uint64) {
	fmt.Printf("Generating emulator log...\n\n")

	lastTime := time.Unix(int64(l.BlockChain.GetBlockByNumber(startBlockInclusive-1).Time()), 0)

	l.Writer.WriteTxPoolSnapshot(lastTime, nil, nil)

	for blockNumber := startBlockInclusive; blockNumber < endBlockExclusive; blockNumber += 1 {
		block := l.BlockChain.GetBlockByNumber(blockNumber)
		if block == nil {
			panic(fmt.Sprintf("addBlock finds no block for %v", blockNumber))
		}
		blockTime := time.Unix(int64(block.Time()), 0)

		var timedTxs []timedTx

		for _, tx := range block.Transactions() {
			txTime := time.Unix(
				rand.Int63n(blockTime.Unix()-lastTime.Unix())+lastTime.Unix(),
				rand.Int63n(999999999+1)).Add(-20 * time.Millisecond)

			timedTxs = append(timedTxs, timedTx{
				time: txTime,
				tx:   tx,
			})
		}

		sort.Slice(timedTxs, func(i, j int) bool {
			return timedTxs[i].time.Sub(timedTxs[j].time) < 0
		})

		for _, timedTx := range timedTxs {
			fmt.Printf("Adding tx at %v...\n", timedTx.time)

			var txs []*types.Transaction
			txs = append(txs, timedTx.tx)

			l.Writer.WriteAddRemotes(timedTx.time, txs)
		}

		fmt.Printf("Adding block %v at %v...\n", blockNumber, blockTime)

		l.addBlock(blockTime, block)

		lastTime = blockTime
	}
}
