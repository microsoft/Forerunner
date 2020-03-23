package emulator

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"time"
)

const (
	// !!!
	// Keep the values unique across all versions
	// and change them when backwards compatibility is broken
	// Used values: []
	ReplayMsgUnknown = 0 // when missing `type` field
	ReplayMsgBlocks  = 17
	ReplayMsgTxs     = 97
	ReplayMsgTxPool  = 23
)

type ReplayMsg interface {
	GetTime() time.Time
}

type replayMsg struct {
	Time time.Time `json:"time"`
	Type int       `json:"type"`
}

func (r replayMsg) GetTime() time.Time {
	return r.Time
}

func newReplayMsg(Type int, time time.Time) replayMsg {
	return replayMsg{Time: time, Type: Type}
}

func deserializeLine(line []byte) (ReplayMsg, error) {
	var dec replayMsg
	if err := json.Unmarshal(line, &dec); err != nil {
		return nil, err // corrupted json
	}

	switch dec.Type {
	case ReplayMsgUnknown:
		return nil, errors.New("unknown json")
	case ReplayMsgBlocks:
		return parseReplayMsgBlocks(line)
	case ReplayMsgTxs:
		return parseReplayMsgTxs(line)
	case ReplayMsgTxPool:
		return parseReplayMsgTxPool(line)
	default:
		return nil, errors.New("unknown replayMsg type")
	}

	// no return here
}

type ReplayBlockChain interface {
	InsertChain(types.Blocks) (int, error)
}

type ReplayTxPool interface {
	AddRemotes([]*types.Transaction) []error
	LoadTxPoolSnapshot(pending, queue []*types.Transaction) []error
}

type replayMsgConsumer struct {
	BlockChain ReplayBlockChain
	TxPool     ReplayTxPool

	txPoolLoaded    bool
	afterFirstBlock bool
}

func (c *replayMsgConsumer) Accept(msg interface{}) {
	switch msg.(type) {
	case *insertChainData:
		if !c.afterFirstBlock {
			blocks := msg.(*insertChainData).Blocks

			for i := range blocks {
				num := blocks[i].NumberU64()

				emulateStart := rawdb.GlobalEmulateHook.EmulateFrom()
				if num <= emulateStart+1 {
					c.afterFirstBlock = true
					break
				} else {
					panic(fmt.Errorf("first block = %v > (emulate start = %v) + 1", num, emulateStart))
				}
			}
		}

		if c.txPoolLoaded {
			pending, queued := c.TxPool.(*core.TxPool).Stats()

			var lag time.Duration
			if broker, ok := GlobalGethReplayer.broker.(*RealtimeBroker); ok {
				metrics := broker.Metrics
				if metrics.count > 0 {
					lag = time.Duration(int64(metrics.totalLag) / metrics.count)
				}
			}

			log.Info("Blocks loading", "executable", pending, "queued", queued, "lag", lag)
			c.callInsertChain(msg.(*insertChainData))

			GlobalGethReplayer.SetRealtimeMode()
		} else {
			blocks := msg.(*insertChainData).Blocks
			emulateStart := rawdb.GlobalEmulateHook.EmulateFrom()

			for i := range blocks {
				num := blocks[i].NumberU64()
				if num <= emulateStart {
					log.Info("Block skipped", "block", num)
				} else {
					log.Info("Blocks loading sequentially")
					c.callInsertChain(msg.(*insertChainData))
					break
				}
			}
		}
	case *addRemotesData:
		if c.txPoolLoaded {
			c.callAddRemotes(msg.(*addRemotesData))
		}
	case *txPoolSnapshotData:
		if !c.txPoolLoaded {
			log.Info("TxPool Loading")
			c.loadTxPoolSnapshot(msg.(*txPoolSnapshotData))
			c.txPoolLoaded = true
			log.Info("TxPool Loaded")
		}
	default:
		panic(fmt.Sprintf("unknown ReplayMsg type %T", msg))
	}
}

// InsertChain

type insertChainData struct {
	replayMsg
	Blocks types.Blocks `json:"blocks"`
}

func (r *GethRecorder) RecordInsertChain(time time.Time, chain types.Blocks) {
	r.recorder.Accept(&insertChainData{
		replayMsg: newReplayMsg(ReplayMsgBlocks, time),
		Blocks:    chain,
	})
}

func parseReplayMsgBlocks(line []byte) (*insertChainData, error) {
	var dec insertChainData
	if err := json.Unmarshal(line, &dec); err != nil {
		return nil, err
	}
	return &dec, nil
}

func (c *replayMsgConsumer) callInsertChain(dec *insertChainData) {
	if c.BlockChain != nil {
		_, _ = c.BlockChain.InsertChain(dec.Blocks)
	}
}

// AddRemotes

type addRemotesData struct {
	replayMsg
	Txs []*types.Transaction `json:"txs"`
}

func (r *GethRecorder) RecordAddRemotes(time time.Time, txs []*types.Transaction) {
	r.recorder.Accept(&addRemotesData{
		replayMsg: newReplayMsg(ReplayMsgTxs, time),
		Txs:       txs,
	})
}

func parseReplayMsgTxs(line []byte) (*addRemotesData, error) {
	var dec addRemotesData
	if err := json.Unmarshal(line, &dec); err != nil {
		return nil, err
	}
	return &dec, nil
}

func (c *replayMsgConsumer) callAddRemotes(dec *addRemotesData) {
	if c.TxPool != nil {
		_ = c.TxPool.AddRemotes(dec.Txs)
	}
}

// TxPool Snapshot

type txPoolSnapshotData struct {
	replayMsg
	PendingTxs []*types.Transaction `json:"pendingTxs"`
	QueueTxs   []*types.Transaction `json:"queueTxs"`
}

func (r *GethRecorder) RecordTxPoolSnapshot(time time.Time, pendingTxs, queueTxs []*types.Transaction) {
	r.recorder.Accept(&txPoolSnapshotData{
		replayMsg:  newReplayMsg(ReplayMsgTxPool, time),
		PendingTxs: pendingTxs,
		QueueTxs:   queueTxs,
	})
}

func parseReplayMsgTxPool(line []byte) (*txPoolSnapshotData, error) {
	var dec txPoolSnapshotData
	if err := json.Unmarshal(line, &dec); err != nil {
		return nil, err
	}
	return &dec, nil
}

func (c *replayMsgConsumer) loadTxPoolSnapshot(dec *txPoolSnapshotData) {
	if c.TxPool != nil {
		_ = c.TxPool.LoadTxPoolSnapshot(dec.PendingTxs, dec.QueueTxs)
	}
}
