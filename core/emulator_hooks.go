package core

import (
	"github.com/ethereum/go-ethereum/core/types"
	"time"
)

/* MSRA Hooks */

type InsertChainRecorder interface {
	RecordInsertChain(time.Time, types.Blocks)
}

type TxPoolRecorder interface {
	RecordAddRemotes(time.Time, []*types.Transaction) // only new txs are passed
	RecordTxPoolSnapshot(time time.Time, pendingTxs, queueTxs []*types.Transaction)
}
