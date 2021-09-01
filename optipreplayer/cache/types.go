// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

package cache

import (
	"bytes"

	"github.com/ethereum/go-ethereum/common"
)

// Deprecated
// TxExeRecords Main Result

type Delta struct {
	Status       string `json:"status,omitempty"`
	Confirmation string `json:"confirmation,omitempty"`
	BlockNumber  string `json:"blockNumber,omitempty"`
	Reason       string `json:"reason,omitempty"`

	Receipt *DeltaReceipt `json:"receipt,omitempty"`
	Range   *DeltaRange   `json:"range,omitempty"`
}

func (r *PreplayResult) ToTotal() *Total {
	return &Total{
		Result: r,
	}
}

type Total struct {
	Result    *PreplayResult `json:"result,omitempty"`
	Delta     *Delta         `json:"delta,omitempty"`
	Weight    int            `json:"weight"`
	Index     int            `json:"index"`
	BaseIndex int            `json:"baseIndex"`
}

func (ra *Total) Equal(rb *Total) bool {
	// return ra.Result.Equal(rb.Result)
	return false
}

// func (ra *PreplayResult) Equal(rb *PreplayResult) bool {
// 	if ra.Status != rb.Status {
// 		return false
// 	}
// 	if (ra.Receipt == nil && rb.Receipt != nil) || (ra.Receipt != nil && rb.Receipt == nil) {
// 		return false
// 	}
// 	if ra.Receipt != nil && rb.Receipt != nil && !ra.Receipt.Equal(rb.Receipt) {
// 		return false
// 	}
// 	if ra.Confirmation != rb.Confirmation {
// 		return false
// 	}
// 	if ra.BlockNumber != rb.BlockNumber {
// 		return false
// 	}
// 	if ra.Reason != rb.Reason {
// 		return false
// 	}
// 	if (ra.Range == nil && rb.Range != nil) || (ra.Range != nil && rb.Range == nil) {
// 		return false
// 	}
// 	if ra.Range != nil && rb.Range != nil && !ra.Range.Equal(rb.Range) {
// 		return false
// 	}

// 	return true
// }

type Totals []*Total

type Log struct {
	// Consensus fields:
	// address of the contract that generated the event
	Address common.Address `json:"address,omitempty" gencodec:"required"`
	// list of topics provided by the contract.
	Topics []common.Hash `json:"topics,omitempty" gencodec:"required"`
	// supplied by the contract, usually ABI-encoded
	Data []byte `json:"data,omitempty" gencodec:"required"`
}

type DeltaLog struct {
	// Consensus fields:
	// address of the contract that generated the event
	Address string `json:"address,omitempty" gencodec:"required"`
	// list of topics provided by the contract.
	Topics map[int]string `json:"topics,omitempty" gencodec:"required"`
	// supplied by the contract, usually ABI-encoded
	Data string `json:"data,omitempty" gencodec:"required"`
}

func (la *Log) Equal(lb *Log) bool {
	if la.Address != lb.Address {
		return false
	}
	if !bytes.Equal(la.Data, lb.Data) {
		return false
	}
	if len(la.Topics) != len(lb.Topics) {
		return false
	}
	for i := range la.Topics {
		if la.Topics[i] != lb.Topics[i] {
			return false
		}
	}
	return true
}

// Receipt represents the results of a transaction.
// type Receipt struct {
// 	// Consensus fields
// 	Status uint64 `json:"status"`
// 	Logs   []*Log `json:"logs,omitempty"    gencodec:"required"`

// 	// Implementation fields (don't reorder!)
// 	txHash          common.Hash
// 	ContractAddress common.Address `json:"contractAddress,omitempty"`
// 	GasUsed         uint64         `json:"gasUsed,omitempty" gencodec:"required"`
// }

type DeltaReceipt struct {
	// Consensus fields
	Status string            `json:"status,omitempty"`
	Logs   map[int]*DeltaLog `json:"logs,omitempty"    gencodec:"required"`

	// Implementation fields (don't reorder!)
	txHash          string
	ContractAddress string `json:"contractAddress,omitempty"`
	GasUsed         string `json:"gasUsed,omitempty" gencodec:"required"`
}

// func (ra *Receipt) Equal(rb *Receipt) bool {
// 	if ra.Status != rb.Status {
// 		return false
// 	}
// 	if ra.ContractAddress != rb.ContractAddress {
// 		return false
// 	}
// 	if ra.GasUsed != rb.GasUsed {
// 		return false
// 	}
// 	if len(ra.Logs) != len(rb.Logs) {
// 		return false
// 	}
// 	for i := range ra.Logs {
// 		if !ra.Logs[i].Equal(rb.Logs[i]) {
// 			return false
// 		}
// 	}
// 	return true
// }

// func ToReceipt(receipt *types.Receipt) *Receipt {
// 	ret := &Receipt{
// 		Status:          receipt.Status,
// 		txHash:          receipt.TxHash,
// 		ContractAddress: receipt.ContractAddress,
// 		GasUsed:         receipt.GasUsed,
// 	}

// 	ret.Logs = []*Log{}
// 	for _, log := range receipt.Logs {
// 		ret.Logs = append(ret.Logs, &Log{
// 			Address: log.Address,
// 			Topics:  log.Topics,
// 			Data:    log.Data,
// 		})
// 	}

// 	return ret
// }

type DeltaRange struct {
	Min      string `json:"min,omitempty"`
	Max      string `json:"max,omitempty"`
	GasPrice string `json:"gasPrice,omitempty"`
}
