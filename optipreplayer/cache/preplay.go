package cache

import (
	"bytes"
	"encoding/gob"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	lru "github.com/hashicorp/golang-lru"
)

var (
	roundLimit = 100
)

// TxPreplay Three cache structure
type TxPreplay struct {
	// Result
	PreplayResults *PreplayResults

	// Tx Info
	TxHash         common.Hash        `json:"txHash"`

	//Currently, this field is useless and was even wrong
	//From           common.Address     `json:"from"`

	GasPrice       *big.Int           `json:"gasPrice"`
	GasLimit       uint64             `json:"gasLimit"`
	Tx             *types.Transaction `json:"tx"`
	ListenTime     uint64             `json:"listenTime"`
	ListenTimeNano uint64             `json:"listenTimeNano"`

	// Setting Info
	FlagStatus bool // Flag: 0: not in, 1: already in
	Mu         sync.RWMutex
}

// NewTxPreplay create new RWRecord
func NewTxPreplay(tx *types.Transaction) *TxPreplay {
	preplayResults := &PreplayResults{}
	preplayResults.Rounds, _ = lru.New(roundLimit)
	preplayResults.RWrecords, _ = lru.New(roundLimit)
	return &TxPreplay{
		PreplayResults: preplayResults,
		TxHash:         tx.Hash(),
		GasPrice:       tx.GasPrice(),
		GasLimit:       tx.Gas(),
		Tx:             tx,
	}
}

// CreateRound create new round preplay for tx
func (t *TxPreplay) CreateRound(roundID uint64) bool {
	_, ok := t.PreplayResults.Rounds.Get(roundID)
	if ok == true {
		return false
	}

	t.PreplayResults.Rounds.Add(roundID, &PreplayResult{
		RoundID:  roundID,
		TxHash:   t.TxHash,
		GasPrice: t.GasPrice,
		Filled:   -1,
	})
	return true
}

// GetRound get round by roundID
func (t *TxPreplay) GetRound(roundID uint64) (*PreplayResult, bool) {

	rawRound, ok := t.PreplayResults.Rounds.Get(roundID)

	if !ok {
		return nil, false
	}

	return rawRound.(*PreplayResult), true
}

// PeekRound peek round by roundID
func (t *TxPreplay) PeekRound(roundID uint64) (*PreplayResult, bool) {

	rawRound, ok := t.PreplayResults.Rounds.Peek(roundID)

	if !ok {
		return nil, false
	}

	return rawRound.(*PreplayResult), true
}

// PreplayResults record results of several rounds
type PreplayResults struct {
	Rounds    *lru.Cache `json:"-"`
	RWrecords *lru.Cache `json:"-"`
}

// PreplayResult record one round result
type PreplayResult struct {
	// Basic Info
	TxHash   common.Hash `json:"txHash"`
	RoundID  uint64      `json:"roundId"` // RoundID Info
	GasPrice *big.Int    `json:"gasPrice"`

	// Main Result
	Receipt       *types.Receipt `json:"receipt"`
	RWrecord      *RWRecord      `json:"rwrecord"`
	Timestamp     uint64         `json:"timestamp"` // Generation Time
	TimestampNano uint64         `json:"timestampNano"`

	// Extra Result
	CurrentState *CurrentState `json:"-"`
	ExtraResult  *ExtraResult  `json:"extraResult"`

	// FlagStatus: 0 will in, 1 in, 2 will not in
	FlagStatus uint64 `json:"flagStatus"`

	// Filled
	Filled int64
}

// RWRecord for record
type RWRecord struct {
	RWHash string
	IterMu sync.Mutex
	// HashOrder [][]byte

	RState map[common.Address]map[cmptypes.Field]interface{}
	RChain map[cmptypes.Field]interface{}
	WState map[common.Address]map[cmptypes.Field]interface{}

	Failed bool
	Hashed bool

	Round *PreplayResult `json:"-"`
}

// NewRWRecord create new RWRecord
func NewRWRecord(
	rstate map[common.Address]map[cmptypes.Field]interface{},
	rchain map[cmptypes.Field]interface{},
	wstate map[common.Address]map[cmptypes.Field]interface{},
	failed bool,
) *RWRecord {
	return &RWRecord{
		RState: rstate,
		RChain: rchain,
		WState: wstate,
		Failed: failed,
		Hashed: false,
	}
	// return nil
}

// Equal judge equal
func (rw *RWRecord) Equal(rwb *RWRecord) bool {
	// print(rw.GetHash().String())
	if rw == nil && rwb == nil {
		return true
	}

	if rwb == nil || rw == nil {
		return false
	}

	if rw.Failed != rwb.Failed {
		return false
	}
	// log.Debug("Cmp " + " " + rw.GetHash() + " | " + rwb.GetHash())
	if rw.GetHash() != rwb.GetHash() {
		// log.Debug("Wrong")
		return false
	} else {
		// log.Debug("Right")
	}
	// for key := range rw.RChain {
	// 	switch key {
	// 	case cmptypes.Gasprice:
	// 		continue
	// 	case cmptypes.Coinbase:
	// 		v := rw.RChain[key].(common.Address)
	// 		vb, ok := rwb.RChain[key].(common.Address)
	// 		if !ok || v.Big().Cmp(vb.Big()) != 0 {
	// 			return false
	// 		}
	// 	case cmptypes.Timestamp, cmptypes.Number, cmptypes.Difficulty:
	// 		v := rw.RChain[key].(*big.Int)
	// 		vb, ok := rwb.RChain[key].(*big.Int)
	// 		if !ok || v.Cmp(vb) != 0 {
	// 			return false
	// 		}
	// 	case cmptypes.GasLimit:
	// 		v := rw.RChain[key].(uint64)
	// 		vb, ok := rwb.RChain[key].(uint64)
	// 		if !ok || v != vb {
	// 			return false
	// 		}
	// 	case cmptypes.Blockhash:
	// 		mBlockHash := rw.RChain[key].(map[uint64]common.Hash)
	// 		mBlockHashb, ok := rwb.RChain[key].(map[uint64]common.Hash)

	// 		if !ok {
	// 			return false
	// 		}

	// 		for num, blkHash := range mBlockHash {
	// 			blkHashb, ok := mBlockHashb[num]
	// 			if !ok || blkHash.Big().Cmp(blkHashb.Big()) != 0 {
	// 				return false
	// 			}
	// 		}
	// 	}
	// }

	// for addr, mValues := range rw.RState {
	// 	for key := range mValues {
	// 		switch key {
	// 		case cmptypes.Exist, cmptypes.Empty:
	// 			v := rw.RState[addr][key].(bool)
	// 			vb, ok := rwb.RState[addr][key].(bool)
	// 			if !ok || v != vb {
	// 				return false
	// 			}
	// 		case cmptypes.Balance:
	// 			v := rw.RState[addr][key].(*big.Int)
	// 			vb, ok := rwb.RState[addr][key].(*big.Int)
	// 			if !ok || v.Cmp(vb) != 0 {
	// 				// log.Info("RState Balance miss", "pve", v, "now", statedb.GetBalance(addr))
	// 				return false
	// 			}
	// 		case cmptypes.Nonce:
	// 			v := rw.RState[addr][key].(uint64)
	// 			vb, ok := rwb.RState[addr][key].(uint64)
	// 			if !ok || v != vb {
	// 				// log.Info("RState Nonce miss", "pve", v, "now", statedb.GetNonce(addr))
	// 				return false
	// 			}
	// 		case cmptypes.CodeHash:
	// 			v := rw.RState[addr][key].(common.Hash)
	// 			vb, ok := rwb.RState[addr][key].(common.Hash)
	// 			if !ok || v.Big().Cmp(vb.Big()) != 0 {
	// 				// log.Info("RState CodeHash miss", "pve", v.Big(), "now", statedb.GetCodeHash(addr).Big())
	// 				return false
	// 			}
	// 		case cmptypes.Storage, cmptypes.CommittedStorage:
	// 			storage := rw.RState[addr][key].(map[common.Hash]common.Hash)
	// 			storageb, ok := rwb.RState[addr][key].(map[common.Hash]common.Hash)

	// 			if !ok {
	// 				return false
	// 			}

	// 			for k, v := range storage {
	// 				vb, ok := storageb[k]
	// 				if !ok || v.Big().Cmp(vb.Big()) != 0 {
	// 					return false
	// 				}
	// 			}
	// 		}
	// 	}
	// }

	return true
}

// GetBytes get hash fir rw record
func GetBytes(v interface{}) []byte {
	var b bytes.Buffer
	b.Reset()
	gob.NewEncoder(&b).Encode(v)
	return b.Bytes()
}

// GetHash get hash function
func (rw *RWRecord) GetHash() string {
	if rw.Hashed {
		return rw.RWHash
	}
	rw.IterMu.Lock()
	defer rw.IterMu.Unlock()

	var res [][]byte

	for key := range rw.RChain {
		switch key {
		case cmptypes.Coinbase:
			v := rw.RChain[key].(common.Address)
			res = append(res, GetBytes(v.String()))
		case cmptypes.Timestamp, cmptypes.Number, cmptypes.Difficulty:
			v := rw.RChain[key].(*big.Int)
			res = append(res, GetBytes(v.String()))
		case cmptypes.GasLimit:
			v := rw.RChain[key].(uint64)
			res = append(res, GetBytes(v))
		case cmptypes.Blockhash:
			mBlockHash, ok := rw.RChain[key].(map[uint64]common.Hash)
			if ok {
				for _, blkHash := range mBlockHash {
					res = append(res, GetBytes(blkHash.Big().String()))
				}
			} else {
				mBlockHash := rw.RChain[key].(map[uint64]*big.Int)
				for _, blkHash := range mBlockHash {
					res = append(res, GetBytes(blkHash.String()))
				}
			}

		}
	}

	for addr, mValues := range rw.RState {
		for key := range mValues {
			switch key {
			case cmptypes.Exist, cmptypes.Empty:
				v := rw.RState[addr][key].(bool)
				res = append(res, GetBytes(v))
			case cmptypes.Balance:
				v := rw.RState[addr][key].(*big.Int)
				res = append(res, GetBytes(v.String()))
			case cmptypes.Nonce:
				v := rw.RState[addr][key].(uint64)
				res = append(res, GetBytes(v))
			case cmptypes.CodeHash:
				v := rw.RState[addr][key].(common.Hash)
				res = append(res, GetBytes(v.String()))
			case cmptypes.Storage, cmptypes.CommittedStorage:
				storage := rw.RState[addr][key].(map[common.Hash]common.Hash)
				for _, v := range storage {
					res = append(res, GetBytes(v.String()))
				}
			}
		}
	}

	sort.Slice(res, func(i, j int) bool {
		return bytes.Compare(res[i], res[j]) == -1
	})
	// rw.HashOrder = res
	rw.RWHash = crypto.Keccak256Hash(res...).String()
	rw.Hashed = true

	return rw.RWHash
}

// Dump create tmpMap for log print
func (rw *RWRecord) Dump() map[string]interface{} {
	tmpMap := make(map[string]interface{})
	tmpMap["RState"] = rw.RState
	tmpMap["RChain"] = rw.RChain
	tmpMap["WState"] = rw.WState
	return tmpMap
}

// CurrentState Extra Result part 1
type CurrentState struct {
	PreplayID         string             `json:"preplayID"`
	PreplayName       string             `json:"preplayName"`
	Number            uint64             `json:"number"`
	Hash              string             `json:"hash"`
	RawHash           common.Hash        `json:"-"`
	Txs               types.Transactions `json:"-"`
	TotalDifficuly    string             `json:"totalDifficulty"`
	SnapshotTimestamp int64              `json:"snapshotTimestamp"`
	StartTimeString   string             `json:"startTimestamp"`
	EndTimeString     string             `json:"endTimestamp"`
}

// ExtraResult Extra Result part 2
type ExtraResult struct {
	TxHash        common.Hash `json:"-"`
	Status        string      `json:"status,omitempty"`
	Confirmation  int64       `json:"confirmation,omitempty"`
	BlockNumber   uint64      `json:"blockNumber,omitempty"`
	Reason        string      `json:"reason,omitempty"`
	Timestamp     uint64      `json:"timestamp"` // Generation Time
	TimestampNano uint64      `json:"timestampNano"`

	Receipt *types.Receipt `json:"-"`
	Range   *Range         `json:"range,omitempty"`
	Rank    *Rank          `json:"rank,omitempty"`
}

// Range contain range info
type Range struct {
	Min      uint64 `json:"min,omitempty"`
	Max      uint64 `json:"max,omitempty"`
	GasPrice uint64 `json:"gasPrice,omitempty"`
	txHash   common.Hash
}

// UpdateMin Equal
func (r *Range) UpdateMin(left uint64) {
	if left < r.Min {
		r.Min = left
	}
}

// UpdateMax Equal
func (r *Range) UpdateMax(right uint64) {
	if right > r.Max {
		r.Max = right
	}
}

// Equal Equal
func (r *Range) Equal(rb *Range) bool {
	if r.Min != rb.Min {
		return false
	}
	if r.Max != rb.Max {
		return false
	}
	if r.GasPrice != rb.GasPrice {
		return false
	}
	return true
}

// Copy Copy
func (r *Range) Copy(h common.Hash) *Range {
	tR := &Range{
		Min:      r.Min,
		Max:      r.Max,
		GasPrice: r.GasPrice,
		txHash:   h,
	}
	return tR
}

// TxHash txHash
func (r *Range) TxHash() common.Hash {
	return r.txHash
}

// Rank contain rank info
type Rank struct {
	PriceRk      uint64 `json:"priceRank,omitempty"`
	SmTxNum      uint64 `json:"sameTxNum"`
	SmTotGasUsed uint64 `json:"sameTotGasUsed"`

	GtTxNum      uint64 `json:"greaterTxNum"`
	GtTotGasUsed uint64 `json:"greaterTotGasUsed"`

	txHash common.Hash
}

// Equal judge equal
func (ra *Rank) Equal(rb *Rank) bool {
	if ra.PriceRk != rb.PriceRk {
		return false
	}
	if ra.SmTxNum != rb.SmTxNum {
		return false
	}
	if ra.SmTotGasUsed != rb.SmTotGasUsed {
		return false
	}
	if ra.GtTxNum != rb.GtTxNum {
		return false
	}
	if ra.GtTotGasUsed != rb.GtTotGasUsed {
		return false
	}

	return true
}

// Copy copy
func (ra *Rank) Copy(h common.Hash) *Rank {
	tR := &Rank{
		PriceRk:      ra.PriceRk,
		SmTxNum:      ra.SmTxNum,
		SmTotGasUsed: ra.SmTotGasUsed,
		GtTxNum:      ra.GtTxNum,
		GtTotGasUsed: ra.GtTotGasUsed,
		txHash:       h,
	}
	return tR
}

// TxHash return hash
func (ra *Rank) TxHash() common.Hash {
	return ra.txHash
}

// GetTxPreplay return the result of preplay
func (r *GlobalCache) GetTxPreplay(txHash common.Hash) *TxPreplay {

	// r.ResultMu.RLock()
	// defer r.ResultMu.RUnlock()

	result, response := r.PreplayCache.Get(txHash)

	if !response {
		return nil
	}

	if tx, ok := result.(*TxPreplay); ok {
		return tx
	}

	return nil
}

// GetPreplayCacheTxs return all the tx that in cache
func (r *GlobalCache) GetPreplayCacheTxs() map[common.Address]types.Transactions {

	// r.PreplayMu.RLock()
	// defer r.PreplayMu.RUnlock()

	// cacheTxs := make(map[common.Address]types.Transactions)
	// keys := r.PreplayCache.Keys()
	// for _, key := range keys {
	// 	iTx, ok := r.PreplayCache.Peek(key)
	// 	if !ok {
	// 		continue
	// 	}

	// 	tx, ok := iTx.(*TxPreplay)
	// 	if !ok {
	// 		continue
	// 	}

	// 	if tx.FlagStatus != true {

	// 		if _, ok := cacheTxs[tx.From]; !ok {
	// 			cacheTxs[tx.From] = types.Transactions{}
	// 		}

	// 		cacheTxs[tx.From] = append(cacheTxs[tx.From], tx.Tx)
	// 	}
	// }

	// return cacheTxs
	return nil
}

// SetMainResult set the result for a tx
func (r *GlobalCache) SetMainResult(roundID uint64, hash common.Hash, receipt *types.Receipt, rwRecord *RWRecord) bool {

	if receipt == nil || rwRecord == nil {
		log.Debug("[PreplayCache] Nil Error", "txHash", hash)
		return false
	}

	txPreplay := r.GetTxPreplay(hash)
	if txPreplay == nil {
		log.Debug("[PreplayCache] SetMainResult Error", "txHash", hash)
		return false
	}

	txPreplay.Mu.Lock()
	defer txPreplay.Mu.Unlock()

	round, _ := txPreplay.GetRound(roundID)
	if round == nil {
		txPreplay.CreateRound(roundID)
		round, _ = txPreplay.GetRound(roundID)
	}

	nowTime := time.Now()
	round.Timestamp = uint64(nowTime.Unix())
	round.TimestampNano = uint64(nowTime.UnixNano())

	round.Filled = -1
	roundKeys := txPreplay.PreplayResults.Rounds.Keys()

	// iterate roundKeys reversely
	for i:= len(roundKeys)-1; i>=0; i--{
		rawKey := roundKeys[i].(uint64)
		// do not need update the priority in LRU probably
		rawRoundB, _ := txPreplay.PreplayResults.Rounds.Peek(rawKey)
		roundB := rawRoundB.(*PreplayResult)
		if rwRecord.Equal(roundB.RWrecord) {
			if roundB.Filled == -1 {
				round.Filled = int64(roundB.RoundID)
			} else {
				round.Filled = roundB.Filled
			}
			round.RWrecord = roundB.RWrecord
			round.Receipt = roundB.Receipt
			break
		}
	}
	if round.Filled == -1 {
		round.RWrecord = rwRecord
		round.Receipt = receipt
		rwRecord.Round = round
		txPreplay.PreplayResults.RWrecords.Add(roundID, rwRecord)
	}
	return true
}

// SetExtraResult set the result for extra part of preplay
func (r *GlobalCache) SetExtraResult(roundID uint64, hash common.Hash, currentState *CurrentState, extra *ExtraResult) bool {

	if currentState == nil || extra == nil {
		return false
	}

	txPreplay := r.GetTxPreplay(hash)
	if txPreplay == nil {
		log.Debug("[PreplayCache] SetMainResult Error", "txHash", hash)
		return false
	}

	txPreplay.Mu.Lock()
	defer txPreplay.Mu.Unlock()

	round, _ := txPreplay.GetRound(roundID)
	if round == nil {
		txPreplay.CreateRound(roundID)
		round, _ = txPreplay.GetRound(roundID)
	}

	nowTime := time.Now()
	round.CurrentState = currentState
	round.ExtraResult = extra
	round.ExtraResult.Timestamp = uint64(nowTime.Unix())
	round.ExtraResult.TimestampNano = uint64(nowTime.UnixNano())

	return true
}

// CommitTxRreplay update after preplay
func (r *GlobalCache) CommitTxRreplay(txPreplay *TxPreplay) {
	if txPreplay == nil {
		return
	}

	r.PreplayCache.Add(txPreplay.TxHash, txPreplay)
}

// CommitTxResult update after preplay
func (r *GlobalCache) CommitTxResult(roundID uint64, currentState *CurrentState, txResult map[common.Hash]*ExtraResult) {

	log.Debug(
		"Preplay update",
		"preplay", currentState.PreplayName,
		"len", len(txResult),
		"tot", len(r.PreplayCache.Keys()))

	if len(txResult) == 0 {
		return
	}

	for txHash, res := range txResult {
		r.SetExtraResult(roundID, txHash, currentState, res)
	}
}

// NewRoundID return the New ID of round
func (r *GlobalCache) NewRoundID() uint64 {
	r.PreplayRoundIDMu.Lock()
	defer r.PreplayRoundIDMu.Unlock()
	r.PreplayRoundID++
	return r.PreplayRoundID
}

// NewTimeStamp return New timestamp for preplay
func (r *GlobalCache) NewTimeStamp() int64 {
	r.TimestampField++
	if r.TimestampField > 2 {
		r.TimestampField = -2
	}
	return r.PreplayTimestamp + r.TimestampField
}

// func (r *GlobalCache) GetPreplayTxCnt(block *types.Block) (uint64, uint64, uint64) {

// 	// Super RLock
// 	r.ResultMu.RLock()
// 	defer func() {
// 		r.ResultMu.RUnlock()
// 	}()

// 	willTxCnt := uint64(0)
// 	inTxCnt := uint64(0)
// 	willNotTxCnt := uint64(0)

// 	for _, tx := range block.Body().Transactions {

// 		txHash := tx.Hash()

// 		preplayResult := r.GetTxPreplay(txHash)
// 		if preplayResult != nil {
// 			if preplayResult.FlagStatus == 0 {
// 				willTxCnt++
// 			}

// 			if preplayResult.FlagStatus == 1 {
// 				inTxCnt++
// 			}

// 			if preplayResult.FlagStatus == 2 {
// 				willNotTxCnt++
// 			}
// 		}
// 	}

// 	return willTxCnt, inTxCnt, willNotTxCnt
// }
