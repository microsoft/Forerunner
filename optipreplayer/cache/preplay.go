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
	"github.com/ethereum/go-ethereum/core/state"
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
	TxHash common.Hash `json:"txHash"`

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
	preplayResults.ReadDeps, _ = lru.New(3)

	preplayResults.RWRecordTrie = cmptypes.NewPreplayResTrie()
	return &TxPreplay{
		PreplayResults: preplayResults,
		TxHash:         tx.Hash(),
		GasPrice:       tx.GasPrice(),
		GasLimit:       tx.Gas(),
		Tx:             tx,
	}
}

// CreateRound create new round preplay for tx;
func (t *TxPreplay) CreateOrGetRound(roundID uint64) (*PreplayResult, bool) {
	round, ok := t.PreplayResults.Rounds.Get(roundID)
	if ok == true {
		return round.(*PreplayResult), false // false means this roundId already exists
	}

	roundNew := &PreplayResult{
		RoundID:  roundID,
		TxHash:   t.TxHash,
		GasPrice: t.GasPrice,
		Filled:   -1,
	}

	t.PreplayResults.Rounds.Add(roundID, roundNew)
	return roundNew, true // true means this roundId is just created
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
	Rounds       *lru.Cache `json:"-"`
	RWrecords    *lru.Cache `json:"-"`
	ReadDeps     *lru.Cache `json:"-"`
	RWRecordTrie *cmptypes.PreplayResTrie
}

// PreplayResult record one round result
type PreplayResult struct {
	// Basic Info
	TxHash   common.Hash `json:"txHash"`
	RoundID  uint64      `json:"roundId"` // RoundID Info
	GasPrice *big.Int    `json:"gasPrice"`

	// Main Result
	Receipt       *types.Receipt  `json:"receipt"`
	RWrecord      *RWRecord       `json:"rwrecord"`
	WObjects      state.ObjectMap `json:"wobjects"`
	Timestamp     uint64          `json:"timestamp"` // Generation Time
	TimestampNano uint64          `json:"timestampNano"`

	// fastCheck info
	BasedBlockHash common.Hash   `json:"basedBlock"`
	FormerTxs      []common.Hash `json:"formerTxs"`

	// Extra Result
	CurrentState *CurrentState `json:"-"`
	ExtraResult  *ExtraResult  `json:"extraResult"`

	// FlagStatus: 0 will in, 1 in, 2 will not in
	FlagStatus uint64 `json:"flagStatus"`

	// Filled by the former round. -1 for no former round has the same RWRecord
	Filled int64
}

// RWRecord for record
type RWRecord struct {
	RWHash string
	IterMu sync.Mutex
	// HashOrder [][]byte

	RState     map[common.Address]*state.ReadState
	ReadDetail *cmptypes.ReadDetail
	RChain     state.ReadChain
	WState     map[common.Address]*state.WriteState

	Failed bool
	Hashed bool

	Round *PreplayResult `json:"-"`
}

// id of each round result of each tx, TODO: encode this id into a simple type instead of a struct
type TxExecutionID struct {
	TxHash  common.Hash
	RoundID uint64
}

// record the read state in address level
type ReadDep struct {
	BasedBlockHash common.Hash
	IsNoDep        bool
	RoundID        uint64
	// Deprecated: useless currently;
	RAddress map[common.Address][]TxExecutionID
	// Deprecated: useless currently; can be got by roundID
	RWRecord *RWRecord
}

// NewRWRecord create new RWRecord
func NewRWRecord(
	rstate map[common.Address]*state.ReadState,
	rchain state.ReadChain,
	wstate map[common.Address]*state.WriteState,
	rdetail *cmptypes.ReadDetail,
	failed bool,
) *RWRecord {
	return &RWRecord{
		RState:     rstate,
		RChain:     rchain,
		WState:     wstate,
		ReadDetail: rdetail,
		Failed:     failed,
		Hashed:     false,
	}
}

// Equal judge equal
func (rw *RWRecord) Equal(rwi cmptypes.RecordHolder) bool {
	rwb, _ := rwi.(*RWRecord)
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

	if rw.RChain.Blockhash != nil {
		for _, blkHash := range rw.RChain.Blockhash {
			res = append(res, blkHash.Bytes())
		}
	}
	if rw.RChain.Coinbase != nil {
		res = append(res, rw.RChain.Coinbase.Bytes())
	}
	if rw.RChain.Timestamp != nil {
		res = append(res, rw.RChain.Timestamp.Bytes())
	}
	if rw.RChain.Number != nil {
		res = append(res, rw.RChain.Number.Bytes())
	}
	if rw.RChain.Difficulty != nil {
		res = append(res, rw.RChain.Difficulty.Bytes())
	}
	if rw.RChain.GasLimit != nil {
		res = append(res, GetBytes(*rw.RChain.GasLimit))
	}

	for _, rstate := range rw.RState {
		if rstate.Balance != nil {
			res = append(res, rstate.Balance.Bytes())
		}
		if rstate.Nonce != nil {
			res = append(res, GetBytes(*rstate.Nonce))
		}
		if rstate.CodeHash != nil {
			res = append(res, rstate.CodeHash.Bytes())
		}
		if rstate.Exist != nil {
			res = append(res, GetBytes(*rstate.Exist))
		}
		if rstate.Empty != nil {
			res = append(res, GetBytes(*rstate.Empty))
		}
		if rstate.Storage != nil {
			for _, v := range rstate.Storage {
				res = append(res, v.Bytes())
			}
		}
		if rstate.CommittedStorage != nil {
			for _, v := range rstate.CommittedStorage {
				res = append(res, v.Bytes())
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

func (rw *RWRecord) GetPreplayRes() interface{} {
	return rw.Round
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

// Deprecated:
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
func (r *GlobalCache) SetMainResult(roundID uint64, txHash common.Hash, receipt *types.Receipt, rwRecord *RWRecord,
	wobjects state.ObjectMap, preBlockHash common.Hash, txPreplay *TxPreplay) bool {

	if receipt == nil || rwRecord == nil {
		log.Debug("[PreplayCache] Nil Error", "txHash", txHash)
		return false
	}
	//
	//txPreplay.Mu.Lock()
	//defer txPreplay.Mu.Unlock()

	round, _ := txPreplay.CreateOrGetRound(roundID)

	// mark the round for every rwRecord, even this rwRecord would not be stored into LRU cache, but it would be used in Trie cache
	rwRecord.Round = round
	round.RWrecord = rwRecord
	round.Receipt = receipt

	nowTime := time.Now()
	round.WObjects = wobjects
	round.Timestamp = uint64(nowTime.Unix())
	round.TimestampNano = uint64(nowTime.UnixNano())

	round.BasedBlockHash = preBlockHash
	//formerTx := *(statedb.ProcessedTxs)
	//round.FormerTxs = formerTx[0 : len(formerTx)-1] // the last one is the current tx

	round.Filled = -1
	roundKeys := txPreplay.PreplayResults.Rounds.Keys()

	// iterate roundKeys reversely
	for i := len(roundKeys) - 1; i >= 0; i-- {
		rawKey := roundKeys[i].(uint64)
		if rawKey == roundID {
			continue
		}
		// do not need update the priority in LRU
		rawRoundB, _ := txPreplay.PreplayResults.Rounds.Peek(rawKey)
		roundB := rawRoundB.(*PreplayResult)
		if rwRecord.Equal(roundB.RWrecord) {
			// TODO : this round could be skipped
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

	round, _ := txPreplay.CreateOrGetRound(roundID)

	nowTime := time.Now()
	round.CurrentState = currentState
	round.ExtraResult = extra
	round.ExtraResult.Timestamp = uint64(nowTime.Unix())
	round.ExtraResult.TimestampNano = uint64(nowTime.UnixNano())

	return true
}

// SetReadDep set the read dep info for a tx in a given round
func (r *GlobalCache) SetReadDep(roundID uint64, txHash common.Hash, txPreplay *TxPreplay, rwRecord *RWRecord, preBlockHash common.Hash, isNoDep bool) bool {
	if txPreplay == nil {
		log.Debug("[PreplayCache] SetMainResult Error", "txHash", txHash)
		return false
	}
	if rwRecord == nil {
		return false
	}
	//txPreplay.Mu.Lock()
	//defer txPreplay.Mu.Unlock()

	if isNoDep {
		//readDep, ok := txPreplay.PreplayResults.ReadDeps.Get(preBlockHash)
		readDep := &ReadDep{
			BasedBlockHash: preBlockHash,
			IsNoDep:        isNoDep,
			RoundID:        roundID, // TODO might be the first round which has the same rw record
		}
		txPreplay.PreplayResults.ReadDeps.Add(preBlockHash, readDep)
	} else {
		// TODO: handle tx with depended txs ; key might be blockhash + txhash
		// key := preBlockHash + preTxHashes
		//readDep, ok := txPreplay.PreplayResults.ReadDeps.Get(key)
		//if !ok {
		//	readDep = &ReadDep{
		//		BasedBlockHash: preBlockHash,
		//		IsNoDep:        isNoDep,
		//	}
		//	txPreplay.PreplayResults.ReadDeps.Add(key, readDep)
		//} else {
		//	curReadDep := readDep.(*ReadDep)
		//	curReadDep.BasedBlockHash = preBlockHash
		//	curReadDep.IsNoDep = isNoDep
		//}

	}
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
