// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

package cache

import (
	"bytes"
	"container/heap"
	"encoding/gob"
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	lru "github.com/hashicorp/golang-lru"
	"math"
	"math/big"
	"sort"
	"sync"
	"time"
	"unsafe"
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
	Timestamp      time.Time

	// Setting Info
	FlagStatus bool // Flag: 0: not in, 1: already in
}

// NewTxPreplay create new RWRecord
func NewTxPreplay(tx *types.Transaction) *TxPreplay {
	rounds, _ := lru.New(roundLimit)
	//RWrecords, _ := lru.New(roundLimit)

	preplayResults := &PreplayResults{
		Rounds: rounds,
		//ReadDepTree:          cmptypes.NewPreplayResTrie(),
		MixTree:            cmptypes.NewPreplayResTrie(),
		MixTreeMu:          cmptypes.NewSimpleTryLock(), // trylock.New(),
		DeltaTree:          cmptypes.NewPreplayResTrie(),
		DeltaTreeMu:        cmptypes.NewSimpleTryLock(), // trylock.New(),
		TraceTrieMu:        cmptypes.NewSimpleTryLock(), // trylock.New(),
		RWRecordTrie:       cmptypes.NewPreplayResTrie(),
		RWRecordTrieMu:     cmptypes.NewSimpleTryLock(), // trylock.New(),
		wobjectHolderMap:   make(state.ObjectHolderMap),
		wobjectHolderMapMu: cmptypes.NewSimpleTryLock(), //trylock.New(),
		//objectPointerToObjID: make(map[uintptr]uintptr),
		//RWrecords:            RWrecords,
	}

	txPreplay := &TxPreplay{
		TxHash:    tx.Hash(),
		GasPrice:  tx.GasPrice(),
		GasLimit:  tx.Gas(),
		Tx:        tx,
		Timestamp: time.Now(),
	}

	txPreplay.PreplayResults, preplayResults.txPreplay = preplayResults, txPreplay

	return txPreplay
}

func IsExternalTransfer(readDetail *cmptypes.ReadDetail, tx *types.Transaction) bool {
	if tx.To() == nil || len(readDetail.ReadDetailSeq) > 5 || len(readDetail.ReadAddressAndBlockSeq) > 2 {
		// this check is necessary.
		return false
	}

	for _, alv := range readDetail.ReadDetailSeq {
		if alv.AddLoc.Field == cmptypes.CodeHash {
			if alv.AddLoc.Address != *tx.To() {
				return false
			}
			value := alv.Value.(common.Hash)
			if value != cmptypes.NilCodeHash && value != cmptypes.EmptyCodeHash {
				return false
			}
		}
	}
	return true

}

//func IsExternalTransfer(seqRecord []*cmptypes.AddrLocValue, tx *types.Transaction) bool {
//
//	if tx.To() == nil {
//		// this check is necessary.
//		return false
//	}
//	// assert seqRecord len
//	switch len(seqRecord) {
//	case 0:
//		fallthrough
//	case 1:
//		fallthrough
//	case 2:
//		ss, _ := json.Marshal(seqRecord)
//		log.Error("zhongxin: unexpected len", "tx", tx.Hash().Hex(), "seq", string(ss))
//		return false
//	case 3:
//		// self transfer
//		return true
//	case 4: // 'to' is a new address
//		return true
//	case 5: //assert 'to' is a common user
//		if seqRecord[4].AddLoc.Field != cmptypes.CodeHash {
//			return false
//		} else {
//			value := seqRecord[4].Value.(common.Hash)
//			if value != cmptypes.NilCodeHash && value != cmptypes.EmptyCodeHash {
//				return false
//			}
//		}
//		return true
//	default:
//		return false
//	}
//
//}

func (t *TxPreplay) SetExternalTransferInfo(record *RWRecord, tx *types.Transaction, setDelta bool) {
	t.PreplayResults.IsExternalTransfer = IsExternalTransfer(record.ReadDetail, tx)
	if setDelta && t.PreplayResults.IsExternalTransfer {
		wDeltas := make(map[common.Address]*WStateDelta)
		sender := record.ReadDetail.ReadAddressAndBlockSeq[0].AddLoc.Address
		to := *tx.To()

		if senderWB := record.WState[sender].Balance; senderWB != nil {
			senderBalanceDelta := new(big.Int).Sub(senderWB, record.RState[sender].Balance)
			wDeltas[sender] = &WStateDelta{senderBalanceDelta}
		}

		if to != sender {
			if _, ok := record.WState[to]; ok {
				toBalanceDelta := tx.Value()
				wDeltas[to] = &WStateDelta{toBalanceDelta}
			}
		}
		t.PreplayResults.DeltaWrites = wDeltas

	}
}

//func (t *TxPreplay) StoreWObjects(objMap state.ObjectMap, roundId uint64) WObjectWeakRefMap {
//	result := make(WObjectWeakRefMap)
//	for addr, obj := range objMap {
//		cmptypes.MyAssert(obj != nil)
//
//		t.PreplayResults.wobjectHolderMapMu.Lock()
//
//		id := t.PreplayResults.GetOrNewObjectIDNoLock((uintptr)((unsafe.Pointer)(obj)))
//		ref := NewWObjectWeakReference(t.TxHash, addr, t.Timestamp, id, roundId)
//		if _, hasHolder := t.PreplayResults.GetHolderNoLock(ref); !hasHolder {
//			holder := state.NewObjectHolder(obj, id)
//			t.PreplayResults.SetHolderNoLock(ref, holder)
//			result[addr] = ref
//		}
//
//		t.PreplayResults.wobjectHolderMapMu.Unlock()
//	}
//	return result
//}

func (t *TxPreplay) RLockRound() {
	t.PreplayResults.RoundsMu.RLock()
}

func (t *TxPreplay) RUnlockRound() {
	t.PreplayResults.RoundsMu.RUnlock()
}

func (t *TxPreplay) LockRound() {
	t.PreplayResults.RoundsMu.Lock()
}

func (t *TxPreplay) UnlockRound() {
	t.PreplayResults.RoundsMu.Unlock()
}

// CreateOrGetRound get round by roundID, if not exists, create new round for txPreplay.
func (t *TxPreplay) CreateOrGetRound(roundID uint64) (*PreplayResult, bool) {
	round, ok := t.PreplayResults.Rounds.Get(roundID)
	if ok {
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

// PeekRound peek round by roundID.
func (t *TxPreplay) PeekRound(roundID uint64) (*PreplayResult, bool) {
	rawRound, ok := t.PreplayResults.Rounds.Peek(roundID)
	if !ok {
		return nil, false
	}
	return rawRound.(*PreplayResult), true
}

// KeysOfRound get a slice of the roundID, from oldest to newest.
func (t *TxPreplay) KeysOfRound() []uint64 {
	var keys []uint64
	roundKeys := t.PreplayResults.Rounds.Keys()
	for _, raw := range roundKeys {
		keys = append(keys, raw.(uint64))
	}
	return keys
}

func (t *TxPreplay) GetWObjectHolder(wref *WObjectWeakReference) *state.ObjectHolder {
	if t.Timestamp == wref.Timestamp {
		if holder, hok := t.PreplayResults.GetHolder(wref); hok {
			return holder
		}
	}
	return nil
}

// PreplayResults record results of several rounds
type PreplayResults struct {
	Rounds   *lru.Cache `json:"-"`
	RoundsMu sync.RWMutex

	// deprecated
	ReadDepTree               *cmptypes.PreplayResTrie `json:"-"`
	MixTree                   *cmptypes.PreplayResTrie
	IsTraceBaseMixTree        bool
	MixTreeMu                 *cmptypes.SimpleTryLock //trylock.TryLocker
	TraceTrie                 ITracerTrie
	TraceTrieMu               *cmptypes.SimpleTryLock //trylock.TryLocker
	ForcedTraceTrieGeneration bool
	DeltaTree                 *cmptypes.PreplayResTrie
	DeltaTreeMu               *cmptypes.SimpleTryLock //trylock.TryLocker
	RWRecordTrie              *cmptypes.PreplayResTrie
	IsTraceBasedRWTrie        bool
	RWRecordTrieMu            *cmptypes.SimpleTryLock

	IsExternalTransfer bool
	DeltaWrites        map[common.Address]*WStateDelta

	wobjectHolderMap   state.ObjectHolderMap
	wobjectHolderMapMu *cmptypes.SimpleTryLock //trylock.TryLocker
	wobjectIDCounter   uintptr
	//objectPointerToObjID map[uintptr]uintptr

	// deprecated
	RWrecords *lru.Cache `json:"-"`
	// deprecated
	ReadDeps *lru.Cache `json:"-"`

	txPreplay *TxPreplay
}

//func (rs *PreplayResults) GetOrNewObjectID(objPointerAsInt uintptr) uintptr {
//	rs.wobjectHolderMapMu.Lock()
//	defer rs.wobjectHolderMapMu.Unlock()
//	if objID, ok := rs.objectPointerToObjID[objPointerAsInt]; ok {
//		return objID
//	}
//	objID := rs.wobjectIDCounter
//	rs.objectPointerToObjID[objPointerAsInt] = objID
//	rs.wobjectIDCounter++
//	return objID
//}

func (rs *PreplayResults) GetMixTreeActiveRounds() []cmptypes.IRound {
	rs.MixTreeMu.Lock()
	defer rs.MixTreeMu.Unlock()
	mixTree := rs.MixTree
	if mixTree != nil {
		return mixTree.GetActiveIRounds()
	}
	return nil
}

func (rs *PreplayResults) GetTraceTrieActiveRounds() []*PreplayResult {
	rs.TraceTrieMu.Lock()
	defer rs.TraceTrieMu.Unlock()
	traceTrie := rs.TraceTrie
	if traceTrie != nil {
		return traceTrie.GetActiveRounds()
	}
	return nil
}

func (rs *PreplayResults) TryGetMixTreeNodeCount() int64 {
	if rs.MixTreeMu.TryLock() {
		defer rs.MixTreeMu.Unlock()
		mixTree := rs.MixTree
		if mixTree != nil {
			return mixTree.GetNodeCount()
		}
	}
	return 0
}

func (rs *PreplayResults) TryGetTraceTrieNodeCount() int64 {
	if rs.TraceTrieMu.TryLock() {
		defer rs.TraceTrieMu.Unlock()
		traceTrie := rs.TraceTrie
		if traceTrie != nil {
			return traceTrie.GetNodeCount()
		}
	}
	return 0
}

func (rs *PreplayResults) TryGetTraceActivePathCount() int64 {
	if rs.TraceTrieMu.TryLock() {
		defer rs.TraceTrieMu.Unlock()
		traceTrie := rs.TraceTrie
		if traceTrie != nil {
			return traceTrie.GetActivePathCount()
		}
	}
	return 0
}

func (rs *PreplayResults) TryGetMixTreeActiveRWRecordCount() int64 {
	if rs.MixTreeMu.TryLock() {
		defer rs.MixTreeMu.Unlock()
		mixTree := rs.MixTree
		if mixTree != nil {
			rounds := mixTree.GetActiveIRounds()
			rwRecordIds := make(map[uintptr]struct{})
			for _, r := range rounds {
				rwRecordIds[r.GetRWRecordId()] = struct{}{}
			}
			//if len(rounds) > 10 && len(rwRecordIds) != len(rounds) {
			//	log.Warn(fmt.Sprintf("tx %v len rwRecordIDs %v len rounds %v", rs.txPreplay.TxHash.String(), len(rwRecordIds) ,len(rounds)))
			//}
			return int64(len(rwRecordIds))
		}
	}

	return 0
}

func (rs *PreplayResults) TryGetRWRecordTrieNodeCount() int64 {
	if rs.RWRecordTrieMu.TryLock() {
		defer rs.RWRecordTrieMu.Unlock()
		trie := rs.RWRecordTrie
		if trie != nil {
			return trie.GetNodeCount()
		}
	}
	return 0
}

func (rs *PreplayResults) TryGetDeltaTrieNodeCount() int64 {
	if rs.DeltaTreeMu.TryLock() {
		defer rs.DeltaTreeMu.Unlock()
		trie := rs.DeltaTree
		if trie != nil {
			return trie.GetNodeCount()
		}
	}
	return 0
}

func (rs *PreplayResults) NewObjectIDNoLock() uintptr {
	objID := rs.wobjectIDCounter
	rs.wobjectIDCounter++
	return objID
}

//func (rs *PreplayResults) GetOrNewObjectIDNoLock(objPointerAsInt uintptr) uintptr {
//	if objID, ok := rs.objectPointerToObjID[objPointerAsInt]; ok {
//		panic("same wobject stored twice!")
//		return objID
//	}
//	objID := rs.wobjectIDCounter
//	rs.objectPointerToObjID[objPointerAsInt] = objID
//	rs.wobjectIDCounter++
//	return objID
//}

func (rs *PreplayResults) GetAndDeleteHolder(wref *WObjectWeakReference) (*state.ObjectHolder, bool) {
	if rs.wobjectHolderMapMu.TryLock() {
		//holder, hok := rs.wobjectHolderMap.GetAndDeleteNoLock(wref.ObjectID)
		//if hok {
		//	objPointerAsInt := uintptr(unsafe.Pointer(holder.Obj))
		//	if _, ok := rs.objectPointerToObjID[objPointerAsInt]; !ok {
		//		panic("unsynced wobjectHolderMap and objectPointerToObjID")
		//	}
		//	delete(rs.objectPointerToObjID, objPointerAsInt)
		//}
		holder, hok := rs.GetAndDeleteHolderNoLock(wref.ObjectID)
		rs.wobjectHolderMapMu.Unlock()
		return holder, hok
	} else {
		return nil, false
	}
}

func (rs *PreplayResults) GetAndDeleteHolderNoLock(ObjectID uintptr) (*state.ObjectHolder, bool) {

	holder, hok := rs.wobjectHolderMap.GetAndDeleteNoLock(ObjectID)
	//if hok {
	//	objPointerAsInt := uintptr(unsafe.Pointer(holder.Obj))
	//	if objID, ok := rs.objectPointerToObjID[objPointerAsInt]; !ok {
	//		panic("unsynced wobjectHolderMap and objectPointerToObjID")
	//	}else{
	//		if objID != ObjectID {
	//			panic("objID mismatch with ObjectID")
	//		}
	//	}
	//
	//	delete(rs.objectPointerToObjID, objPointerAsInt)
	//}
	return holder, hok
}

func (rs *PreplayResults) GetHolder(wref *WObjectWeakReference) (*state.ObjectHolder, bool) {
	rs.wobjectHolderMapMu.Lock()
	defer rs.wobjectHolderMapMu.Unlock()
	return rs.GetHolderNoLock(wref)
}

func (rs *PreplayResults) GetHolderNoLock(wref *WObjectWeakReference) (*state.ObjectHolder, bool) {
	h, ok := rs.wobjectHolderMap[wref.ObjectID]
	if ok && h.ObjID != wref.ObjectID {
		cmptypes.MyAssert(false)
	}
	return h, ok
}

func (rs *PreplayResults) SetHolderNoLock(wref *WObjectWeakReference, holder *state.ObjectHolder) {
	_, ok := rs.wobjectHolderMap[wref.ObjectID]
	cmptypes.MyAssert(!ok)
	rs.wobjectHolderMap[wref.ObjectID] = holder
}

func (rs *PreplayResults) GCWObjects() {
	activeRounds := make(map[*PreplayResult]bool)
	for _, iRound := range rs.GetMixTreeActiveRounds() {
		round := iRound.(*PreplayResult)
		activeRounds[round] = true
	}
	for _, round := range rs.GetTraceTrieActiveRounds() {
		activeRounds[round] = true
	}

	//if rs.MixTree != nil {
	//	for _, iRound := range rs.MixTree.GetActiveIRounds() {
	//		round := iRound.(*PreplayResult)
	//		activeRounds[round] = true
	//	}
	//}
	//if rs.TraceTrie != nil {
	//	for _, round := range rs.TraceTrie.GetActiveRounds() {
	//		activeRounds[round] = true
	//	}
	//}

	activeHolderIDs := make(map[uintptr]bool)
	for round, _ := range activeRounds {
		for _, wref := range round.WObjectWeakRefs {
			activeHolderIDs[wref.ObjectID] = true
		}
	}

	rs.wobjectHolderMapMu.Lock()
	defer rs.wobjectHolderMapMu.Unlock()
	for objID, _ := range rs.wobjectHolderMap {
		if !activeHolderIDs[objID] {
			//holder := rs.wobjectHolderMap[objID]
			//delete(rs.wobjectHolderMap, objID)
			//objPointerAsInt := uintptr(unsafe.Pointer(holder.Obj))
			//cmptypes.MyAssert(holder.ObjID == objID)
			//_, ok := rs.objectPointerToObjID[objPointerAsInt]
			//cmptypes.MyAssert(ok)
			//delete(rs.objectPointerToObjID, objPointerAsInt)
			rs.GetAndDeleteHolderNoLock(objID)
		}
	}
}

func (rs *PreplayResults) GetWObjectSize() (objectCount int64, storageItemCount int64) {
	rs.wobjectHolderMapMu.Lock()
	defer rs.wobjectHolderMapMu.Unlock()
	for _, holder := range rs.wobjectHolderMap {
		objectCount++
		storageItemCount += int64(len(holder.Obj.GetOriginStorage()))
	}
	return
}

func (rs *PreplayResults) StoreUniqueWObjects(objMap state.ObjectMap, roundId uint64) WObjectWeakRefMap {
	result := make(WObjectWeakRefMap)
	for addr, obj := range objMap {
		cmptypes.MyAssert(obj != nil)

		rs.wobjectHolderMapMu.Lock()

		id := rs.NewObjectIDNoLock()
		ref := NewWObjectWeakReference(rs.txPreplay.TxHash, addr, rs.txPreplay.Timestamp, id, roundId)
		holder := state.NewObjectHolder(obj, id)
		rs.SetHolderNoLock(ref, holder)
		result[addr] = ref

		rs.wobjectHolderMapMu.Unlock()
	}
	return result
}

type ITracerTrie interface {
	GetNodeCount() int64
	GetActiveRounds() []*PreplayResult
	GetActivePathCount() int64
}

// PreplayResult record one round result
type PreplayResult struct {
	// Basic Info
	TxHash   common.Hash       `json:"txHash"`
	RoundID  uint64            `json:"roundId"` // RoundID Info
	GasPrice *big.Int          `json:"gasPrice"`
	TxResID  *cmptypes.TxResID `json:"-"`

	// Main Result
	Receipt  *types.Receipt `json:"receipt"`
	RWrecord *RWRecord      `json:"rwrecord"`
	//WObjects       state.ObjectMap     `json:"wobjects"`
	WObjectWeakRefs WObjectWeakRefMap   `json:"wobjects"`
	WObjectCopy     uint64              `json:"wobjectCopy"`
	WObjectNotCopy  uint64              `json:"wobjectNotCopy"`
	AccountChanges  cmptypes.TxResIDMap `json:"changes"`   // the written address changedby
	Timestamp       uint64              `json:"timestamp"` // Generation Time
	TimestampNano   uint64              `json:"timestampNano"`

	BasedBlockHash common.Hash              `json:"basedBlock"`
	ReadDepSeq     []*cmptypes.AddrLocValue `json:"deps"`
	// fastCheck info
	FormerTxs []common.Hash `json:"formerTxs"`

	// Extra Result
	CurrentState *CurrentState `json:"-"`
	ExtraResult  *ExtraResult  `json:"extraResult"`

	// FlagStatus: 0 will in, 1 in, 2 will not in
	FlagStatus uint64 `json:"flagStatus"`

	// Filled by the former round. -1 for no former round has the same RWRecord
	Filled int64

	Trace cmptypes.ISTrace `json:"-"`
}

func (r *PreplayResult) GetRoundId() uint64 {
	return r.RoundID
}

func (r *PreplayResult) GetRWRecordId() uintptr {
	return uintptr(unsafe.Pointer(r.RWrecord))
}

type WStateDelta struct {
	Balance *big.Int
}

// RWRecord for record
type RWRecord struct {
	RWHash common.Hash
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

// Deprecated
// record the read state in address level
type ReadDep struct {
	BasedBlockHash common.Hash
	IsNoDep        bool
	RoundID        uint64
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
func (rw *RWRecord) GetHash() common.Hash {
	if rw.Hashed {
		return rw.RWHash
	}
	rw.IterMu.Lock()
	defer rw.IterMu.Unlock()

	var res [][]byte

	if rw.RChain.Blockhash != nil {
		var blocknumbers []uint64
		for key, _ := range rw.RChain.Blockhash {
			blocknumbers = append(blocknumbers, key)
		}
		sort.Slice(blocknumbers, func(i, j int) bool {
			return blocknumbers[i] < blocknumbers[j]
		})
		for _, bn := range blocknumbers {
			res = append(res, rw.RChain.Blockhash[bn].Bytes())
		}
		//for _, blkHash := range rw.RChain.Blockhash {
		//	res = append(res, blkHash.Bytes())
		//}
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

	var addrByOrder []common.Address
	for addr := range rw.RState {
		addrByOrder = append(addrByOrder, addr)
	}
	sort.Slice(addrByOrder, func(i, j int) bool {
		return bytes.Compare(addrByOrder[i].Bytes(), addrByOrder[j].Bytes()) < 0
	})

	for _, addr := range addrByOrder {
		res = append(res, addr.Bytes())
		rstate := rw.RState[addr]
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
			var hashByOrder []common.Hash
			for key, _ := range rstate.Storage {
				hashByOrder = append(hashByOrder, key)
			}
			sort.Slice(hashByOrder, func(i, j int) bool {
				return bytes.Compare(hashByOrder[i].Bytes(), hashByOrder[j].Bytes()) < 0
			})
			for _, hashkey := range hashByOrder {
				res = append(res, hashkey.Bytes())
				res = append(res, rstate.Storage[hashkey].Bytes())
			}
			//for _, v := range rstate.Storage {
			//	res = append(res, v.Bytes())
			//}
		}
		if rstate.CommittedStorage != nil {
			var hashByOrder []common.Hash
			for key, _ := range rstate.CommittedStorage {
				hashByOrder = append(hashByOrder, key)
			}
			sort.Slice(hashByOrder, func(i, j int) bool {
				return bytes.Compare(hashByOrder[i].Bytes(), hashByOrder[j].Bytes()) < 0
			})
			for _, hashkey := range hashByOrder {
				res = append(res, hashkey.Bytes())
				res = append(res, rstate.CommittedStorage[hashkey].Bytes())
			}
			//for _, v := range rstate.CommittedStorage {
			//	res = append(res, v.Bytes())
			//}
		}
	}

	//sort.Slice(res, func(i, j int) bool {
	//	return bytes.Compare(res[i], res[j]) == -1
	//})
	rw.RWHash = crypto.Keccak256Hash(res...)
	rw.Hashed = true

	return rw.RWHash
}

// Deprecated
func (rw *RWRecord) GetRChainHash() string {
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
	sort.Slice(res, func(i, j int) bool {
		return bytes.Compare(res[i], res[j]) == -1
	})

	return crypto.Keccak256Hash(res...).String()
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
	TotalDifficulty   string             `json:"totalDifficulty"`
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

type TxFromCheapest struct {
	types.TxByPrice
}

func (s TxFromCheapest) Less(i, j int) bool {
	return s.Txns[i].CmpGasPriceWithTxn(s.Txns[j]) < 0
}

type TxPreplayMap struct {
	Size int

	txnMap      map[common.Hash]*TxPreplay
	removed     map[common.Hash]struct{}
	sortByPrice TxFromCheapest
	lock        sync.RWMutex
}

func NewTxPreplayMap(pSize int) *TxPreplayMap {
	return &TxPreplayMap{
		Size:        pSize,
		txnMap:      make(map[common.Hash]*TxPreplay),
		removed:     make(map[common.Hash]struct{}),
		sortByPrice: TxFromCheapest{},
	}
}

func (m *TxPreplayMap) Get(txn common.Hash) *TxPreplay {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.txnMap[txn]
}

func (m *TxPreplayMap) GetCheapest() *TxPreplay {
	m.lock.RLock()
	defer m.lock.RUnlock()

	for {
		if m.sortByPrice.Len() == 0 {
			return nil
		}
		cheapest := m.sortByPrice.Txns[0].Hash()
		if _, ok := m.removed[cheapest]; ok {
			heap.Pop(&m.sortByPrice)
		} else {
			return m.txnMap[cheapest]
		}
	}
}

func (m *TxPreplayMap) Len() int {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return len(m.txnMap)
}

func (m *TxPreplayMap) Commit(txPreplay *TxPreplay) {
	if txPreplay == nil {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	m.txnMap[txPreplay.TxHash] = txPreplay
	heap.Push(&m.sortByPrice, txPreplay.Tx)
}

func (m *TxPreplayMap) Remove(txn common.Hash) {
	m.lock.Lock()
	defer m.lock.Unlock()

	originSize := len(m.txnMap)
	delete(m.txnMap, txn)
	if len(m.txnMap) < originSize {
		m.removed[txn] = struct{}{}
	}
}

func (m *TxPreplayMap) RemoveCheapest(remove int) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for i := 0; i < remove; {
		if m.sortByPrice.Len() == 0 {
			return
		}
		txn := heap.Pop(&m.sortByPrice).(*types.Transaction)
		if _, ok := m.removed[txn.Hash()]; ok {
			delete(m.removed, txn.Hash())
		} else {
			i++
			delete(m.txnMap, txn.Hash())
		}
	}
}

// GetTxPreplay returns the result of preplay and updates the "recently used"-ness of the key
func (r *GlobalCache) GetTxPreplay(txHash interface{}) *TxPreplay {
	r.PauseForProcess()

	r.PreplayCacheTryLock.Lock()
	defer r.PreplayCacheTryLock.Unlock()

	result, response := r.PreplayCache.Get(txHash)

	if !response {
		return nil
	}

	if tx, ok := result.(*TxPreplay); ok {
		return tx
	}

	return nil
}

func (r *GlobalCache) doPeekTxPreplay(txHash  interface{}) *TxPreplay {
	r.PreplayCacheTryLock.Lock()
	defer r.PreplayCacheTryLock.Unlock()
	result, response := r.PreplayCache.Peek(txHash)

	if !response {
		return nil
	}

	if tx, ok := result.(*TxPreplay); ok {
		return tx
	}

	return nil
}

// PeekTxPreplay returns the result of preplay and will not update the "recently used"-ness of the key
func (r *GlobalCache) PeekTxPreplayInNonProcess(txHash interface{}) *TxPreplay {
	r.PauseForProcess()
	return r.doPeekTxPreplay(txHash)
}

func (r *GlobalCache) PeekTxPreplayInProcessForDebug(txHash interface{}) *TxPreplay {
   return r.doPeekTxPreplay(txHash)
}

func (r *GlobalCache) KeysOfTxPreplay() []interface{} {
	r.PauseForProcess()
	r.PreplayCacheTryLock.Lock()
	defer r.PreplayCacheTryLock.Unlock()
	return r.PreplayCache.Keys()
}

func (r *GlobalCache) LenOfTxPreplay() int {
	r.PauseForProcess()
	r.PreplayCacheTryLock.Lock()
	defer r.PreplayCacheTryLock.Unlock()
	return r.PreplayCache.Len()
}

func (r *GlobalCache) AddReduplicatedNonceTxn(txnMap map[common.Address]types.Transactions) {
	r.reduplicatedNonceTxnMu.Lock()
	defer r.reduplicatedNonceTxnMu.Unlock()

	for sender, txns := range txnMap {
		if dupTxns, ok := r.reduplicatedNonceTxn[sender]; ok && len(dupTxns) > 0 {
			var addedTxn types.Transactions
		Loop:
			for _, txn := range txns {
				for _, cache := range dupTxns {
					if cache.Nonce() > txn.Nonce() {
						break
					}
					if cache.Nonce() == txn.Nonce() && cache.Hash() == txn.Hash() {
						continue Loop
					}
				}
				addedTxn = append(addedTxn, txn)
			}
			r.reduplicatedNonceTxn[sender] = append(dupTxns, addedTxn...)
		} else {
			r.reduplicatedNonceTxn[sender] = types.TxByNonce(txns)
		}
		sort.Sort(r.reduplicatedNonceTxn[sender])
	}
}

func (r *GlobalCache) DropReduplicatedNonceTxn(dropOld func(addr common.Address, txns types.TxByNonce) types.TxByNonce) {
	r.reduplicatedNonceTxnMu.Lock()
	defer r.reduplicatedNonceTxnMu.Unlock()

	for addr, txns := range r.reduplicatedNonceTxn {
		newTxns := dropOld(addr, txns)
		if len(newTxns) == 0 {
			delete(r.reduplicatedNonceTxn, addr)
		} else {
			if len(txns) != len(newTxns) {
				r.reduplicatedNonceTxn[addr] = newTxns
			}
		}
	}
}

func (r *GlobalCache) SearchReduplicatedNonceTxn(sender common.Address, minNonce, maxNonce uint64) (types.Transactions, bool) {
	r.reduplicatedNonceTxnMu.RLock()
	defer r.reduplicatedNonceTxnMu.RUnlock()

	if txns, ok := r.reduplicatedNonceTxn[sender]; ok {
		minIndex := sort.Search(len(txns), func(i int) bool {
			return txns[i].Nonce() >= minNonce
		})
		maxIndex := sort.Search(len(txns), func(i int) bool {
			return txns[i].Nonce() > maxNonce
		})
		if maxIndex > minIndex {
			retTxns := txns[minIndex:maxIndex]
			txnsCpy := make(types.Transactions, len(retTxns))
			copy(txnsCpy, retTxns)
			return txnsCpy, true
		}
	}
	return nil, false
}

func (r *GlobalCache) PrintReduplicatedNonceTxn() {
	r.reduplicatedNonceTxnMu.RLock()
	defer r.reduplicatedNonceTxnMu.RUnlock()

	var addrList []common.Address
	for addr := range r.reduplicatedNonceTxn {
		addrList = append(addrList, addr)
	}
	sort.Slice(addrList, func(i, j int) bool {
		return bytes.Compare(addrList[i].Bytes(), addrList[j].Bytes()) < 0
	})

	for _, addr := range addrList {
		if len(r.reduplicatedNonceTxn[addr]) > 0 {
			log.Info("In reduplicated nonce txn", "sender", addr.Hex(), "txns count", len(r.reduplicatedNonceTxn[addr]),
				"first", r.reduplicatedNonceTxn[addr][0].Hash().Hex())
		}
	}
}

func (r *GlobalCache) GetReduplicatedNonceTxnCount() int {
	r.reduplicatedNonceTxnMu.RLock()
	defer r.reduplicatedNonceTxnMu.RUnlock()

	var count int
	for _, txns := range r.reduplicatedNonceTxn {
		count += len(txns)
	}
	return count
}

// GetGasUsedResult return the cache of gas
func (r *GlobalCache) GetGasUsedCache(sender common.Address, txn *types.Transaction) uint64 {
	gasLimit := txn.Gas()

	if rawGasUsed, ok := r.PrimaryGasUsedCache.Get(txn.Hash()); ok {
		gasUsed := rawGasUsed.(uint64)
		if gasUsed <= gasLimit {
			return gasUsed
		} else {
			log.Error(fmt.Sprintf("Get a too large gas used of transaction %s", txn.Hash().Hex()))
		}
	}

	if txn.To() != nil {
		secondaryKey := getSecondaryKeyFromTxn(sender, txn)
		if rawGasUsed, ok := r.SecondaryGasUsedCache.Get(secondaryKey); ok {
			gasUsed := rawGasUsed.(uint64)
			if gasUsed <= gasLimit {
				return gasUsed
			}
		}

		tertiaryKey := getTertiaryKeyFromTxn(txn)
		if rawGasUsed, ok := r.TertiaryGasUsedCache.Get(tertiaryKey); ok {
			gasUsed := rawGasUsed.(uint64)
			if gasUsed <= gasLimit {
				return gasUsed
			}
		}
	}
	if gasLimit == params.TxGas {
		return gasLimit
	}
	return gasLimit/2 + 1
}

type SecondaryKey [2*common.AddressLength + 4]byte
type TertiaryKey [common.AddressLength + 4]byte

// only for transaction txn.To() != nil
func getSecondaryKeyFromTxn(sender common.Address, txn *types.Transaction) (key SecondaryKey) {
	copy(key[:], append(sender[:], (*txn.To())[:]...))
	if len(txn.Data()) >= 4 {
		copy(key[2*common.AddressLength:], txn.Data()[:4])
	}
	return
}

// only for transaction txn.To() != nil
func getTertiaryKeyFromTxn(txn *types.Transaction) (key TertiaryKey) {
	copy(key[:], (*txn.To())[:])
	if len(txn.Data()) >= 4 {
		copy(key[common.AddressLength:], txn.Data()[:4])
	}
	return
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
func (r *GlobalCache) SetMainResult(roundID uint64, receipt *types.Receipt, rwRecord *RWRecord, wobjects state.ObjectMap, wobjectCopy, wobjectNotCopy uint64,
	accChanges cmptypes.TxResIDMap, readDeps []*cmptypes.AddrLocValue, preBlockHash common.Hash, txPreplay *TxPreplay) (*PreplayResult, bool) {

	if receipt == nil || rwRecord == nil {
		log.Debug("[PreplayCache] Nil Error", "txHash", txPreplay.TxHash)
		return nil, false
	}

	txPreplay.LockRound()
	defer txPreplay.UnlockRound()

	round, isNewRound := txPreplay.CreateOrGetRound(roundID)

	cmptypes.MyAssert(isNewRound, "Should always be a new round here txHash %v roundID %v old RoundID %v rwrecord %v",
		txPreplay.TxHash.Hex(), roundID, round.RoundID, round.RWrecord == nil)

	round.RWrecord = rwRecord
	round.Receipt = receipt
	round.ReadDepSeq = readDeps
	round.AccountChanges = accChanges

	nowTime := time.Now()
	//round.WObjects = wobjects
	round.WObjectWeakRefs = txPreplay.PreplayResults.StoreUniqueWObjects(wobjects, roundID) //txPreplay.StoreWObjects(wobjects, roundID)
	round.WObjectCopy = wobjectCopy
	round.WObjectNotCopy = wobjectNotCopy
	round.Timestamp = uint64(nowTime.Unix())
	round.TimestampNano = uint64(nowTime.UnixNano())

	round.BasedBlockHash = preBlockHash

	if rwRecord.Round == nil {
		// this is a new RWRecord (reuseStatus is noHit)
		round.Filled = -1
		rwRecord.Round = round
		//txPreplay.PreplayResults.RWrecords.Add(roundID, rwRecord)
	} else {
		// this is a rwRecord got by trie/iter hit
		round.Filled = int64(rwRecord.Round.RoundID)
	}

	return round, true
}

// SetGasUsedResult set the gas used cache for a tx
func (r *GlobalCache) SetGasUsedCache(txn *types.Transaction, receipt *types.Receipt, sender common.Address) {
	gasUsed := receipt.GasUsed

	primaryKey := txn.Hash()
	r.PrimaryGasUsedCache.Add(primaryKey, gasUsed)

	if txn.To() != nil {
		secondaryKey := getSecondaryKeyFromTxn(sender, txn)
		r.SecondaryGasUsedCache.Add(secondaryKey, gasUsed)

		tertiaryKey := getTertiaryKeyFromTxn(txn)
		r.TertiaryGasUsedCache.Add(tertiaryKey, gasUsed)
	}
}

// SetExtraResult set the result for extra part of preplay
func (r *GlobalCache) SetExtraResult(roundID uint64, hash common.Hash, currentState *CurrentState, extra *ExtraResult) bool {

	if currentState == nil || extra == nil {
		return false
	}

	txPreplay := r.PeekTxPreplayInNonProcess(hash)
	if txPreplay == nil {
		log.Debug("[PreplayCache] SetMainResult Error", "txHash", hash)
		return false
	}

	txPreplay.LockRound()
	defer txPreplay.UnlockRound()

	round, _ := txPreplay.CreateOrGetRound(roundID)

	nowTime := time.Now()
	round.CurrentState = currentState
	round.ExtraResult = extra
	round.ExtraResult.Timestamp = uint64(nowTime.Unix())
	round.ExtraResult.TimestampNano = uint64(nowTime.UnixNano())

	return true
}

// Deprecated
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
		//readDep, ok := txPreplay.PreplayResults.ReadDepSeq.Get(preBlockHash)
		readDep := &ReadDep{
			BasedBlockHash: preBlockHash,
			IsNoDep:        isNoDep,
			RoundID:        roundID, // TODO might be the first round which has the same rw record
		}
		txPreplay.PreplayResults.ReadDeps.Add(preBlockHash, readDep)
	} else {
		// TODO: handle tx with depended txs ; key might be blockhash + txhash
		// key := preBlockHash + preTxHashes
		//readDep, ok := txPreplay.PreplayResults.ReadDepSeq.Get(key)
		//if !ok {
		//	readDep = &ReadDep{
		//		BasedBlockHash: preBlockHash,
		//		IsNoDep:        isNoDep,
		//	}
		//	txPreplay.PreplayResults.ReadDepSeq.Add(key, readDep)
		//} else {
		//	curReadDep := readDep.(*ReadDep)
		//	curReadDep.BasedBlockHash = preBlockHash
		//	curReadDep.IsNoDep = isNoDep
		//}

	}
	return true
}

// AddTxPreplay update after preplay
func (r *GlobalCache) AddTxPreplay(txPreplay *TxPreplay) {
	r.PauseForProcess()
	if txPreplay == nil {
		return
	}
	r.PreplayCacheTryLock.Lock()
	defer r.PreplayCacheTryLock.Unlock()
	r.PreplayCache.Add(txPreplay.TxHash, txPreplay)
}

func (r *GlobalCache) ResizeTxPreplay(size int) int {
	r.PauseForProcess()
	r.PreplayCacheTryLock.Lock()
	defer r.PreplayCacheTryLock.Unlock()
	return r.PreplayCache.Resize(size)
}

func (c *GlobalCache) RemoveOldest() (int64, bool) {
	c.PauseForProcess()
	c.PreplayCacheTryLock.Lock()
	key, value, ok := c.PreplayCache.GetOldest()
	c.PreplayCacheTryLock.Unlock()

	if ok {
		r := value.(*TxPreplay).PreplayResults
		var nodeCount int64
		if r.MixTree != nil {
			nodeCount += r.MixTree.GetNodeCount()
		}
		if r.TraceTrie != nil {
			nodeCount += r.TraceTrie.GetNodeCount()
		}
		if r.RWRecordTrie != nil {
			nodeCount += r.RWRecordTrie.GetNodeCount()
		}
		c.PreplayCacheTryLock.Lock()
		c.PreplayCache.Remove(key)
		c.PreplayCacheTryLock.Unlock()
		return nodeCount, true
	} else {
		return 0, false
	}
}

func (r *GlobalCache) RemoveTxPreplay(txn common.Hash) {
	r.PauseForProcess()
	r.PreplayCacheTryLock.Lock()
	defer r.PreplayCacheTryLock.Unlock()
	r.PreplayCache.Remove(txn)
}

// CommitTxResult update after preplay
func (r *GlobalCache) CommitTxResult(roundID uint64, currentState *CurrentState, rawTxs map[common.Address]types.Transactions,
	rawTxResult map[common.Hash]*ExtraResult) {

	//log.Debug(
	//	"Preplay update",
	//	"preplay", currentState.PreplayName,
	//	"len", len(rawTxResult),
	//	"tot", r.PreplayCache.Len())

	if len(rawTxResult) == 0 {
		return
	}

	txResult := make(map[common.Hash]*ExtraResult)
	for _, txs := range rawTxs {
		for _, tx := range txs {
			txHash := tx.Hash()
			if result, ok := rawTxResult[txHash]; ok {
				txResult[txHash] = result
			}
		}
	}

	for txHash, res := range txResult {
		r.SetExtraResult(roundID, txHash, currentState, res)
	}
}

// NewRoundID return the New ID of round
func (r *GlobalCache) NewRoundID() uint64 {
	r.PreplayRoundIDMu.Lock()
	defer r.PreplayRoundIDMu.Unlock()
	if r.PreplayRoundID == math.MaxUint64 {
		r.PreplayRoundID = 1
	} else {
		r.PreplayRoundID++
	}
	return r.PreplayRoundID
}

// GetPreplayTimeStamp return Timestamp for calculate dependency directly
func (r *GlobalCache) GetPreplayTimeStamp() uint64 {
	r.TimestampMu.RLock()
	defer r.TimestampMu.RUnlock()
	return r.PreplayTimestamp
}

// Deprecated: NewTimeStamp return New timestamp for preplay
func (r *GlobalCache) NewTimeStamp() int64 {
	r.TimestampMu.Lock()
	defer r.TimestampMu.Unlock()
	r.TimestampField++
	if r.TimestampField > 2 {
		r.TimestampField = -2
	}
	return int64(r.PreplayTimestamp) + r.TimestampField
}
