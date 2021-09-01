// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

// MSRA Computation Reuse Model

package cmptypes

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/optipreplayer/config"
	"math/big"
	"sync/atomic"
	"unsafe"
)

var EmptyCodeHash = crypto.Keccak256Hash(nil)
var NilCodeHash = common.Hash{}

type NodeTypeDiffError struct {
	CurNodeType *AddrLocation
	NewNodeType *AddrLocation
}

func (e *NodeTypeDiffError) Error() string {
	curjs, _ := json.Marshal(e.CurNodeType)
	newjs, _ := json.Marshal(e.NewNodeType)
	return fmt.Sprintf("NodeTypeDiffError: \n\tCurrentNodeType:%s;\n\tNewNodeType:%s", string(curjs), string(newjs))
}

type Field int

const (
	// Chain info
	Blockhash Field = iota + 1
	Coinbase
	Timestamp
	Number
	Difficulty
	GasLimit  // 6

	//PreBlockHash // used in the top of dep tree (in fact, there is a blocknumber layer on the top of PreBlockHash

	// State info
	Balance           //7
	Nonce             //8
	CodeHash          //9
	Exist             // 10
	Empty             // 11
	Code              // 12
	Storage           // 13
	CommittedStorage  //14

	// Dep info
	Dependence  //15

	// Write State
	DirtyStorage
	Suicided

	//DeltaInfo
	MinBalance

	// State info used by tracer
	CodeSize
	FieldTotalSize
)

func (f Field) String() string {
	switch f {
	case Blockhash:
		return "blockhash"
	case Coinbase:
		return "coinbase"
	case Timestamp:
		return "timestamp"
	case Number:
		return "number"
	case Difficulty:
		return "difficulty"
	case GasLimit:
		return "gasLimit"
	case Balance:
		return "balance"
	case Nonce:
		return "nonce"
	case CodeHash:
		return "codeHash"
	case Exist:
		return "exist"
	case Empty:
		return "empty"
	case Code:
		return "code"
	case Storage:
		return "storage"
	case CommittedStorage:
		return "committedStorage"
	case Dependence:
		return "dependence"
	case Suicided:
		return "suicided"
	case MinBalance:
		return "minBalance"
	case CodeSize:
		return "codeSize"
	}
	return ""
}

func IsChainField(field Field) bool {
	return field < Balance
}

type ReuseStatus struct {
	BaseStatus        ReuseBaseStatus
	HitType           HitType
	MissType          MissType
	MixStatus         *MixStatus
	TraceStatus       *TraceStatus
	MissNode          *PreplayResTrieNode
	MissValue         interface{} // to reduce the cost of converting interfaces, mute the miss Value
	AbortStage        AbortStage
	TraceTrieHitAddrs TxResIDMap
	TxHash            common.Hash
	BlockHash         common.Hash
	BlockNumber       *big.Int
	GasUsed           uint64
	TryPeekFailed     bool
	RoundCount        uint64 // has value when miss
}

type MixStatus struct {
	MixHitType         HitSubType
	DepHitAddr         []common.Address
	DepHitAddrMap      map[common.Address]interface{}
	DepUnmatchedInHead int // when partial hit, the count of dep unmatched addresses which are in the front of the first matched addr
	HitDepNodeCount    int
	UnhitDepNodeCount  int
	DetailCheckedCount int // detail(include chain field) count
	BasicDetailCount   int
	WriteDepCount      int
	WriteDetailCount   int
	WriteDetailTotal   int

	RoundCount    uint64
	HitRoundID    uint64
	HitRWRecordID uintptr
}

func (ms *MixStatus) GetPerfString() string {
	return fmt.Sprintf("fARm %v fPRm %v aRm %v aRFm %v fAWm %v fPWm %v aWm %v RnC %v",
		ms.DetailCheckedCount, ms.BasicDetailCount, ms.HitDepNodeCount+ms.UnhitDepNodeCount, ms.UnhitDepNodeCount,
		ms.WriteDetailCount, ms.WriteDetailTotal, ms.WriteDepCount, ms.RoundCount)
}

type TraceStatus struct {
	TraceHitType               HitSubType
	TotalNodes                 uint64
	ExecutedNodes              uint64
	ExecutedInputNodes         uint64
	ExecutedChainInputNodes    uint64
	ExecutedOutputNodes        uint64
	ExecutedLogNodes           uint64
	TotalOpNodes               uint64 // number of original EVM dynamic op
	AccountReadCount           uint64
	AccountReadUnknownCount    uint64
	ChainReadUnknownCount      uint64
	FieldReadUnknownCount      uint64
	FieldActualReadCount       uint64
	FieldPotentialReadCount    uint64
	AccountWrittenCount        uint64
	FieldActualWrittenCount    uint64
	FieldPotentialWrittenCount uint64

	TotalAccountLaneJumps       uint64
	TotalAccountLaneFailedJumps uint64
	TotalAccountLaneJumpKeys    uint64

	TotalFieldLaneAJumps       uint64
	TotalFieldLaneFJumps       uint64
	TotalFieldLaneFailedAJumps uint64
	TotalFieldLaneFailedFJumps uint64
	TotalFieldLaneAJumpKeys    uint64
	TotalFieldLaneFJumpKeys    uint64

	TotalOpLaneAJumps       uint64
	TotalOpLaneFailedAJumps uint64
	TotalOpLaneFJumps       uint64
	TotalOpLaneFailedFJumps uint64
	TotalOpLaneAJumpKeys    uint64
	TotalOpLaneFJumpKeys    uint64

	OpLaneSuccessfulJumps uint64

	AvgSpecializationDurationInNanoSeconds int64
	AvgMemoizationDurationInNanoSeconds    int64
	TotalTraceCount                        int64

	HitPathId     uintptr
	HitRWRecordId uintptr

	PathCount  int
	RoundCount int

	SearchResult ITraceTrieSearchResult
}

func (th *TraceStatus) GetPerfString() string {
	totalJumps := th.TotalAccountLaneJumps + th.TotalFieldLaneAJumps + th.TotalFieldLaneFJumps + th.TotalOpLaneAJumps + th.TotalOpLaneFJumps
	totalJumpKeys := th.TotalAccountLaneJumpKeys + th.TotalFieldLaneAJumpKeys + th.TotalFieldLaneFJumpKeys + th.TotalOpLaneAJumpKeys + th.TotalOpLaneFJumpKeys
	totalFailedJumps := th.TotalAccountLaneFailedJumps + th.TotalFieldLaneFailedAJumps + th.TotalFieldLaneFailedFJumps + th.TotalOpLaneFailedAJumps + th.TotalOpLaneFailedFJumps
	accountLaneSuccessfulJumps := th.TotalAccountLaneJumps - th.TotalAccountLaneFailedJumps
	fieldLaneSucessfulJumps := th.TotalFieldLaneAJumps + th.TotalFieldLaneFJumps - th.TotalFieldLaneFailedAJumps - th.TotalFieldLaneFailedFJumps
	return fmt.Sprintf("eN %v tN %v pN %v iN %v cN %v oN %v lN %v cRU %v fAR %v fPR %v fARU %v aR %v aRU %v fAW %v fPW %v aW %v faJ %v tJ %v tK %v aJ %v fiJ %v OJ %v sD %v mD %v tC %v PaC %v RnC %v",
		th.ExecutedNodes, th.TotalNodes, th.TotalOpNodes, th.ExecutedInputNodes, th.ExecutedChainInputNodes, th.ExecutedOutputNodes, th.ExecutedLogNodes,
		th.ChainReadUnknownCount, th.FieldActualReadCount, th.FieldPotentialReadCount, th.FieldReadUnknownCount, th.AccountReadCount, th.AccountReadUnknownCount,
		th.FieldActualWrittenCount, th.FieldPotentialWrittenCount, th.AccountWrittenCount,
		totalFailedJumps, totalJumps, totalJumpKeys,
		accountLaneSuccessfulJumps, fieldLaneSucessfulJumps, th.OpLaneSuccessfulJumps,
		th.AvgSpecializationDurationInNanoSeconds, th.AvgMemoizationDurationInNanoSeconds, th.TotalTraceCount,
		th.PathCount, th.RoundCount)
}

func (s ReuseStatus) String() string {
	var statusStr = s.BaseStatus.String()
	switch s.BaseStatus {
	case Hit:
		switch s.HitType {
		case MixHit:
			statusStr += ":MixHit"
		case TrieHit:
			statusStr += ":TrieHit"
		case DeltaHit:
			statusStr += ":DeltaHit"
		case TraceHit:
			statusStr += ":TraceHit"
		}
	case Miss:
		switch s.MissType {
		case NoMatchMiss:
			statusStr += ":NoMatchMiss"
		}
	}
	return statusStr
}

type ReuseBaseStatus int

func (s ReuseBaseStatus) String() string {
	switch s {
	case Fail:
		return "Fail"
	case NoPreplay:
		return "NoPreplay"
	case Hit:
		return "Hit"
	case Miss:
		return "Miss"
	case Unknown:
		return "Unknown"
	case Undefined:
		return "Undefined"
	default:
		panic("")
	}
	return ""
}

const (
	Fail ReuseBaseStatus = iota
	NoPreplay
	Hit
	Miss
	Unknown
	Undefined
)

type HitType int

const (
	IteraHit HitType = iota
	FastHit
	TrieHit
	DepHit
	MixHit
	DeltaHit
	TraceHit
)

func (s HitType) String() string {
	switch s {
	case TrieHit:
		return "Trie"
	case MixHit:
		return "Mix"
	case DeltaHit:
		return "Delta"
	case TraceHit:
		return "Trace"
	}
	return ""
}

type HitSubType int

const (
	AllDepHit HitSubType = iota
	AllDetailHit
	PartialHit  // partial dep and partial detail hit
	AllDeltaHit
	PartialDeltaHit  // partial dep and partial delta hit
	NotMixHit
	OpHit
)

func (m HitSubType) String() string {
	switch m {
	case AllDepHit:
		return "AllDep"
	case AllDetailHit:
		return "AllDetail"
	case PartialHit:
		return "Partial"
	case AllDeltaHit:
		return "AllDelta"
	case PartialDeltaHit:
		return "PartialDelta"
	case NotMixHit:
		return "NotMix"
	case OpHit:
		return "OpHit"
	default:
		panic(fmt.Sprintf("Unknown HitSubType %v", m))
	}
	return ""
}

type MissType int

const (
	TraceMiss MissType = iota
	MixMiss
	NoMatchMiss
)

func (m MissType) String() string {
	switch m {
	case TraceMiss:
		return "TraceMiss"
	case MixMiss:
		return "MixMiss"
	case NoMatchMiss:
		return "NoMatchMiss"
	default:
		panic("")
	}
}

type AbortStage int

const (
	TxPreplayLock AbortStage = iota
	TraceCheck
	MixCheck
	DeltaCheck
	TrieCheck
	ApplyDB
)

type RecordHolder interface {
	Equal(rwb RecordHolder) bool
	GetHash() common.Hash
	Dump() map[string]interface{}
	GetPreplayRes() interface{}
}

type IRound interface {
	GetRoundId() uint64
	GetRWRecordId() uintptr
}

type TxResID struct {
	Txhash  *common.Hash `json:"tx"`
	RoundID uint64       `json:"rID"`

	hash *string
}

func NewTxResID(txHash common.Hash, roundID uint64) *TxResID {
	bs := append(txHash.Bytes(), GetBytes(roundID)...)
	return &TxResID{Txhash: &txHash, RoundID: roundID, hash: ImmutableBytesToStringPtr(bs)}
}

//var DEFAULT_TXRESID_Hash = "123"
//var DEFAULT_TXRESID = &TxResID{hash: &DEFAULT_TXRESID_Hash}
var DEFAULT_TXRESID *TxResID

func (t *TxResID) Hash() *string {
	return t.hash
}

func (t *TxResID) Copy() AccountDepValue {
	return &TxResID{Txhash: t.Txhash, RoundID: t.RoundID, hash: t.hash}
}

func (t *TxResID) String() string {
	return fmt.Sprintf("%v_%v", t.Txhash.Hex(), t.RoundID)
}

type AccountDepValue interface {
	Hash() *string
	Copy() AccountDepValue
	String() string
}

type AccountSnap struct {
	hash  *string // convert bytes into `string`, that would be helpful for comparing
	bytes []byte
}

var EmptySnapHash = ""
var EmptyAccountSnap = &AccountSnap{hash: &EmptySnapHash, bytes: make([]byte, 0)}

func (a *AccountSnap) Copy() AccountDepValue {
	return &AccountSnap{hash: a.hash, bytes: a.bytes}
}

func (a AccountSnap) String() string {
	return hexutil.Encode(a.bytes)
}

func (a AccountSnap) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("0x%s", a.String()))
}

func (a AccountSnap) Hex() string {
	return a.String()
}

func (a AccountSnap) Hash() *string {
	return a.hash
}

func (a AccountSnap) Bytes() []byte {
	return a.bytes
}

func BytesToAccountSnap(bs []byte) *AccountSnap {
	//newBytes := make([]byte, len(bs))
	//copy(newBytes, bs)
	res := &AccountSnap{hash: ImmutableBytesToStringPtr(bs), bytes: bs}
	return res
}

func ImmutableBytesToStringPtr(buf []byte) *string {
	res := *(*string)(unsafe.Pointer(&buf))
	return &res
}

type SnapWithBlockHash struct {
	ParentBlockhash *common.Hash
	Snap            *AccountSnap
}

//
//type ChangedBy struct {
//	AccountSnap *AccountSnap `json:"snap"`
//	LastTxResID *TxResID     `json:"lastRes"`
//}
//
//func NewChangedBy(id *TxResID) *ChangedBy {
//	return &ChangedBy{LastTxResID: id}
//}
//
//func NewChangedBy2(accountSnap *AccountSnap) *ChangedBy {
//	return &ChangedBy{AccountSnap: accountSnap, LastTxResID: nil}
//}
//
//func (c *ChangedBy) AppendTx(txResID *TxResID) {
//	c.LastTxResID = txResID
//}
//
//func (c *ChangedBy) Copy() *ChangedBy {
//	return &ChangedBy{AccountSnap: c.AccountSnap, LastTxResID: c.LastTxResID}
//}
//
//func (c *ChangedBy) Hash() string {
//	if c.LastTxResID == nil {
//		return *c.AccountSnap.Hash()
//	} else {
//		return *c.LastTxResID.Hash()
//	}
//}

func GetBytes(v interface{}) []byte {
	var b bytes.Buffer
	b.Reset()
	gob.NewEncoder(&b).Encode(v)
	return b.Bytes()
}

type AccountDepMap map[common.Address]AccountDepValue
type TxResIDMap map[common.Address]*TxResID

type Location struct {
	Field Field
	Loc   interface{} // hash / number
}

type AddrLocation struct {
	Address common.Address `json:"address"`
	Field   Field          `json:"field"`
	Loc     interface{}    `json:"loc"` // hash / number
}

func (a AddrLocation) String() string {
	switch {
	case IsChainField(a.Field):
		switch a.Field {
		case Blockhash:
			return fmt.Sprintf("At %v.%d", a.Field, a.Loc.(uint64))
		default:
			return fmt.Sprintf("At %v", a.Field)
		}
	default:
		switch a.Field {
		case Storage, CommittedStorage:
			return fmt.Sprintf("At %s.%s.%s", a.Address.Hex(), a.Field, a.Loc.(common.Hash).TerminalString())
		default:
			return fmt.Sprintf("At %s.%s", a.Address.Hex(), a.Field)
		}
	}
}

func (a AddrLocation) Copy() AddrLocation {
	return AddrLocation{a.Address, a.Field, a.Loc}
}

type AddrLocValue struct {
	AddLoc *AddrLocation `json:"add_loc"`
	Value  interface{}   `json:"value"`
}

type ReadDetail struct {
	ReadDetailSeq          []*AddrLocValue // make sure all kinds of Value are simple types
	ReadAddressAndBlockSeq []*AddrLocValue // blockinfo (except for blockhash and blocknumber) and read account dep info seq
	IsBlockNumberSensitive bool            // whether block-related info (except for blockhash) in ReadDetailSeq
}

func NewReadDetail() *ReadDetail {
	return &ReadDetail{
		ReadDetailSeq:          []*AddrLocValue{},
		IsBlockNumberSensitive: false,
	}
}

func MyAssert(b bool, params ...interface{}) {
	if !b {
		msg := ""
		if len(params) > 0 {
			msg = fmt.Sprintf(params[0].(string), params[1:]...)
		}
		panic(msg)
	}
}

type IChildren interface {
	GetChild(interface{}) (*PreplayResTrieNode, bool)
	//InsertChild(interface{}, *PreplayResTrieNode)
	Delete(interface{})
	Size() int
	CopyKey() IChildren
	GetKeys() []interface{}
}

type UintChildren map[uint64]*PreplayResTrieNode

func (u UintChildren) GetChild(value interface{}) (*PreplayResTrieNode, bool) {
	realValue, ok := value.(uint64)
	if ok {
		node, find := u[realValue]
		return node, find
	} else {
		return nil, false
	}
}

func (u UintChildren) Delete(value interface{}) {
	realValue, ok := value.(uint64)
	MyAssert(ok)
	delete(u, realValue)
}

func (u UintChildren) Size() int {
	return len(u)
}

func (u UintChildren) CopyKey() IChildren {
	copyU := make(UintChildren, u.Size())

	for child := range u {
		copyU[child] = nil
	}
	return copyU
}

func (u UintChildren) GetKeys() []interface{} {
	res := make([]interface{}, u.Size())
	index := 0
	for k := range u {
		res[index] = k
		index++
	}
	return res
}

type HashChildren map[common.Hash]*PreplayResTrieNode

func (h HashChildren) GetChild(value interface{}) (*PreplayResTrieNode, bool) {
	realValue, ok := value.(common.Hash)
	if ok {
		node, find := h[realValue]
		return node, find
	} else {
		return nil, false
	}
}

func (h HashChildren) Delete(value interface{}) {
	realValue, ok := value.(common.Hash)
	MyAssert(ok)
	delete(h, realValue)
}

func (h HashChildren) Size() int {
	return len(h)
}

func (h HashChildren) CopyKey() IChildren {
	copyU := make(HashChildren, h.Size())

	for child := range h {
		copyU[child] = nil
	}
	return copyU
}

func (h HashChildren) GetKeys() []interface{} {
	res := make([]interface{}, h.Size())
	index := 0
	for k := range h {
		res[index] = k
		index++
	}
	return res
}

type BoolChildren map[bool]*PreplayResTrieNode

func (b BoolChildren) GetChild(value interface{}) (*PreplayResTrieNode, bool) {
	realValue, ok := value.(bool)
	if ok {
		node, find := b[realValue]
		return node, find
	} else {
		return nil, false
	}
}

func (b BoolChildren) Delete(value interface{}) {
	realValue, ok := value.(bool)
	MyAssert(ok)
	delete(b, realValue)
}

func (b BoolChildren) Size() int {
	return len(b)
}

func (b BoolChildren) CopyKey() IChildren {
	copyU := make(BoolChildren, b.Size())

	for child := range b {
		copyU[child] = nil
	}
	return copyU
}

func (b BoolChildren) GetKeys() []interface{} {
	res := make([]interface{}, b.Size())
	index := 0
	for k := range b {
		res[index] = k
		index++
	}
	return res
}

type AddressChildren map[common.Address]*PreplayResTrieNode

func (a AddressChildren) GetChild(value interface{}) (*PreplayResTrieNode, bool) {
	realValue, ok := value.(common.Address)
	if ok {
		node, find := a[realValue]
		return node, find
	} else {
		return nil, false
	}
}

func (a AddressChildren) Delete(value interface{}) {
	realValue, ok := value.(common.Address)
	MyAssert(ok)
	delete(a, realValue)
}

func (a AddressChildren) Size() int {
	return len(a)
}

func (a AddressChildren) CopyKey() IChildren {
	copyU := make(AddressChildren, a.Size())

	for child := range a {
		copyU[child] = nil
	}
	return copyU
}

func (a AddressChildren) GetKeys() []interface{} {
	res := make([]interface{}, a.Size())
	index := 0
	for k := range a {
		res[index] = k
		index++
	}
	return res
}

type StringChildren map[string]*PreplayResTrieNode

func (s StringChildren) GetChild(value interface{}) (*PreplayResTrieNode, bool) {
	realValue, ok := value.(string)
	if ok {
		node, find := s[realValue]
		return node, find
	} else {
		return nil, false
	}
}

func (s StringChildren) Delete(value interface{}) {
	realValue, ok := value.(string)
	MyAssert(ok)
	delete(s, realValue)
}

func (s StringChildren) Size() int {
	return len(s)
}

func (s StringChildren) CopyKey() IChildren {
	copyU := make(StringChildren, s.Size())

	for child := range s {
		copyU[child] = nil
	}
	return copyU
}

func (s StringChildren) GetKeys() []interface{} {
	res := make([]interface{}, len(s))
	index := 0
	for k := range s {
		res[index] = k
		index++
	}
	return res
}

const InitialChidrenLen = 10

func NewChildren(nodeType *AddrLocation) IChildren {
	switch nodeType.Field {
	case Coinbase:
		return make(AddressChildren, InitialChidrenLen)
	case Timestamp, Number, Difficulty, GasLimit, Nonce:
		return make(UintChildren, InitialChidrenLen)
	case Blockhash, Balance, CodeHash, Storage, CommittedStorage:
		return make(HashChildren, InitialChidrenLen)
	case Exist, Empty, MinBalance:
		return make(BoolChildren, InitialChidrenLen)
	case Dependence:
		return make(StringChildren, InitialChidrenLen)
	default:
		panic("Wrong Field")
	}
	return nil
}

type PreplayResTrieRoundNodes struct {
	LeafNodes []*PreplayResTrieNode
	RoundID   uint64
	Round     IRound
	Next      *PreplayResTrieRoundNodes
}

type PreplayResTrieNode struct {
	Value        interface{} `json:"value"` // this value is the key in its parent
	Children     IChildren                  //`json:"children"` //  map[interface{}]*PreplayResTrieNode // value => child node
	NodeType     *AddrLocation       `json:"node_type"`
	DetailChild  *PreplayResTrieNode `json:"detail_child"`
	Parent       *PreplayResTrieNode
	IsLeaf       bool `json:"is_leaf"`
	LeafRefCount int
	Round        IRound
	RSeqIndex    int // for detail node (which is not dependence node) in mix tree,  ReqIndex means the position of this read AddrLocVal in the origin read detail sequence
}

func (p *PreplayResTrieNode) MarshalJSON() ([]byte, error) {
	node := struct {
		Value       interface{}         `json:"value"`    // this value is the key in its parent
		Children    IChildren           `json:"children"` //  map[interface{}]*PreplayResTrieNode // value => child node
		NodeType    *AddrLocation       `json:"node_type"`
		DetailChild *PreplayResTrieNode `json:"detail_child"`
		IsLeaf      bool                `json:"is_leaf"`
		RSeqIndex   int
	}{
		Value:       p.Value,
		Children:    p.Children,
		NodeType:    p.NodeType,
		DetailChild: p.DetailChild,
		IsLeaf:      p.IsLeaf,
		RSeqIndex:   p.RSeqIndex,
	}
	return json.Marshal(node)
}

func (p *PreplayResTrieNode) IsDepNode() bool {
	return p.NodeType.Field == Dependence
}

func (p *PreplayResTrieNode) GetChildrenCount() int {
	res := 0
	if p.Children != nil {
		res += p.Children.Size()
	}
	if p.DetailChild != nil {
		res++
	}
	return res
}

func (p *PreplayResTrieNode) removeSelf() (removed int) {
	MyAssert(p.GetChildrenCount() == 0)
	removed = 1
	if p.Parent == nil {
		MyAssert(false, "remove ophan!")
	} else {
		if p.Value == nil {
			// this is a detailChild
			MyAssert(p == p.Parent.DetailChild)
			p.Parent.DetailChild = nil
			p.Parent.RSeqIndex = p.RSeqIndex
		} else {
			parent2child, ok := p.Parent.Children.GetChild(p.Value)
			MyAssert(ok)
			MyAssert(p == parent2child)
			p.Parent.Children.Delete(p.Value)
		}
		removed += p.Parent.RemoveRecursivelyIfNoChildren(nil)
	}
	return
}

func (p *PreplayResTrieNode) RemoveRecursivelyIfNoChildren(value interface{}) (removed int) {
	var refCount int
	if p.IsLeaf {
		MyAssert(p.LeafRefCount > 0)
		p.LeafRefCount -= 1
		if p.LeafRefCount == 0 {
			p.Round = nil
		}
		refCount = p.LeafRefCount
	} else {
		refCount = p.GetChildrenCount()
	}

	if refCount == 0 {
		removed = p.removeSelf()
	}
	return
}

type PreplayResTrie struct {
	Root              *PreplayResTrieNode
	LatestBN          uint64
	RoundCount        uint64 // rwset cound for detail trie. round count for dep tree and mix tree
	RoundIds          map[uint64]bool
	RoundRefNodesHead *PreplayResTrieRoundNodes
	RoundRefNodesTail *PreplayResTrieRoundNodes
	RoundRefCount     uint
	TrieNodeCount     int64
}

func (tt *PreplayResTrie) IsEmpty() bool {
	return tt.RoundRefCount == 0
}

func (tt *PreplayResTrie) GetNodeCount() int64 {
	return atomic.LoadInt64(&tt.TrieNodeCount)
}

func (rr *PreplayResTrie) TrackRoundNodes(refNodes []*PreplayResTrieNode, newNodeCount uint, round IRound) {
	if newNodeCount > 0 {
		r := &PreplayResTrieRoundNodes{
			LeafNodes: refNodes,
			RoundID:   round.GetRoundId(),
			Round:     round,
		}

		rr.RoundRefCount++
		atomic.AddInt64(&rr.TrieNodeCount, int64(newNodeCount))
		if rr.RoundRefNodesHead == nil {
			MyAssert(rr.RoundRefNodesTail == nil)
			//MyAssert(uint(len(refNodes)) == newNodeCount)
			rr.RoundRefNodesHead = r
			rr.RoundRefNodesTail = r
		} else {
			MyAssert(rr.RoundRefNodesTail.Next == nil)
			rr.RoundRefNodesTail.Next = r
			rr.RoundRefNodesTail = r
		}
		rr.GCRoundNodes()
	}
}

func (tt *PreplayResTrie) GetActiveIRounds() []IRound {
	rounds := make([]IRound, 0, tt.RoundRefCount)
	rr := tt.RoundRefNodesHead
	for rr != nil {
		rounds = append(rounds, rr.Round)
		rr = rr.Next
	}
	return rounds
}

func (rr *PreplayResTrie) GCRoundNodes() int64 {
	MyAssert(rr.RoundRefNodesHead != nil && rr.RoundRefNodesTail != nil)
	totalRemoved := int64(0)
	if rr.RoundRefCount >= uint(config.TXN_PREPLAY_ROUND_LIMIT*10+1) {

		head := rr.RoundRefNodesHead
		totalRemoved += rr.RemoveRoundNodes(head)

		rr.RoundRefNodesHead = head.Next

	}
	return totalRemoved
}

func (rr *PreplayResTrie) RemoveRoundNodes(rf *PreplayResTrieRoundNodes) int64 {
	removedNodes := int64(0)
	for _, n := range rf.LeafNodes {
		removedNodes += int64(n.RemoveRecursivelyIfNoChildren(rf.RoundID))
	}
	rr.RoundRefCount--
	atomic.AddInt64(&(rr.TrieNodeCount), -removedNodes)
	MyAssert(rr.RoundRefCount >= 0)
	MyAssert(atomic.LoadInt64(&(rr.TrieNodeCount)) >= 0)
	return removedNodes
}

func (p *PreplayResTrie) AddExistedRound(blockNumber uint64) {
	if blockNumber > p.LatestBN {
		p.LatestBN = blockNumber
	}
}

func NewPreplayResTrie() *PreplayResTrie {
	rootNode := &PreplayResTrieNode{}
	return &PreplayResTrie{
		Root:       rootNode,
		LatestBN:   0,
		RoundCount: 0,
	}
}

//func (t *PreplayResTrie) Clear() {
//	t.Root = &PreplayResTrieNode{}
//	t.LatestBN = 0
//	t.RoundCount = 0
//}

//// used for dep tree and mix tree
//func (t *PreplayResTrie) ClearOld(gap uint64) {
//	if t.Root == nil || t.Root.NodeType == nil {
//		return
//	}
//	if t.Root.NodeType.Field != Number {
//		panic("wrong nodeType of root node")
//	}
//	if t.Root.Children == nil {
//		return
//	}
//	for bn := range t.Root.Children {
//		if bn.(uint64) < t.LatestBN-gap {
//			delete(t.Root.Children, bn)
//		}
//	}
//
//}
