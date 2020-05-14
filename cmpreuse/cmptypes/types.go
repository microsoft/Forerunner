// MSRA Computation Reuse Model

package cmptypes

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/optipreplayer/config"
)

type Field int

const (
	// Chain info
	Blockhash Field = iota + 1
	Coinbase
	Timestamp
	Number
	Difficulty
	GasLimit  // 6

	PreBlockHash  // used in the top of dep tree (in fact, there is a blocknumber layer on the top of PreBlockHash

	// State info
	Balance           //8
	Nonce             //9
	CodeHash          //10
	Exist             // 11
	Empty             // 12
	Code              // 13
	Storage           // 14
	CommittedStorage  //15

	// Dep info
	Dependence  //16

	// Write State
	DirtyStorage
	Suicided

	//DeltaInfo
	MinBalance
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
	case PreBlockHash:
		return "preBlockHash"
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
	}
	return ""
}

func IsChainField(field Field) bool {
	return field < Balance
}

func IsStateField(field Field) bool {
	return field > PreBlockHash && field < Dependence
}

type ReuseStatus struct {
	BaseStatus        ReuseBaseStatus
	HitType           HitType
	MissType          MissType
	MixHitStatus      *MixHitStatus
	MissNode          *PreplayResTrieNode
	MissValue         interface{}
	AbortStage        AbortStage
	TraceTrieHitAddrs TxResIDMap
}

type MixHitStatus struct {
	MixHitType         MixHitType
	DepHitAddr         []common.Address
	DepHitAddrMap      map[common.Address]interface{}
	DepUnmatchedInHead int // when partial hit, the count of dep unmatched addresses which are in the front of the first matched addr
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
	}
	return ""
}

const (
	Fail ReuseBaseStatus = iota
	NoPreplay
	Hit
	Miss
	Unknown
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

type MixHitType int

const (
	AllDepHit MixHitType = iota
	AllDetailHit
	PartialHit
	NotMixHit
)

type MissType int

const (
	NoInMiss MissType = iota
	NoMatchMiss
)

type AbortStage int

const (
	TraceCheck AbortStage = iota
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
}

type TxResID struct {
	Txhash  *common.Hash `json:"tx"`
	RoundID uint64       `json:"rID"`

	hash    *common.Hash
	hasHash bool
}

func NewTxResID(txHash common.Hash, roundID uint64) *TxResID {
	return &TxResID{Txhash: &txHash, RoundID: roundID, hasHash: false}
}

func NewDefaultTxResID(txHash common.Hash) *TxResID {
	return &TxResID{Txhash: &txHash, RoundID: 0, hasHash: true, hash: &txHash}
}

func (t *TxResID) Hash() *common.Hash {
	if t.hasHash {
		return t.hash
	} else {
		bs := append(t.Txhash.Bytes(), GetBytes(t.RoundID)...)
		res := new(common.Hash)
		res.SetBytes(bs)
		t.hash = res
		t.hasHash = true
		return t.hash
	}
}

const AccountSnapLen = 90

type AccountSnap [AccountSnapLen]byte

func (a AccountSnap) String() string {
	return hexutil.Encode(a[:])
}

func (a AccountSnap) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("0x%s", a.String()))
}

func BytesToAccountSnap(bs []byte) *AccountSnap {
	if len(bs) > AccountSnapLen {
		bs = bs[len(bs)-AccountSnapLen:]
		//
		panic("too long bytes " + "" + string(len(bs)))
	}
	var as AccountSnap
	copy(as[AccountSnapLen-len(bs):], bs)
	return &as
}

type ChangedBy struct {
	AccountSnap *AccountSnap
	LastTxResID *TxResID `json:"lastRes"`

	hash        interface{} // for each seq, only cal once
	hashUpdated bool
}

func NewChangedBy() *ChangedBy {
	return &ChangedBy{LastTxResID: nil, hash: nil, hashUpdated: false}
}

func NewChangedBy2(accountSnap *AccountSnap) *ChangedBy {
	return &ChangedBy{AccountSnap: accountSnap, LastTxResID: nil, hash: nil, hashUpdated: false}
}

func (c *ChangedBy) AppendTx(txResID *TxResID) {
	c.LastTxResID = txResID
	c.hashUpdated = false
}

func (c *ChangedBy) Copy() *ChangedBy {
	c.updateHash()
	return &ChangedBy{AccountSnap: c.AccountSnap, LastTxResID: c.LastTxResID, hash: c.hash, hashUpdated: c.hashUpdated}
}

func (c *ChangedBy) updateHash() {
	if !c.hashUpdated {
		if c.LastTxResID == nil {
			c.hash = c.AccountSnap
		} else {
			c.hash = c.LastTxResID.Hash()

		}
	}
	c.hashUpdated = true
}

func (c *ChangedBy) Hash() interface{} {
	c.updateHash()
	if c.LastTxResID == nil {
		return *c.hash.(*AccountSnap)
	} else {
		return *c.hash.(*common.Hash)
	}
}

func GetBytes(v interface{}) []byte {
	var b bytes.Buffer
	b.Reset()
	gob.NewEncoder(&b).Encode(v)
	return b.Bytes()
}

type ChangedMap map[common.Address]*ChangedBy
type TxResIDMap map[common.Address]*TxResID

func (cm ChangedMap) Copy() ChangedMap {
	newCM := make(map[common.Address]*ChangedBy)
	for addr, changedBy := range cm {
		newCM[addr] = changedBy.Copy()
	}
	return newCM
}

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

type ISRefCountNode interface {
	GetRefCount() uint
	AddRef() uint                // add a ref and return the new ref count
	RemoveRef(value interface{}) // remove a ref
}

type SRefCount struct {
	refCount uint
}

func (rf *SRefCount) GetRefCount() uint {
	return rf.refCount
}

func (rf *SRefCount) AddRef() uint {
	rf.refCount++
	return rf.refCount
}

func (rf *SRefCount) SRemoveRef() uint {
	MyAssert(rf.refCount > 0)
	rf.refCount--
	return rf.refCount
}

type TrieRoundRefNodes struct {
	RefNodes []ISRefCountNode
	RoundID  uint64
	Next     *TrieRoundRefNodes
}

type TrieRoundRef struct {
	RoundRefNodesHead *TrieRoundRefNodes
	RoundRefNodesTail *TrieRoundRefNodes
	RoundRefCount     uint
	TrieNodeCount     int64
}

func (rr *TrieRoundRef) TrackRoundRefNodes(refNodes []ISRefCountNode, newNodeCount uint, round IRound) {
	if newNodeCount > 0 {
		//for _, n := range refNodes {
		//	rc := n.AddRef()
		//	MyAssert(rc > 0)
		//}

		r := &TrieRoundRefNodes{
			RefNodes: refNodes,
			RoundID:  round.GetRoundId(),
		}

		rr.RoundRefCount++
		rr.TrieNodeCount += int64(newNodeCount)
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
		rr.GCRoundRefNodes()
	}
}

func (rr *TrieRoundRef) GCRoundRefNodes() int64 {
	MyAssert(rr.RoundRefNodesHead != nil && rr.RoundRefNodesTail != nil)
	totalRemoved := int64(0)
	if rr.RoundRefCount > uint(config.TXN_PREPLAY_ROUND_LIMIT) {

		for i := 0; i < 1; i++ {
			head := rr.RoundRefNodesHead
			totalRemoved += rr.RemoveRoundRef(head)

			rr.RoundRefNodesHead = head.Next
		}
	}
	return totalRemoved
}

func (rr *TrieRoundRef) RemoveRoundRef(rf *TrieRoundRefNodes) int64 {
	removedNodes := int64(0)
	for _, n := range rf.RefNodes {
		n.RemoveRef(rf.RoundID)
		if n.GetRefCount() == 0 {
			removedNodes++ // TODO
		}
	}
	rr.RoundRefCount--
	rr.TrieNodeCount -= removedNodes
	MyAssert(rr.RoundRefCount >= 0)
	MyAssert(rr.TrieNodeCount >= 0)
	return removedNodes
}

type PreplayResTrieNode struct {
	Value       interface{}                         // this value is the key in its parent
	Children    map[interface{}]*PreplayResTrieNode // value => child node
	NodeType    *AddrLocation
	DetailChild *PreplayResTrieNode
	Parent      *PreplayResTrieNode
	IsLeaf      bool
	Round       IRound

	//SRefCount // RefCount in PreplayResTrieNode means the number of children nodes (including DetailChild)
}

func (p *PreplayResTrieNode) GetRefCount() uint {
	res := 0
	if p.Children != nil {
		res += len(p.Children)
	}
	if p.DetailChild != nil {
		res++
	}
	return uint(res)
}

// this function is useless
func (p *PreplayResTrieNode) AddRef() uint{
	return p.GetRefCount()
}

func (p *PreplayResTrieNode) deRefSelf() {
	MyAssert(p.GetRefCount() == 0)
	if p.Parent == nil {

	} else {
		if p.Value == nil {
			// this is a detailChild
			MyAssert(p == p.Parent.DetailChild)
			p.Parent.DetailChild = nil
		} else {
			parent2child, ok := p.Parent.Children[p.Value]
			MyAssert(ok)
			MyAssert(p == parent2child)
			delete(p.Parent.Children, p.Value)
		}
		p.Parent.RemoveRef(nil)
	}
}

func (p *PreplayResTrieNode) RemoveRef(value interface{}) {
	refCount := p.GetRefCount()

	if p.IsLeaf {
		roundId := value.(uint64)
		MyAssert(roundId == p.Round.GetRoundId())
		MyAssert(refCount == 0)
		p.Round = nil
	}

	if refCount == 0 {
		p.deRefSelf()
	}
}

type PreplayResTrie struct {
	Root      *PreplayResTrieNode
	LatestBN  uint64
	LeafCount uint64 // rwset cound for detail trie. round count for dep tree and mix tree
	RoundIds  map[uint64]bool
	IsCleared bool
	TrieRoundRef
}

func (p *PreplayResTrie) AddExistedRound(blockNumber uint64) {
	if blockNumber > p.LatestBN {
		p.LatestBN = blockNumber
	}
}

func NewPreplayResTrie() *PreplayResTrie {
	rootNode := &PreplayResTrieNode{Children: make(map[interface{}]*PreplayResTrieNode)}
	return &PreplayResTrie{
		Root:      rootNode,
		LatestBN:  0,
		LeafCount: 0,
		RoundIds:  make(map[uint64]bool),
		IsCleared: false,
	}
}

func (t *PreplayResTrie) Clear() {
	t.Root = &PreplayResTrieNode{Children: make(map[interface{}]*PreplayResTrieNode)}
	t.LatestBN = 0
	t.LeafCount = 0
	t.RoundIds = make(map[uint64]bool)
	t.IsCleared = true
}

// used for dep tree and mix tree
func (t *PreplayResTrie) ClearOld(gap uint64) {
	if t.Root == nil || t.Root.NodeType == nil {
		return
	}
	if t.Root.NodeType.Field != Number {
		panic("wrong nodeType of root node")
	}
	if t.Root.Children == nil {
		return
	}
	for bn := range t.Root.Children {
		if bn.(uint64) < t.LatestBN-gap {
			delete(t.Root.Children, bn)
		}
	}

}
