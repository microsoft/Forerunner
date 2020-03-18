// MSRA Computation Reuse Model

package cmptypes

import (
	"bytes"
	"encoding/gob"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

type Field int

const (
	// Chain info
	Blockhash Field = iota + 1
	Coinbase
	Timestamp
	Number
	Difficulty
	GasLimit // 6

	PreBlockHash // used in the top of dep tree (in fact, there is a blocknumber layer on the top of PreBlockHash

	// State info
	Balance //8
	Nonce
	CodeHash
	Exist
	Empty
	Code
	Storage
	CommittedStorage //15

	// Dep info
	Dependence //16

	// Write State
	DirtyStorage
	Suicided

	//
)

func IsChainField(field Field) bool {
	return field < Balance
}

func IsStateField(field Field) bool {
	return field > PreBlockHash && field < Dependence
}

type ReuseStatus struct {
	BaseStatus   ReuseBaseStatus
	HitType      HitType
	MissType     MissType
	MixHitStatus *MixHitStatus
}

type MixHitStatus struct {
	MixHitType         MixHitType
	DepHitAddr         []common.Address
	DepHitAddrMap      map[common.Address]bool
	DepUnmatchedInHead int // when partial hit, the count of dep unmatched addresses which are in the front of the first matched addr
}

func (s ReuseStatus) String() string {
	var statusStr = s.BaseStatus.String()
	switch s.BaseStatus {
	case Hit:
		switch s.HitType {
		case IteraHit:
			statusStr += ":IteraHit"
		case TrieHit:
			statusStr += ":TrieHit"
		case DepHit:
			statusStr += ":DepHit"
		}
	case Miss:
		switch s.MissType {
		case NoInMiss:
			statusStr += ":NoInMiss"
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
)

type MixHitType int

const (
	AllDepHit MixHitType = iota
	AllDetailHit
	PartialHit
)

type MissType int

const (
	NoInMiss MissType = iota
	NoMatchMiss
)

type RecordHolder interface {
	Equal(rwb RecordHolder) bool
	GetHash() common.Hash
	Dump() map[string]interface{}
	GetPreplayRes() interface{}
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
	return []byte(a.String()), nil
}

func BytesToAccountSnap(bs []byte) *AccountSnap {
	if len(bs) > AccountSnapLen {
		bs = bs[len(bs)-AccountSnapLen:]
		//fixme:
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

type PreplayResTrieNode struct {
	Children map[interface{}]*PreplayResTrieNode // value => child node
	NodeType *AddrLocation
	//QuickChild  *PreplayResTrieNode
	DetailChild *PreplayResTrieNode
	IsLeaf      bool
	RWRecord    RecordHolder
	Round       interface{} // TODO: XXX
}

//type RWTrieHolder interface {
//	Insert(record RecordHolder)
//	Search(db interface{}, bc interface{}, header *types.Header) (*PreplayResTrieNode, bool)
//}

type PreplayResTrie struct {
	Root     *PreplayResTrieNode
	LatestBN uint64
	// deprecated
	LeafCount uint64 // rwset cound for detail trie. round count for dep tree and mix tree
	RoundIds  map[uint64]bool
	IsCleared bool
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
