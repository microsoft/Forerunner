// MSRA Computation Reuse Model

package cmptypes

import (
	"bytes"
	"encoding/gob"
	"github.com/ethereum/go-ethereum/common"
)

type Field int

const (
	// Chain info
	Blockhash Field = iota + 1
	Coinbase
	Timestamp
	Number
	Difficulty
	GasLimit

	PreBlockHash // used in the top of dep tree (in fact, there is a blocknumber layer on the top of PreBlockHash

	// State info
	Balance
	Nonce
	CodeHash
	Exist
	Empty
	Code
	Storage
	CommittedStorage

	// Dep info
	Dependence

	// Write State
	DirtyStorage
	Suicided

	//
)

type ReuseStatus struct {
	BaseStatus ReuseBaseStatus
	HitType    HitType
	MissType   MissType
}

func (s ReuseStatus) String() string {
	var statusStr string
	switch s.BaseStatus {
	case Fail:
		statusStr = "Fail"
	case NoPreplay:
		statusStr = "NoPreplay"
	case Hit:
		statusStr = "Hit:"
		switch s.HitType {
		case IteraHit:
			statusStr += "IteraHit"
		case TrieHit:
			statusStr += "TrieHit"
		case DepHit:
			statusStr += "DepHit"
		}
	case Miss:
		statusStr = "Miss:"
		switch s.MissType {
		case NoInMiss:
			statusStr += "NoInMiss"
		case NoMatchMiss:
			statusStr += "NoMatchMiss"
		}
	case Unknown:
		statusStr = "Unknown"
	}
	return statusStr
}

type ReuseBaseStatus int

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

type ChangedBy struct {
	LastTxResID *TxResID `json:"lastRes"`
	//Bytes       []byte
	//
	hash        *common.Hash // for each seq, only cal once
	hashUpdated bool
}

func NewChangedBy() *ChangedBy {
	return &ChangedBy{LastTxResID: nil, hash: nil, hashUpdated: false}
}

func (c *ChangedBy) AppendTx(txResID *TxResID) {
	c.LastTxResID = txResID
	c.hashUpdated = false
}

func (c *ChangedBy) Copy() *ChangedBy {
	//newPtr := new(big.Int).Set(c.LastTxResID)
	//return &ChangedBy{LastTxResID:newPtr}
	c.updateHash()
	return &ChangedBy{LastTxResID: c.LastTxResID, hash: c.hash, hashUpdated: c.hashUpdated}
}

func (c *ChangedBy) updateHash() {
	if !c.hashUpdated {
		if c.LastTxResID == nil {
			c.hash = &common.Hash{}
		} else {
			c.hash = c.LastTxResID.Hash()
			c.hashUpdated = true
		}
	}
}
func (c *ChangedBy) Hash() common.Hash {
	c.updateHash()

	// TODO : might return origin
	res := new(common.Hash)
	res.SetBytes(c.hash.Bytes())
	return *res
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

// Deprecated
type StateDBChangedBy struct {
	ChangedBy   *ChangedBy
	CannotMatch bool // true for: When block processing, there is a tx whose result did not exist. which means if true, the following txs which read this account cannot be matched in dep tree
}

// Deprecated
type StateDBChangedMap map[common.Address]*StateDBChangedBy

//var AddressForChainField = common.BigToAddress(big.NewInt(-1))

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
	ReadAddressAndBlockSeq []*AddrLocValue // only block-related info seq
	IsBlockSensitive       bool            // whether block-related info (except for blockhash) in ReadDetailSeq
}

func NewReadDetail() *ReadDetail {
	return &ReadDetail{
		ReadDetailSeq:    []*AddrLocValue{},
		IsBlockSensitive: false,
	}
}

type PreplayResTrieNode struct {
	Children    map[interface{}]*PreplayResTrieNode // value => child node
	NodeType    *AddrLocation
	QuickChild  *PreplayResTrieNode
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
	Root             *PreplayResTrieNode
	IsBlockSensitive bool // whether blocknumber or blocktime in the trie
	LatestBN         uint64
	LeafCount        uint64
	RoundIds         map[uint64]bool
}

func (p *PreplayResTrie) AddExistedRound(blockNumber uint64) {
	if blockNumber > p.LatestBN {
		p.LatestBN = blockNumber
	}
}

func NewPreplayResTrie() *PreplayResTrie {
	rootNode := &PreplayResTrieNode{}
	return &PreplayResTrie{
		Root:             rootNode,
		IsBlockSensitive: false,
		LatestBN:         0,
		LeafCount:        0,
		RoundIds:         make(map[uint64]bool),
	}
}

func (t *PreplayResTrie) Clear() {
	t.Root = &PreplayResTrieNode{}
	t.IsBlockSensitive = false
	t.LatestBN = 0
	t.LeafCount = 0
	t.RoundIds = make(map[uint64]bool)
}
