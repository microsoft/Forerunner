// MSRA Computation Reuse Model

package cmptypes

import (
	"github.com/ethereum/go-ethereum/common"
)

type Field int

const (
	// Chain info
	Blockhash  Field = iota + 1
	Coinbase
	Timestamp
	Number
	Difficulty
	GasLimit

	// State info
	Balance
	Nonce
	CodeHash
	Exist
	Empty
	Code
	Storage
	CommittedStorage

	// Write State
	DirtyStorage
	Suicided
)

type ReuseStatus struct {
	BaseStatus ReuseBaseStatus
	HitType    HitType
}

type ReuseBaseStatus int

const (
	Fail         ReuseBaseStatus = iota
	Hit
	NoCache
	CacheNoIn
	CacheNoMatch
	Unknown
)

type HitType int

const (
	IteraHit HitType = iota
	FastHit
	TrieHit
)

type RecordHolder interface {
	Equal(rwb RecordHolder) bool
	GetHash() string
	Dump() map[string]interface{}
	GetPreplayRes() interface{}
}

type PreplayResHolder interface {
	// TODO
}

//var AddressForChainField = common.BigToAddress(big.NewInt(-1))

type Location struct {
	Field Field
	Loc   interface{} // hash / number
}

type AddrLocation struct {
	Address common.Address `json:"address"`
	Field   Field          `json:"field"`
	IsChain bool           `json:"is_chain"`
	Loc     interface{}    `json:"loc"` // hash / number
}

type AddrLocValue struct {
	AddLoc *AddrLocation  `json:"add_loc"`
	Value  interface{}	`json:"value"`
}

type ReadDetail struct {
	ReadDetailSeq    []*AddrLocValue `json:"read_detail_seq"`// make sure all kinds of Value are simple types

	ReadAddress      []common.Address
	IsBlockSensitive bool // whether block-related info (except for blockhash) in ReadDetailSeq
}

func NewReadDetail() *ReadDetail {
	return &ReadDetail{
		ReadDetailSeq:    []*AddrLocValue{},
		ReadAddress:      []common.Address{},
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
}

//type RWTrieHolder interface {
//	Insert(record RecordHolder)
//	Search(db interface{}, bc interface{}, header *types.Header) (*PreplayResTrieNode, bool)
//}

type PreplayResTrie struct {
	Root             *PreplayResTrieNode
	IsBlockSensitive bool // whether blocknumber or blocktime in the trie
	LatestBN         uint64
	RWSetCount       uint64
	RoundIds         map[uint64]bool
}

func NewPreplayResTrie() *PreplayResTrie {
	rootNode := &PreplayResTrieNode{}
	return &PreplayResTrie{
		Root:             rootNode,
		IsBlockSensitive: false,
		LatestBN:         0,
		RWSetCount:       0,
		RoundIds:         make(map[uint64]bool),
	}
}

func (t *PreplayResTrie) Clear() {
	t.Root = &PreplayResTrieNode{}
	t.IsBlockSensitive = false
	t.LatestBN = 0
	t.RWSetCount = 0
	t.RoundIds = make(map[uint64]bool)
}
