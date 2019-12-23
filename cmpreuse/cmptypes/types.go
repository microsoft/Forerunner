// MSRA Computation Reuse Model

package cmptypes

type Field int

const (
	// Chain info
	Blockhash Field = iota + 1
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

type ReuseStatus int

const (
	Fail ReuseStatus = iota
	Hit
	NoCache
	CacheNoIn
	CacheNoMatch
	Unknown
	FastHit
)
