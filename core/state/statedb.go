// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package state provides a caching layer atop the Ethereum state trie.
package state

import (
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"

	lru "github.com/hashicorp/golang-lru"
)

type revision struct {
	id           int
	journalIndex int
}

var (
	// emptyRoot is the known root hash of an empty trie.
	emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

	// emptyCode is the known hash of the empty EVM bytecode.
	emptyCode = crypto.Keccak256Hash(nil)
)

type proofList [][]byte

func (n *proofList) Put(key []byte, value []byte) error {
	*n = append(*n, value)
	return nil
}

func (n *proofList) Delete(key []byte) error {
	panic("not supported")
}

type deltaDB struct {
	stateObjects      map[common.Address]*stateObject
	stateObjectsDirty map[common.Address]struct{}
	logs              []*types.Log
	preimages         map[common.Hash][]byte
}

func newDeltaDB() *deltaDB {
	return &deltaDB{
		stateObjects:      make(map[common.Address]*stateObject, 300),
		stateObjectsDirty: make(map[common.Address]struct{}, 300),
		logs:              make([]*types.Log, 0),
		preimages:         make(map[common.Hash][]byte),
	}
}

type TxPerfAndStatus struct {
	Receipt              *types.Receipt
	Time                 time.Duration
	ReuseStatus          *cmptypes.ReuseStatus
	Tx                   *types.Transaction
	Delay                float64
	AddrWarmupMiss       int
	KeyWarmupMiss        int
	AddrCreateWarmupMiss int
	KeyCreateWarmupMiss  int
}

type PauseableCache interface {
	PauseForProcess()
}

// StateDBs within the ethereum protocol are used to store anything
// within the merkle trie. StateDBs take care of caching and storing
// nested states. It's the general query interface to retrieve:
// * Contracts
// * Accounts
type StateDB struct {
	db   Database
	trie Trie

	// This map holds 'live' objects, which will get modified while processing a state transition.
	stateObjects           map[common.Address]*stateObject
	stateObjectsPending    map[common.Address]struct{} // State objects finalized but not yet written to the trie
	stateObjectsDirty      map[common.Address]struct{} // State objects modified in the current execution
	createdObjectsInWarmup map[common.Address]*stateObject

	// DB error.
	// State objects are used by the consensus core and VM which are
	// unable to deal with database-level errors. Any error that occurs
	// during a database read is memoized here and will eventually be returned
	// by StateDB.Commit.
	dbErr error

	// The refund counter, also used by state transitioning.
	refund uint64

	thash, bhash common.Hash
	txIndex      int
	logs         map[common.Hash][]*types.Log
	logSize      uint

	preimages map[common.Hash][]byte

	// Journal of state modifications. This is the backbone of
	// Snapshot and RevertToSnapshot.
	journal        *journal
	validRevisions []revision
	nextRevisionId int

	savedDirties map[common.Address]int

	// Measurements gathered during execution for debugging purposes
	AccountReads   time.Duration
	AccountHashes  time.Duration
	AccountUpdates time.Duration
	AccountCommits time.Duration
	StorageReads   time.Duration
	StorageHashes  time.Duration
	StorageUpdates time.Duration
	StorageCommits time.Duration

	// MSRA fields !!! don't forget to initialize in New() and Copy()
	EnableFeeToCoinbase bool // default true
	allowObjCopy        bool // default true
	addrNotCopy         map[common.Address]struct{}
	copyForShare        bool       // default false
	rwRecorder          RWRecorder // RWRecord mode
	rwRecorderEnabled   bool

	ReuseTracer cmptypes.IReuseTracer

	primary bool
	pair    *StateDB
	delta   *deltaDB

	// Deprecated
	ProcessedTxs []common.Hash // txs which have been processed after the base block

	AccountDeps      cmptypes.AccountDepMap
	IsParallelHasher bool
	BloomProcessor   *types.ParallelBloomProcessor
	ReceiptMutex     sync.Mutex
	CurrentReceipt   *types.Receipt
	// to pre-allocate frequently used objects
	preAllocatedOriginStorages  []Storage
	nextOriginStorageIndex      int
	preAllocatedPendingStorages []Storage
	nextPendingStorageIndex     int
	preAllocatedDeltaObjects    []*deltaObject
	nextDeltaObjectIndex        int
	preAllocatedBigInt          []*big.Int
	nextBigIntIndex             int
	preAllocatedTxPerfs         []*TxPerfAndStatus
	nextTxPerfIndex             int
	preAllocatedJournals        []*journal
	nextJournalIndex            int
	preAllocatedLogArrays       [][]*types.Log
	nextLogArrayIndex           int

	ProcessedForDb         map[common.Address]map[common.Hash]struct{}
	ProcessedForObj        map[common.Address]map[common.Hash]struct{}
	CalWarmupMiss          bool
	WarmupMissDetail       bool
	AccountCreate          int
	AddrWarmupMiss         int
	AddrNoWarmup           int
	AddrCreateWarmupMiss   map[common.Address]struct{}
	KeyWarmupMiss          int
	KeyNoWarmup            int
	KeyCreateWarmupMiss    int
	AccessedAccountAndKeys map[common.Address]map[common.Hash]struct{}

	UnknownTxs        []*types.Transaction
	UnknownTxReceipts []*types.Receipt
	TxPerfs           []*TxPerfAndStatus

	//Time consumption detail in finalize
	waitUpdateRoot time.Duration
	updateObj      time.Duration
	hashTrie       time.Duration

	IgnoreJournalEntry bool
	InWarmupMode       bool
	PauseableCache     PauseableCache
}

func (self *StateDB) AddTxPerf(receipt *types.Receipt, time time.Duration, status *cmptypes.ReuseStatus, delayInSecond float64, tx *types.Transaction,
	AddrWarmupMiss, KeyWarmupMiss, AddrCreateWarmupMiss, KeyCreateWarmupMiss int) {
	tf := self.GetNewTxPerf()
	tf.ReuseStatus = status
	tf.Receipt = receipt
	tf.Time = time
	tf.Delay = delayInSecond
	tf.Tx = tx
	tf.AddrWarmupMiss = AddrWarmupMiss
	tf.KeyWarmupMiss = KeyWarmupMiss
	tf.AddrCreateWarmupMiss = AddrCreateWarmupMiss
	tf.KeyCreateWarmupMiss = KeyCreateWarmupMiss
	self.TxPerfs = append(self.TxPerfs, tf)
}

func (self *StateDB) GetPendingStateObject() map[common.Address]*stateObject {
	objects := self.stateObjects
	if self.IsShared() {
		objects = self.delta.stateObjects
	}
	ret := make(map[common.Address]*stateObject)
	for addr, obj := range objects {
		if _, ok := self.stateObjectsPending[addr]; ok {
			ret[addr] = obj
		}
	}
	return ret
}

var aBigInt = crypto.Keccak256Hash(common.Hex2Bytes("abignumber")).Big()

func (self *StateDB) PreAllocateObjects() {
	preOriginCount := 400
	prePendingCount := 100
	preDeltaCount := 100
	preBigIntCount := 600
	preTxPerfCount := 300
	preJournalCount := 300
	preLogArrayCount := 300

	self.preAllocatedOriginStorages = make([]Storage, preOriginCount)
	self.preAllocatedPendingStorages = make([]Storage, prePendingCount)
	self.preAllocatedDeltaObjects = make([]*deltaObject, preDeltaCount)
	self.preAllocatedBigInt = make([]*big.Int, preBigIntCount)
	self.preAllocatedJournals = make([]*journal, preJournalCount)
	self.preAllocatedLogArrays = make([][]*types.Log, preLogArrayCount)

	for i := range self.preAllocatedOriginStorages {
		self.preAllocatedOriginStorages[i] = make(Storage)
	}
	for i := range self.preAllocatedPendingStorages {
		self.preAllocatedPendingStorages[i] = make(Storage, 200)
	}
	for i := range self.preAllocatedDeltaObjects {
		self.preAllocatedDeltaObjects[i] = newDeltaObject()
	}

	for i := range self.preAllocatedBigInt {
		bi := big.NewInt(0)
		bi.Set(aBigInt) // increase the capacity of the underlying buffer
		self.preAllocatedBigInt[i] = bi
	}
	self.TxPerfs = make([]*TxPerfAndStatus, 0, preTxPerfCount)
	self.preAllocatedTxPerfs = make([]*TxPerfAndStatus, preTxPerfCount)
	for i := range self.preAllocatedTxPerfs {
		self.preAllocatedTxPerfs[i] = &TxPerfAndStatus{}
	}
	for i := range self.preAllocatedJournals {
		self.preAllocatedJournals[i] = newJournal()
	}
	for i := range self.preAllocatedLogArrays {
		self.preAllocatedLogArrays[i] = make([]*types.Log, 0, 30)
	}
}

func (self *StateDB) GetNewOriginStorage() Storage {
	if self.nextOriginStorageIndex >= len(self.preAllocatedOriginStorages) {
		return make(Storage)
	}
	s := self.preAllocatedOriginStorages[self.nextOriginStorageIndex]
	self.nextOriginStorageIndex++
	return s
}

func (self *StateDB) GetNewPendingStorage() Storage {
	if self.nextPendingStorageIndex >= len(self.preAllocatedPendingStorages) {
		return make(Storage)
	}
	s := self.preAllocatedPendingStorages[self.nextPendingStorageIndex]
	self.nextPendingStorageIndex++
	return s
}

func (self *StateDB) GetNewDeltaObject() *deltaObject {
	if self.nextDeltaObjectIndex >= len(self.preAllocatedDeltaObjects) {
		return newDeltaObject()
	}
	d := self.preAllocatedDeltaObjects[self.nextDeltaObjectIndex]
	self.nextDeltaObjectIndex++
	return d
}

func (self *StateDB) GetNewBigInt() *big.Int {
	if self.nextBigIntIndex >= len(self.preAllocatedBigInt) {
		return new(big.Int)
	}
	b := self.preAllocatedBigInt[self.nextBigIntIndex]
	self.nextBigIntIndex++
	return b
}

func (self *StateDB) GetNewTxPerf() *TxPerfAndStatus {
	if self.nextTxPerfIndex >= len(self.preAllocatedTxPerfs) {
		return &TxPerfAndStatus{}
	}
	tf := self.preAllocatedTxPerfs[self.nextTxPerfIndex]
	self.nextTxPerfIndex++
	return tf
}

func (self *StateDB) GetNewJournal() *journal {
	if self.nextJournalIndex >= len(self.preAllocatedJournals) {
		return newJournal()
	}
	tf := self.preAllocatedJournals[self.nextJournalIndex]
	self.nextJournalIndex++
	return tf
}

func (self *StateDB) GetNewLogArray() []*types.Log {
	if self.nextLogArrayIndex >= len(self.preAllocatedLogArrays) {
		return make([]*types.Log, 0, 30)
	}
	tf := self.preAllocatedLogArrays[self.nextLogArrayIndex]
	self.nextLogArrayIndex++
	return tf
}

func (self *StateDB) StateObjectCountInDelta() int {
	return len(self.delta.stateObjects)
}

func (self *StateDB) IsEnableFeeToCoinbase() bool {
	return self.EnableFeeToCoinbase
}

func (self *StateDB) SetBloomProcessor(bp *types.ParallelBloomProcessor) {
	self.BloomProcessor = bp
	if self.IsShared() {
		self.GetPair().BloomProcessor = bp
	}
}

const PRE_STATE_OBJECT_SLOTS = 400
const PRE_DIRTY_SLOTS = 100
const PRE_LOG_SLOTS = 1000
const PRE_TX_SLOTS = 500
const PRE_JOURNAL_ENTRY_SLOTS = 1000

// Create a new state from a given trie.
func New(root common.Hash, db Database) (*StateDB, error) {
	tr, err := db.OpenTrie(root)
	if err != nil {
		return nil, err
	}
	return &StateDB{
		db:                     db,
		trie:                   tr,
		stateObjects:           make(map[common.Address]*stateObject),
		stateObjectsPending:    make(map[common.Address]struct{}, 300),
		stateObjectsDirty:      make(map[common.Address]struct{}),
		createdObjectsInWarmup: make(map[common.Address]*stateObject),
		logs:                   make(map[common.Hash][]*types.Log, 300),
		preimages:              make(map[common.Hash][]byte),
		journal:                newJournal(),

		EnableFeeToCoinbase: true,
		allowObjCopy:        true,
		addrNotCopy:         make(map[common.Address]struct{}),
		rwRecorder:          emptyRWRecorder{}, // RWRecord mode default off
		ProcessedTxs:        []common.Hash{},
		AccountDeps:         make(cmptypes.AccountDepMap, 400),
	}, nil
}

func NewRWStateDB(state *StateDB) *StateDB {
	state.SetRWMode(true)
	state.EnableFeeToCoinbase = false // RWStateDB default is false
	return state
}

func (self *StateDB) IsAllowObjCopy() bool {
	return self.allowObjCopy
}

func (self *StateDB) SetAllowObjCopy(allow bool) {
	self.allowObjCopy = allow
}

func (self *StateDB) SetAddrNotCopy(addrMap map[common.Address]struct{}) {
	self.addrNotCopy = addrMap
}

func (self *StateDB) SetCopyForShare(copyForShare bool) {
	self.copyForShare = copyForShare
}

func (self *StateDB) SetRWMode(enabled bool) {
	if enabled {
		self.rwRecorder = NewRWHook(self)
		self.rwRecorderEnabled = true
	} else {
		self.rwRecorder = emptyRWRecorder{}
		self.rwRecorderEnabled = false
	}
}

// setError remembers the first non-nil error it is called with.
func (s *StateDB) setError(err error) {
	if s.dbErr == nil {
		s.dbErr = err
	}
}

func (s *StateDB) Error() error {
	return s.dbErr
}

func (self *StateDB) RWRecorder() RWRecorder {
	return self.rwRecorder
}

func (self *StateDB) IsRWMode() bool {
	return self.rwRecorderEnabled
	//switch self.rwRecorder.(type) {
	//case *rwRecorderImpl:
	//	return true
	//default:
	//	return false
	//}
}

// Reset clears out all ephemeral state objects from the state db, but keeps
// the underlying state trie to avoid reloading data for the next operations.
func (s *StateDB) Reset(root common.Hash) error {
	tr, err := s.db.OpenTrie(root)
	if err != nil {
		return err
	}
	s.trie = tr
	s.stateObjects = make(map[common.Address]*stateObject)
	s.stateObjectsPending = make(map[common.Address]struct{})
	s.stateObjectsDirty = make(map[common.Address]struct{})
	s.thash = common.Hash{}
	s.bhash = common.Hash{}
	s.txIndex = 0
	s.logs = make(map[common.Hash][]*types.Log)
	s.logSize = 0
	s.preimages = make(map[common.Hash][]byte)
	s.clearJournalAndRefund()
	return nil
}

func (s *StateDB) AddLog(log *types.Log) {
	if s.IgnoreJournalEntry {
		// no dirty account to add
	} else {
		s.journal.append(addLogChange{txhash: s.thash})
	}

	log.TxHash = s.thash
	log.BlockHash = s.bhash
	log.TxIndex = uint(s.txIndex)
	log.Index = s.logSize
	if s.IsShared() {
		s.delta.logs = append(s.delta.logs, log)
	} else {
		logs, ok := s.logs[s.thash]
		if !ok {
			logs = s.GetNewLogArray()
		}
		s.logs[s.thash] = append(logs, log)
		//s.logs[s.thash] = append(s.logs[s.thash], log)
	}
	s.logSize++
}

func (s *StateDB) GetLogs(hash common.Hash) []*types.Log {
	if s.IsShared() && hash == s.thash {
		return s.delta.logs
	} else {
		return s.logs[hash]
	}

}

func (s *StateDB) Logs() []*types.Log {
	var logs []*types.Log
	for _, lgs := range s.logs {
		logs = append(logs, lgs...)
	}
	if s.IsShared() {
		return append(logs, s.delta.logs...)
	}
	return logs
}

// AddPreimage records a SHA3 preimage seen by the VM.
func (s *StateDB) AddPreimage(hash common.Hash, preimage []byte) {
	if s.IsShared() {
		if _, ok := s.delta.preimages[hash]; ok {
			return
		}
	}
	if _, ok := s.preimages[hash]; !ok {
		if s.IgnoreJournalEntry {
			// no dirty address to add
		} else {
			s.journal.append(addPreimageChange{hash: hash})
		}
		pi := make([]byte, len(preimage))
		copy(pi, preimage)
		if s.IsShared() {
			s.delta.preimages[hash] = pi
		} else {
			s.preimages[hash] = pi
		}
	}
}

// Preimages returns a list of SHA3 preimages that have been submitted.
func (s *StateDB) Preimages() map[common.Hash][]byte {
	return s.preimages
}

// AddRefund adds gas to the refund counter
func (s *StateDB) AddRefund(gas uint64) {
	if s.IgnoreJournalEntry {
		// no dirty addr to add
	} else {
		s.journal.append(refundChange{prev: s.refund})
	}
	s.refund += gas
}

// SubRefund removes gas from the refund counter.
// This method will panic if the refund counter goes below zero
func (s *StateDB) SubRefund(gas uint64) {
	if s.IgnoreJournalEntry {
		// no dirty addr to add
	} else {
		s.journal.append(refundChange{prev: s.refund})
	}
	if gas > s.refund {
		panic("Refund counter below zero")
	}
	s.refund -= gas
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for suicided accounts.
func (s *StateDB) Exist(addr common.Address) bool {
	so := s.getStateObject(addr)
	ret := so != nil
	defer func() { s.rwRecorder._Exist(addr, so, ret) }()
	return ret
}

func (s *StateDB) OriginalExist(addr common.Address) bool {
	so := s.getStateObject(addr)
	ret := so != nil
	return ret
}

// Empty returns whether the state object is either non-existent
// or empty according to the EIP161 specification (balance = nonce = code = 0)
func (s *StateDB) Empty(addr common.Address) bool {
	so := s.getStateObject(addr)
	ret := so == nil || so.empty()
	defer func() { s.rwRecorder._Empty(addr, so, ret) }()
	return so == nil || so.empty()
}

func (s *StateDB) OriginalEmpty(addr common.Address) bool {
	so := s.getStateObject(addr)
	ret := so == nil || so.empty()
	return ret
}

// Retrieve the balance from the given address or 0 if object not found
func (s *StateDB) GetBalance(addr common.Address) (ret *big.Int) {
	stateObject := s.getStateObject(addr)
	if s.IsRWMode() {
		defer func() {
			s.rwRecorder._GetBalance(addr, stateObject, new(big.Int).Set(ret))
		}()
	}
	if stateObject != nil {
		return stateObject.Balance()
	}
	return common.Big0
}

func (s *StateDB) OriginalGetBalance(addr common.Address) (ret *big.Int) {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Balance()
	}
	return common.Big0
}

func (s *StateDB) GetNonce(addr common.Address) (ret uint64) {
	stateObject := s.getStateObject(addr)
	defer func() { s.rwRecorder._GetNonce(addr, stateObject, ret) }()
	if stateObject != nil {
		return stateObject.Nonce()
	}
	return 0
}

func (s *StateDB) OriginalGetNonce(addr common.Address) (ret uint64) {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Nonce()
	}
	return 0
}

// TxIndex returns the current transaction index set by Prepare.
func (s *StateDB) TxIndex() int {
	return s.txIndex
}

// BlockHash returns the current block hash set by Prepare.
func (s *StateDB) BlockHash() common.Hash {
	return s.bhash
}

func (s *StateDB) GetCode(addr common.Address) (ret []byte) {
	stateObject := s.getStateObject(addr)
	defer func() { s.rwRecorder._GetCode(addr, stateObject, ret) }()
	if stateObject != nil {
		return stateObject.Code(s.db)
	}
	return nil
}

func (s *StateDB) GetCodeSize(addr common.Address) (ret int) {
	stateObject := s.getStateObject(addr)
	defer func() { s.rwRecorder._GetCodeSize(addr, stateObject, ret) }()
	if stateObject == nil {
		return 0
	}
	if stateObject.code != nil {
		return len(stateObject.code)
	}
	size, err := s.db.ContractCodeSize(stateObject.addrHash, common.BytesToHash(stateObject.CodeHash()))
	if err != nil {
		s.setError(err)
	}
	return size
}

func (s *StateDB) OriginalGetCodeSize(addr common.Address) (ret int) {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return 0
	}
	if stateObject.code != nil {
		return len(stateObject.code)
	}
	size, err := s.db.ContractCodeSize(stateObject.addrHash, common.BytesToHash(stateObject.CodeHash()))
	if err != nil {
		s.setError(err)
	}
	return size
}

func (s *StateDB) GetCodeHash(addr common.Address) (ret common.Hash) {
	stateObject := s.getStateObject(addr)
	defer func() { s.rwRecorder._GetCodeHash(addr, stateObject, ret) }()
	if stateObject == nil {
		return common.Hash{}
	}
	return common.BytesToHash(stateObject.CodeHash())
}

func (s *StateDB) OriginalGetCodeHash(addr common.Address) (ret common.Hash) {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return common.Hash{}
	}
	return common.BytesToHash(stateObject.CodeHash())
}

// GetState retrieves a value from the given account's storage trie.
func (s *StateDB) GetState(addr common.Address, hash common.Hash) (ret common.Hash) {
	stateObject := s.getStateObject(addr)
	defer func() { s.rwRecorder._GetState(addr, hash, stateObject, ret) }()
	if stateObject != nil {
		if s.WarmupMissDetail {
			s.RecordKeyAccessed(addr, hash)
		}
		return stateObject.GetState(s.db, hash)
	}
	return common.Hash{}
}

// GetProof returns the MerkleProof for a given Account
func (s *StateDB) GetProof(a common.Address) ([][]byte, error) {
	var proof proofList
	err := s.trie.Prove(crypto.Keccak256(a.Bytes()), 0, &proof)
	return [][]byte(proof), err
}

// GetProof returns the StorageProof for given key
func (s *StateDB) GetStorageProof(a common.Address, key common.Hash) ([][]byte, error) {
	var proof proofList
	trie := s.StorageTrie(a)
	if trie == nil {
		return proof, errors.New("storage trie for requested address does not exist")
	}
	err := trie.Prove(crypto.Keccak256(key.Bytes()), 0, &proof)
	return [][]byte(proof), err
}

// GetCommittedState retrieves a value from the given account's committed storage trie.
func (s *StateDB) GetCommittedState(addr common.Address, hash common.Hash) (ret common.Hash) {
	stateObject := s.getStateObject(addr)
	defer func() { s.rwRecorder._GetCommittedState(addr, hash, stateObject, ret) }()
	if stateObject != nil {
		if s.WarmupMissDetail {
			s.RecordKeyAccessed(addr, hash)
		}
		return stateObject.GetCommittedState(s.db, hash)
	}
	return common.Hash{}
}

// IsInPending return whether a state object is in statePending
func (s *StateDB) IsInPending(addr common.Address) bool {
	_, ok := s.stateObjectsPending[addr]
	return ok
}

func (s *StateDB) DirtyAddress() []common.Address {
	var dirties map[common.Address]int
	if s.IsShared() {
		dirties = s.journal.dirties
	} else {
		dirties = s.savedDirties
	}
	var res []common.Address
	for addr := range dirties {
		res = append(res, addr)
	}
	return res
}

func (s *StateDB) ClearSavedDirties() {
	s.savedDirties = nil
	if s.IsShared() {
		s.pair.savedDirties = nil
	}
}

func (s *StateDB) UpdateAccountChangedBySlice(dirties common.Addresses, txRes *cmptypes.TxResID) {
	for _, addr := range dirties {
		s.UpdateAccountChanged(addr, txRes)
	}
}

func (s *StateDB) UpdateAccountChangedByMap(dirties ObjectMap, txRes *cmptypes.TxResID, coinbase *common.Address) {
	for addr := range dirties {
		s.UpdateAccountChanged(addr, txRes)
	}
	if coinbase != nil {
		s.UpdateAccountChanged(*coinbase, txRes)
	}
}

func (s *StateDB) UpdateAccountChangedByMap2(dirties cmptypes.TxResIDMap, txRes *cmptypes.TxResID, coinbase *common.Address) {
	for addr := range dirties {
		s.UpdateAccountChanged(addr, txRes)
	}
	if coinbase != nil {
		s.UpdateAccountChanged(*coinbase, txRes)
	}
}

func (s *StateDB) ApplyAccountChanged(changes cmptypes.TxResIDMap) {
	for addr := range changes {
		s.UpdateAccountChanged(addr, changes[addr])
	}
}

func (s *StateDB) UpdateAccountChanged(addr common.Address, txRes *cmptypes.TxResID) {
	s.AccountDeps[addr] = txRes

}

func (s *StateDB) GetTxDepByAccount(address common.Address) cmptypes.AccountDepValue {
	changed, ok := s.AccountDeps[address]
	if !ok {
		accSnap := s.GetAccountSnap(address)
		s.AccountDeps[address] = accSnap
		return accSnap
	}
	return changed
}

// return hash & IsSnap & IsNil
func (s *StateDB) GetAccountDepValue(address common.Address) (cmptypes.AccountDepValue, bool) {
	changed, ok := s.AccountDeps[address]

	if !ok {
		accSnap := s.GetAccountSnap(address)

		return accSnap, true
		//changed = cmptypes.NewChangedBy2(accSnap)
		//s.AccountDeps[address] = changed
	}

	return changed, false
}

//func (s *StateDB) GetTxDepsByAccounts(addresses []*common.Address) cmptypes.AccountDepMap {
//	res := make(map[common.Address]*cmptypes.ChangedBy)
//	for _, addr := range addresses {
//		res[*addr] = s.GetTxDepByAccount(*addr)
//	}
//	return res
//}

// Database retrieves the low level database supporting the lower level trie ops.
func (s *StateDB) Database() Database {
	return s.db
}

// StorageTrie returns the storage trie of an account.
// The return value is a copy and is nil for non-existent accounts.
func (s *StateDB) StorageTrie(addr common.Address) Trie {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return nil
	}
	cpy := stateObject.deepCopy(s)
	return cpy.updateTrie(s.db)
}

func (s *StateDB) HasSuicided(addr common.Address) bool {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.suicided
	}
	return false
}

/*
 * SETTERS
 */

// AddBalance adds amount to the account associated with addr.
func (s *StateDB) AddBalance(addr common.Address, amount *big.Int) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		if amount.Sign() != 0 {
			s.rwRecorder._AddBalance(addr, stateObject)
		}
		stateObject.AddBalance(amount)
	}
}

// SubBalance subtracts amount from the account associated with addr.
func (s *StateDB) SubBalance(addr common.Address, amount *big.Int) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		if amount.Sign() != 0 {
			s.rwRecorder._SubBalance(addr, stateObject)
		}
		stateObject.SubBalance(amount)
	}
}

func (s *StateDB) SetBalance(addr common.Address, amount *big.Int) {
	stateObject := s.GetOrNewStateObject(addr)
	// no hook for SetBalance
	if stateObject != nil {
		stateObject.SetBalance(amount)
	}
}

func (s *StateDB) SetNonce(addr common.Address, nonce uint64) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetNonce(nonce)
	}
}

func (s *StateDB) SetCode(addr common.Address, code []byte) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetCode(crypto.Keccak256Hash(code), code)
	}
}

func (s *StateDB) SetState(addr common.Address, key, value common.Hash) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetState(s.db, key, value)
	}
}

// SetStorage replaces the entire storage for the specified account with given
// storage. This function should only be used for debugging.
func (s *StateDB) SetStorage(addr common.Address, storage map[common.Hash]common.Hash) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetStorage(storage)
	}
}

// Suicide marks the given account as suicided.
// This clears the account balance.
//
// The account's state object is still available until the state is committed,
// getStateObject will return a non-nil account after Suicide.
func (s *StateDB) Suicide(addr common.Address) bool {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return false
	}
	if s.IgnoreJournalEntry {
		s.journal.addDirty(&addr)
	} else {
		s.journal.append(suicideChange{
			account:     &addr,
			prev:        stateObject.suicided,
			prevbalance: new(big.Int).Set(stateObject.Balance()),
		})
	}
	stateObject.markSuicided()
	stateObject.data.Balance = new(big.Int)

	return true
}

//
// Setting, updating & deleting state object methods.
//

// updateStateObject writes the given object to the trie.
func (s *StateDB) updateStateObjectWithHashedKeyAndEncodedData(obj *stateObject, objData []byte, objKey []byte) {
	// Track the amount of time wasted on updating the account from the trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountUpdates += time.Since(start) }(time.Now())
	}
	// Encode the account and update the account trie
	addr := obj.Address()

	data := objData
	if data == nil {
		var err error
		data, err = rlp.EncodeToBytes(obj)
		if err != nil {
			panic(fmt.Errorf("can't encode object at %x: %v", addr[:], err))
		}
	}

	if objKey == nil {
		s.setError(s.trie.TryUpdate(addr[:], data))
	} else {
		s.setError(s.trie.TryUpdateWithHashedKey(addr[:], objKey[:], data))
	}

}

func (s *StateDB) updateStateObject(obj *stateObject) {
	s.updateStateObjectWithHashedKeyAndEncodedData(obj, nil, nil)
}

func (s *StateDB) updateStateObjectsInBatch(keyCopyList, hexKeyList, valueList [][]byte, hashedKeyStringList []string) {
	// Track the amount of time wasted on updating the account from the trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountUpdates += time.Since(start) }(time.Now())
	}

	s.setError(s.trie.TryInsertInBatch(keyCopyList, hexKeyList, valueList, hashedKeyStringList))
}

// deleteStateObject removes the given object from the state trie.
func (s *StateDB) deleteStateObject(obj *stateObject) {
	// Track the amount of time wasted on deleting the account from the trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountUpdates += time.Since(start) }(time.Now())
	}
	// Delete the account from the trie
	addr := obj.Address()
	s.setError(s.trie.TryDelete(addr[:]))
}

// getStateObject retrieves a state object given by the address, returning nil if
// the object is not found or was deleted in this execution context. If you need
// to differentiate between non-existent/just-deleted, use getDeletedStateObject.
func (s *StateDB) getStateObject(addr common.Address) *stateObject {
	if s.PauseableCache != nil {
		s.PauseableCache.PauseForProcess()
	}
	if obj := s.getDeletedStateObject(addr); obj != nil && !obj.deleted {
		return obj
	}
	return nil
}

// getDeletedStateObject is similar to getStateObject, but instead of returning
// nil for a deleted state object, it returns the actual object with the deleted
// flag set. This is needed by the state journal to revert to the correct s-
// destructed object instead of wiping all knowledge about the state object.
func (s *StateDB) getDeletedStateObject(addr common.Address) *stateObject {
	// Prefer live objects if any is available

	if s.WarmupMissDetail {
		s.RecordAccountAccessed(addr)
	}

	if s.IsShared() {
		if obj := s.delta.stateObjects[addr]; obj != nil {
			return obj
		}
	} else {
		if obj := s.stateObjects[addr]; obj != nil {
			return obj
		}
		if s.InWarmupMode {
			if obj := s.createdObjectsInWarmup[addr]; obj != nil {
				return obj
			}
		}
	}
	// Track the amount of time wasted on loading the object from the database
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountReads += time.Since(start) }(time.Now())
	}

	// if the address is in the warmup cache for non-existent accounts,
	// no need to read the Trie again
	if _, ok := s.createdObjectsInWarmup[addr]; ok {
		//enc, _ := s.trie.TryGet(addr[:])
		//if len(enc) != 0 {
		//	panic("The enc should be zero!")
		//}
		return nil
	}

	// Load the object from the database
	enc, err := s.trie.TryGet(addr[:])
	if len(enc) == 0 {
		if s.InWarmupMode {
			newobj := newObject(s, addr, Account{})
			s.createdObjectsInWarmup[addr] = newobj
			return newobj
		}
		s.setError(err)
		if s.CalWarmupMiss {
			s.AddrWarmupMiss++
			s.AddrCreateWarmupMiss[addr] = struct{}{}
		}
		return nil
	}
	var data Account
	if err := rlp.DecodeBytes(enc, &data); err != nil {
		log.Error("Failed to decode state object", "addr", addr, "err", err)
		return nil
	}
	if s.CalWarmupMiss {
		s.AddrWarmupMiss++
		if s.WarmupMissDetail && !s.IsAddrWarmup(addr) {
			s.AddrNoWarmup++
		}
	}
	// Insert into the live set
	obj := newObject(s, addr, data)
	obj.snap = cmptypes.BytesToAccountSnap(enc)
	if s.IsShared() {
		obj.addDelta()
		s.setDeltaStateObject(obj)
	} else {
		s.setStateObject(obj)
	}
	return obj
}

func (s *StateDB) setStateObject(object *stateObject) {
	s.stateObjects[object.Address()] = object
}

func (self *StateDB) setDeltaStateObject(object *stateObject) {
	self.delta.stateObjects[object.Address()] = object
}

// Retrieve a state object or create a new state object if nil.
func (s *StateDB) GetOrNewStateObject(addr common.Address) *stateObject {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		stateObject, _ = s.createObject(addr)
	}
	return stateObject
}

// createObject creates a new state object. If there is an existing account with
// the given address, it is overwritten and returned as the second return value.
func (s *StateDB) createObject(addr common.Address) (newobj, prev *stateObject) {
	prev = s.getDeletedStateObject(addr) // Note, prev might have been deleted, we need that!

	// Todo: add support for new object warmup
	if prev == nil && !s.IsShared() {
		newobj = s.createdObjectsInWarmup[addr]
		delete(s.createdObjectsInWarmup, addr)
	}
	if newobj == nil {
		newobj = newObject(s, addr, Account{})
	}
	if s.IsShared() {
		newobj.addDelta()
	}
	newobj.setNonce(0) // sets the object to dirty
	if prev == nil {
		if s.IgnoreJournalEntry {
			s.journal.addDirty(&addr)
		} else {
			s.journal.append(createObjectChange{account: &addr})
		}
	} else {
		if s.IgnoreJournalEntry {
			// no dirty addr to add
		} else {
			s.journal.append(resetObjectChange{prev: prev, account: &addr})
		}
	}
	if s.IsShared() {
		s.setDeltaStateObject(newobj)
	} else {
		s.setStateObject(newobj)
	}
	if s.CalWarmupMiss {
		s.AccountCreate++
	}
	return newobj, prev
}

// CreateAccount explicitly creates a state object. If a state object with the address
// already exists the balance is carried over to the new account.
//
// CreateAccount is called during the EVM CREATE operation. The situation might arise that
// a contract does the following:
//
//   1. sends funds to sha(account ++ (nonce + 1))
//   2. tx_create(sha(account ++ nonce)) (note that this gets the address of 1)
//
// Carrying over the balance ensures that Ether doesn't disappear.
func (s *StateDB) CreateAccount(addr common.Address) {
	newObj, prev := s.createObject(addr)
	if prev != nil {
		newObj.setBalance(prev.data.Balance)
		// TODO when rwrecord mode is off
		// The following might not be necessary.
		// But let's copy the dirty count to be on the safe side.
		newObj.dirtyBalanceCount = prev.dirtyBalanceCount
	}
}

func (db *StateDB) ForEachStorage(addr common.Address, cb func(key, value common.Hash) bool) error {
	so := db.getStateObject(addr)
	if so == nil {
		return nil
	}
	it := trie.NewIterator(so.getTrie(db.db).NodeIterator(nil))

	for it.Next() {
		key := common.BytesToHash(db.trie.GetKey(it.Key))
		if value, dirty := so.dirtyStorage[key]; dirty {
			if !cb(key, value) {
				return nil
			}
			continue
		}

		if len(it.Value) > 0 {
			_, content, _, err := rlp.Split(it.Value)
			if err != nil {
				return err
			}
			if !cb(key, common.BytesToHash(content)) {
				return nil
			}
		}
	}
	return nil
}

// Copy creates a deep, independent copy of the state.
// Snapshots of the copied state cannot be applied to the copy.
func (s *StateDB) Copy() *StateDB {
	// Copy all the basic fields, initialize the memory ones
	state := &StateDB{
		db:                     s.db,
		trie:                   s.db.CopyTrie(s.trie),
		stateObjects:           make(map[common.Address]*stateObject, len(s.journal.dirties)),
		stateObjectsPending:    make(map[common.Address]struct{}, len(s.stateObjectsPending)),
		stateObjectsDirty:      make(map[common.Address]struct{}, len(s.journal.dirties)),
		createdObjectsInWarmup: make(map[common.Address]*stateObject),
		refund:                 s.refund,
		logs:                   make(map[common.Hash][]*types.Log, len(s.logs)),
		logSize:                s.logSize,
		preimages:              make(map[common.Hash][]byte, len(s.preimages)),
		journal:                newJournal(),
		ProcessedTxs:           make([]common.Hash, len(s.ProcessedTxs)),
		AccountDeps:            make(cmptypes.AccountDepMap, len(s.AccountDeps)),

		EnableFeeToCoinbase: s.EnableFeeToCoinbase,
		allowObjCopy:        s.allowObjCopy,
		copyForShare:        s.copyForShare,
		rwRecorder:          emptyRWRecorder{},
		IsParallelHasher:    s.IsParallelHasher,
	}
	if s.IsRWMode() {
		state.SetRWMode(true) // Copied RWStateDB resets record
	}
	// Copy the dirty states, logs, and preimages
	for addr := range s.journal.dirties {
		// As documented [here](https://github.com/ethereum/go-ethereum/pull/16485#issuecomment-380438527),
		// and in the Finalise-method, there is a case where an object is in the journal but not
		// in the stateObjects: OOG after touch on ripeMD prior to Byzantium. Thus, we need to check for
		// nil
		if object, exist := s.stateObjects[addr]; exist {
			// Even though the original object is dirty, we are not copying the journal,
			// so we need to make sure that anyside effect the journal would have caused
			// during a commit (or similar op) is already applied to the copy.
			state.stateObjects[addr] = object.deepCopy(state)

			state.stateObjectsDirty[addr] = struct{}{}   // Mark the copy dirty to force internal (code/state) commits
			state.stateObjectsPending[addr] = struct{}{} // Mark the copy pending to force external (account) commits
		}
	}
	// Above, we don't copy the actual journal. This means that if the copy is copied, the
	// loop above will be a no-op, since the copy's journal is empty.
	// Thus, here we iterate over stateObjects, to enable copies of copies
	for addr := range s.stateObjectsPending {
		if _, exist := state.stateObjects[addr]; !exist {
			state.stateObjects[addr] = s.stateObjects[addr].deepCopy(state)
		}
		state.stateObjectsPending[addr] = struct{}{}
	}
	for addr := range s.stateObjectsDirty {
		if _, exist := state.stateObjects[addr]; !exist {
			state.stateObjects[addr] = s.stateObjects[addr].deepCopy(state)
		}
		state.stateObjectsDirty[addr] = struct{}{}
	}
	for hash, logs := range s.logs {
		cpy := make([]*types.Log, len(logs))
		for i, l := range logs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		state.logs[hash] = cpy
	}
	for hash, preimage := range s.preimages {
		state.preimages[hash] = preimage
	}

	for index, txhash := range s.ProcessedTxs {
		state.ProcessedTxs[index] = txhash
	}

	for address, depValue := range s.AccountDeps {
		//state.AccountDeps[address] = depValue.Copy()
		state.AccountDeps[address] = depValue

	}

	return state
}

// ShareCopy copy a state db in a share way, consuming little time
func (s *StateDB) ShareCopy() {
	if s.IsShared() {
		return
	} else {
		s.primary = true
		s.delta = newDeltaDB()
	}
	statedb := &StateDB{
		db:                  s.db,
		trie:                s.db.CopyTrie(s.trie),
		stateObjectsPending: s.stateObjectsPending,
		logs:                s.logs,
		journal:             newJournal(),
		EnableFeeToCoinbase: s.EnableFeeToCoinbase,
		allowObjCopy:        s.allowObjCopy,
		copyForShare:        s.copyForShare,
		rwRecorder:          emptyRWRecorder{},
		primary:             false,
		delta:               newDeltaDB(),
		IsParallelHasher:    s.IsParallelHasher,
		BloomProcessor:      s.BloomProcessor,
	}
	if s.IsRWMode() {
		statedb.SetRWMode(true) // Copied RWStateDB resets record
	}
	s.pair, statedb.pair = statedb, s
}

func (s *StateDB) GetPair() *StateDB {
	return s.pair
}

func (s *StateDB) IsShared() bool {
	return s.delta != nil
}

func (s *StateDB) IsPrimary() bool {
	return s.delta != nil && s.primary
}

func (s *StateDB) IsSecondary() bool {
	return s.delta != nil && !s.primary
}

func (s *StateDB) Update() {
	if s.IsShared() && s.pair.IsShared() {
		s.updateObject(s.pair)
		s.updateLogs(s.pair)
		s.pair.logSize = s.logSize
		s.updatePreimages(s.pair)
		s.clearJournalAndRefund()
		s.pair.clearJournalAndRefund()
		s.pair.nextRevisionId = s.nextRevisionId
		s.updateMeasurements(s.pair)
	}
}

func (s *StateDB) GetAccountSnap(address common.Address) *cmptypes.AccountSnap {
	obj := s.getStateObject(address)
	if obj != nil && obj.snap != nil {
		return obj.snap
	}

	snap, err := s.trie.TryGet(address[:])
	if err != nil { // this address does not exist
		panic(fmt.Sprintf("TryGet Error %v at Tx %v and addr %v", err.Error(), s.thash.Hex(), address.Hex()))
		//return &cmptypes.AccountSnap{}
	}
	if len(snap) != 0 {
		//preTxs, _:= json.Marshal(s.ProcessedTxs)
		//log.Warn("deleted?", "preTxs", string(preTxs))
		return cmptypes.BytesToAccountSnap(snap)
		//panic(fmt.Sprintf("TryGet return unexpected value at Tx %v and addr %v: snap: %v", s.thash.Hex(), address.Hex(), cmptypes.BytesToAccountSnap(snap).Hex()))
	}

	return cmptypes.EmptyAccountSnap
}

// return Snap & Source (0 for prepared StateObject; 1 for LRU; 2 for Trie.TryGet)
func (s *StateDB) GetAccountSnapByCache(addr common.Address, snapCache *lru.Cache, pbhash common.Hash, isBlockProcess bool) (*cmptypes.AccountSnap, int) {

	// Prefer live objects if any is available
	if obj := s.stateObjects[addr]; obj != nil { //obj != nil && !obj.deleted{
		if obj.deleted {
			//log.Warn("stateObject deleted")
			panic("unexpected deleted stateObject")
		}
		if obj.snap == nil {
			panic("snap should not be nil")
		}
		return obj.snap, 0
	}

	// if the address is in the warmup cache for non-existent accounts,
	// no need to read the Trie again
	if _, ok := s.createdObjectsInWarmup[addr]; ok {
		//enc, _ := s.trie.TryGet(addr[:])
		//if len(enc) != 0 {
		//	panic("The enc should be zero!")
		//}
		return cmptypes.EmptyAccountSnap, 0
	}

	//LRU
	if snapCache != nil {
		var value interface{}
		var ok bool
		if isBlockProcess {
			value, ok = snapCache.Peek(addr)
		} else {
			value, ok = snapCache.Get(addr)
		}
		if ok {
			swb, _ := value.(*cmptypes.SnapWithBlockHash)
			if swb.ParentBlockhash != nil && *swb.ParentBlockhash == pbhash {

				enc := swb.Snap.Bytes()
				if len(enc) != 0 {

					var data Account
					if err := rlp.DecodeBytes(enc, &data); err != nil {
						log.Error("Failed to decode state object", "addr", addr, "err", err)
						panic("decode failed")
					}

					// Insert into the live set
					obj := newObject(s, addr, data)
					obj.snap = cmptypes.BytesToAccountSnap(enc)
					if s.IsShared() {
						obj.addDelta()
						s.setDeltaStateObject(obj)
					} else {
						s.setStateObject(obj)
					}
				}

				return swb.Snap, 1
			}
		}
	}

	return s.GetAccountSnap(addr), 2

}

func (s *StateDB) TrieGet(address common.Address) ([]byte, error) {
	return s.trie.TryGet(address[:])
}

func (s *StateDB) updateObject(db *StateDB) {
	for addr := range db.journal.dirties {
		if _, ok := s.journal.dirties[addr]; ok {
			continue
		}
		object1, exist := s.delta.stateObjects[addr]
		if !exist {
			delete(db.delta.stateObjects, addr)
			continue
		}
		if object2, ok := db.delta.stateObjects[addr]; ok {
			if object1.pair == nil || object1.pair != object2 {
				s.linkObject(object1, object2)
			}
			object1.updatePair()
		}
	}
	for addr := range s.journal.dirties {
		object1, exist := s.delta.stateObjects[addr]
		if !exist {
			continue
		}
		if object2, ok := db.delta.stateObjects[addr]; ok {
			if object1.pair == nil {
				s.linkObject(object1, object2)
			}
			object1.updatePair()
			if object1.pair != object2 {
				db.setDeltaStateObject(object1.pair)
			}
		} else {
			if object1.pair == nil {
				object1.shareCopy(db)
			} else {
				object1.updatePair()
			}
			db.setDeltaStateObject(object1.pair)
		}
		object1.UpdateDelta()
		db.delta.stateObjectsDirty[addr] = struct{}{}
	}
}

func (s *StateDB) linkObject(object1, object2 *stateObject) {
	object1.pair, object2.pair = object2, object1
	object2.originStorage = object1.originStorage
	object2.pendingStorage = object1.pendingStorage
}

func (s *StateDB) updateLogs(db *StateDB) {
	if len(s.delta.logs) > 0 {
		s.logs[s.thash] = s.delta.logs
		s.delta.logs = nil
		db.delta.logs = nil
	}
	if len(db.delta.logs) > 0 {
		db.delta.logs = nil
	}
}

func (s *StateDB) updatePreimages(db *StateDB) {
	for hash, preimage1 := range s.delta.preimages {
		s.preimages[hash] = preimage1
	}
	if len(s.delta.preimages) > 0 {
		s.delta.preimages = make(map[common.Hash][]byte)
	}
	if len(db.delta.preimages) > 0 {
		db.delta.preimages = make(map[common.Hash][]byte)
	}
}

func (s *StateDB) updateMeasurements(db *StateDB) {
	db.AccountReads = s.AccountReads
	db.AccountHashes = s.AccountHashes
	db.AccountUpdates = s.AccountUpdates
	db.AccountCommits = s.AccountCommits
	db.StorageReads = s.StorageReads
	db.StorageHashes = s.StorageHashes
	db.StorageUpdates = s.StorageUpdates
	db.StorageCommits = s.StorageCommits
}

func (s *StateDB) MergeDelta() {
	s.stateObjects = s.delta.stateObjects
	for _, object := range s.stateObjects {
		object.delta = nil
	}
	s.stateObjectsDirty = s.delta.stateObjectsDirty
	s.preimages = s.delta.preimages
	s.pair = nil
	s.delta = nil
}

// Snapshot returns an identifier for the current revision of the state.
func (s *StateDB) Snapshot() int {
	id := s.nextRevisionId
	s.nextRevisionId++
	s.validRevisions = append(s.validRevisions, revision{id, s.journal.length()})
	return id
}

// RevertToSnapshot reverts all state changes made since the given revision.
func (s *StateDB) RevertToSnapshot(revid int) {
	// Find the snapshot in the stack of valid snapshots.
	idx := sort.Search(len(s.validRevisions), func(i int) bool {
		return s.validRevisions[i].id >= revid
	})
	if idx == len(s.validRevisions) || s.validRevisions[idx].id != revid {
		panic(fmt.Errorf("revision id %v cannot be reverted", revid))
	}
	snapshot := s.validRevisions[idx].journalIndex

	// Replay the journal to undo changes and remove invalidated snapshots
	s.journal.revert(s, snapshot)
	s.validRevisions = s.validRevisions[:idx]
}

// GetRefund returns the current value of the refund counter.
func (s *StateDB) GetRefund() uint64 {
	return s.refund
}

// Finalise finalises the state by removing the s destructed objects and clears
// the journal as well as the refunds. Finalise, however, will not push any updates
// into the tries just yet. Only IntermediateRoot or Commit will do that.
func (s *StateDB) Finalise(deleteEmptyObjects bool) {
	isShared := s.IsShared()
	for addr := range s.journal.dirties {
		var (
			obj   *stateObject
			exist bool
		)
		if isShared {
			obj, exist = s.delta.stateObjects[addr]
		} else {
			obj, exist = s.stateObjects[addr]
		}
		if !exist {
			// ripeMD is 'touched' at block 1714175, in tx 0x1237f737031e40bcde4a8b7e717b2d15e3ecadfe49bb1bbc71ee9deb09c6fcf2
			// That tx goes out of gas, and although the notion of 'touched' does not exist there, the
			// touch-event will still be recorded in the journal. Since ripeMD is a special snowflake,
			// it will persist in the journal even though the journal is reverted. In this special circumstance,
			// it may exist in `s.journal.dirties` but not in `s.stateObjects`.
			// Thus, we can safely ignore it here
			continue
		}
		if obj.suicided || (deleteEmptyObjects && obj.empty()) {
			if obj.suicided {
				s.rwRecorder.UpdateSuicide(addr)
			}
			obj.deleted = true
		} else {
			s.rwRecorder.UpdateDirtyStateObject(obj)
			obj.finalise()
		}
		s.rwRecorder.UpdateWObject(addr, obj)
		s.stateObjectsPending[addr] = struct{}{}
		if isShared {
			s.delta.stateObjectsDirty[addr] = struct{}{}
		} else {
			s.stateObjectsDirty[addr] = struct{}{}
		}
	}
	if !isShared {
		// Invalidate journal because reverting across transactions is not allowed.
		s.clearJournalAndRefund()
	}
}

// IntermediateRoot computes the current root hash of the state trie.
// It is called in between transactions to get the root hash that
// goes into transaction receipts.
func (s *StateDB) IntermediateRoot(deleteEmptyObjects bool) common.Hash {
	// Finalise all the dirty storage states and write them into the tries
	s.Finalise(deleteEmptyObjects)

	var start time.Time
	if s.IsParallelHasher {
		toDelete := make([]*stateObject, 0, len(s.stateObjectsPending))
		toUpdateRoot := make([]*stateObject, 0, len(s.stateObjectsPending))
		for addr := range s.stateObjectsPending {
			obj := s.stateObjects[addr]
			if obj == nil && s.IsShared() {
				obj = s.delta.stateObjects[addr]
			}
			if obj.deleted {
				toDelete = append(toDelete, obj)
			} else {
				toUpdateRoot = append(toUpdateRoot, obj)
			}
		}
		var allJobs sync.WaitGroup
		// use a separate goroutine to execute all deletes serially
		if len(toDelete) > 0 {
			allJobs.Add(1)
			go func() {
				for _, obj := range toDelete {
					s.deleteStateObject(obj)
				}
				allJobs.Done()
			}()
		}

		encodedData := make([][]byte, len(toUpdateRoot))
		//hashedKeys := make([][]byte, len(toUpdateRoot))
		hexKeys := make([][]byte, len(toUpdateRoot))
		hashedStrings := make([]string, len(toUpdateRoot))
		keyCopies := make([][]byte, len(toUpdateRoot))

		// execute the updateRoots in parallel
		if len(toUpdateRoot) > 0 {
			for i, obj := range toUpdateRoot {
				allJobs.Add(1)
				theObject := obj // avoid capturing the loop variable
				theIndex := i
				job := func() {
					theObject.updateRoot(s.db)
					data, err := rlp.EncodeToBytes(theObject)
					if err != nil {
						panic("Encoding of theObject should never fail!")
					}
					encodedData[theIndex] = data
					hashedKey := theObject.trie.HashKey(theObject.Address().Bytes())
					hexKeys[theIndex] = trie.KeybytesToHex(hashedKey)
					hashedStrings[theIndex] = string(hashedKey)
					keyCopies[theIndex] = common.CopyBytes(theObject.Address().Bytes())
					allJobs.Done()
				}
				trie.ExecuteInParallelPool(job)
			}
		}

		start = time.Now()
		allJobs.Wait()
		s.waitUpdateRoot = time.Since(start)
		start = time.Now()
		// this has to be done in serial
		//for i, obj := range toUpdateRoot {
		//	//s.updateStateObject(obj)
		//	s.updateStateObjectWithHashedKeyAndEncodedData(obj, encodedData[i], hashedKeys[i])
		//}
		s.updateStateObjectsInBatch(keyCopies, hexKeys, encodedData, hashedStrings)
		s.updateObj = time.Since(start)
	} else {
		for addr := range s.stateObjectsPending {
			obj := s.stateObjects[addr]
			if obj.deleted {
				s.deleteStateObject(obj)
			} else {
				obj.updateRoot(s.db)
				s.updateStateObject(obj)
			}
		}
	}
	if len(s.stateObjectsPending) > 0 {
		s.stateObjectsPending = make(map[common.Address]struct{})
	}
	// Track the amount of time wasted on hashing the account trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountHashes += time.Since(start) }(time.Now())
	}

	if s.IsParallelHasher {
		s.trie.UseParallelHasher(true)
		defer s.trie.UseParallelHasher(false)

		start = time.Now()
		defer func() {
			s.hashTrie = time.Since(start)
		}()
	}

	return s.trie.Hash()
}

func (s *StateDB) DumpFinalizeDetail() (waitUpdateRoot, updateObj, hashTrie time.Duration) {
	waitUpdateRoot, s.waitUpdateRoot = s.waitUpdateRoot, 0
	updateObj, s.updateObj = s.updateObj, 0
	hashTrie, s.hashTrie = s.hashTrie, 0
	return
}

// Prepare sets the current transaction hash and index and block hash which is
// used when the EVM emits new state logs.
func (s *StateDB) Prepare(thash, bhash common.Hash, ti int) {
	s.thash = thash
	s.bhash = bhash
	s.txIndex = ti
	if s.pair != nil {
		s.pair.thash = thash
		s.pair.bhash = bhash
		s.pair.txIndex = ti
	}
	s.ProcessedTxs = append(s.ProcessedTxs, thash)
}

func (s *StateDB) clearJournalAndRefund() {
	if len(s.journal.entries) > 0 || len(s.journal.dirties) > 0 {
		s.savedDirties = s.journal.dirties
		s.journal = s.GetNewJournal() //newJournal()
		s.refund = 0
	}
	s.validRevisions = s.validRevisions[:0] // Snapshots can be created without journal entires
}

// Commit writes the state to the underlying in-memory trie database.
func (s *StateDB) Commit(deleteEmptyObjects bool) (common.Hash, error) {
	// Finalize any pending changes and merge everything into the tries
	s.IntermediateRoot(deleteEmptyObjects)

	// Commit objects to the trie, measuring the elapsed time
	if s.IsParallelHasher {
		var allJobs sync.WaitGroup

		allJobs.Add(1)
		// execute insert code in a separate thread
		go func() {
			for addr := range s.stateObjectsDirty {
				if obj := s.stateObjects[addr]; !obj.deleted {
					// Write any contract code associated with the state object
					if obj.code != nil && obj.dirtyCode {
						s.db.TrieDB().InsertBlob(common.BytesToHash(obj.CodeHash()), obj.code)
						obj.dirtyCode = false
					}
				}
			}
			allJobs.Done()
		}()

		// execute commitTrie in parallel
		for addr := range s.stateObjectsDirty {
			if obj := s.stateObjects[addr]; !obj.deleted {
				allJobs.Add(1)
				job := func() {
					// Write any storage changes in the state object to its storage trie
					if err := obj.CommitTrie(s.db); err != nil {
						panic("Should never have CommitTrie error!")
					}
					allJobs.Done()
				}
				trie.ExecuteInParallelPool(job)
			}
		}
		allJobs.Wait()

	} else {
		for addr := range s.stateObjectsDirty {
			if obj := s.stateObjects[addr]; !obj.deleted {
				// Write any contract code associated with the state object
				if obj.code != nil && obj.dirtyCode {
					s.db.TrieDB().InsertBlob(common.BytesToHash(obj.CodeHash()), obj.code)
					obj.dirtyCode = false
				}
				// Write any storage changes in the state object to its storage trie
				if err := obj.CommitTrie(s.db); err != nil {
					return common.Hash{}, err
				}
			}
		}
	}
	if len(s.stateObjectsDirty) > 0 {
		s.stateObjectsDirty = make(map[common.Address]struct{})
	}
	// Write the account trie changes, measuing the amount of wasted time
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountCommits += time.Since(start) }(time.Now())
	}

	if s.IsParallelHasher {
		s.trie.UseParallelHasher(true)
		defer s.trie.UseParallelHasher(false)
	}

	return s.trie.Commit(func(leaf []byte, parent common.Hash) error {
		var account Account
		if err := rlp.DecodeBytes(leaf, &account); err != nil {
			return nil
		}
		if account.Root != emptyRoot {
			s.db.TrieDB().Reference(account.Root, parent)
		}
		code := common.BytesToHash(account.CodeHash)
		if code != emptyCode {
			s.db.TrieDB().Reference(code, parent)
		}
		return nil
	})
}

func (s *StateDB) ApplyStateObject(object *stateObject) {
	object.db = s
	if s.IsShared() {
		object.pair.db = s.pair
		s.setDeltaStateObject(object)
	} else {
		s.setStateObject(object)
	}
	s.journal.dirties[object.address]++
}

func (s *StateDB) IsAddrWarmup(addr common.Address) bool {
	_, ok1 := s.ProcessedForDb[addr]
	_, ok2 := s.ProcessedForObj[addr]
	return ok1 || ok2
}

func (s *StateDB) IsKeyWarmup(addr common.Address, key common.Hash) bool {
	_, ok1 := s.ProcessedForDb[addr]
	if ok1 {
		_, ok1 = s.ProcessedForDb[addr][key]
	}
	_, ok2 := s.ProcessedForObj[addr]
	if ok2 {
		_, ok2 = s.ProcessedForObj[addr][key]
	}
	return ok1 || ok2
}

func (s *StateDB) HaveMiss() bool {
	return s.AddrWarmupMiss > 0 || s.KeyWarmupMiss > 0
}

func (s *StateDB) RecordAccountAccessed(addr common.Address) {
	if _, ok := s.AccessedAccountAndKeys[addr]; !ok {
		s.AccessedAccountAndKeys[addr] = make(map[common.Hash]struct{})
	}
}

func (s *StateDB) RecordKeyAccessed(addr common.Address, key common.Hash) {
	aMap, ok := s.AccessedAccountAndKeys[addr]
	if !ok {
		panic("Account Should always be accessed before keys")
	}
	aMap[key] = struct{}{}
}

func (s *StateDB) ClearMiss() {
	s.AccountCreate = 0
	s.AddrWarmupMiss, s.KeyWarmupMiss = 0, 0
	s.AddrNoWarmup, s.KeyNoWarmup = 0, 0
	s.AddrCreateWarmupMiss, s.KeyCreateWarmupMiss = make(map[common.Address]struct{}), 0
}

func IntermediateRootCalc(s *StateDB) common.Hash {
	for addr := range s.stateObjectsPending {
		obj := s.stateObjects[addr]
		if obj.deleted {
			s.deleteStateObject(obj)
		} else {
			obj.updateRoot(s.db)
			s.updateStateObject(obj)
		}
	}
	if len(s.stateObjectsPending) > 0 {
		s.stateObjectsPending = make(map[common.Address]struct{})
	}
	// Track the amount of time wasted on hashing the account trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountHashes += time.Since(start) }(time.Now())
	}
	return s.trie.Hash()
}
