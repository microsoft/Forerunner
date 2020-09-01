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

package state

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"io"
	"math/big"
	"time"
)

var emptyCodeHash = crypto.Keccak256(nil)

type Code []byte

func (c Code) String() string {
	return string(c) //strings.Join(Disassemble(c), " ")
}

type Storage map[common.Hash]common.Hash

func (s Storage) String() (str string) {
	for key, value := range s {
		str += fmt.Sprintf("%X : %X\n", key, value)
	}

	return
}

func (s Storage) Copy() Storage {
	cpy := make(Storage)
	for key, value := range s {
		cpy[key] = value
	}

	return cpy
}

type deltaObject struct {
	originStorage  Storage
	pendingStorage Storage
}

func newDeltaObject() *deltaObject {
	return &deltaObject{
		originStorage:  make(Storage),
		pendingStorage: make(Storage, 100),
	}
}

const stateObjectLen = 10

type ObjectMap map[common.Address]*stateObject

type ObjectHolder struct {
	Obj   *stateObject
	ObjID uintptr
}

type ObjectHolderList []*ObjectHolder

func NewObjectHolder(obj *stateObject, objID uintptr) *ObjectHolder {
	return &ObjectHolder{
		Obj:   obj,
		ObjID: objID,
	}
}

type ObjectHolderMap map[uintptr]*ObjectHolder

func (hm ObjectHolderMap) GetAndDelete(key uintptr) (holder *ObjectHolder, hok bool) {
	if holder, hok = hm[key]; hok {
		delete(hm, key)
	}
	return
}

// stateObject represents an Ethereum account which is being modified.
//
// The usage pattern is as follows:
// First you need to obtain a state object.
// Account values can be accessed and modified through the object.
// Finally, call CommitTrie to write the modified storage trie into a database.
type stateObject struct {
	address  common.Address
	addrHash common.Hash // hash of ethereum address of the account
	data     Account
	db       *StateDB

	// DB error.
	// State objects are used by the consensus core and VM which are
	// unable to deal with database-level errors. Any error that occurs
	// during a database read is memoized here and will eventually be returned
	// by StateDB.Commit.
	dbErr error

	// Write caches.
	trie Trie // storage trie, which becomes non-nil on first access
	code Code // contract bytecode, which gets set when code is loaded

	originStorage  Storage // Storage cache of original entries to dedup rewrites, reset for every transaction
	pendingStorage Storage // Storage entries that need to be flushed to disk, at the end of an entire block
	dirtyStorage   Storage // Storage entries that have been modified in the current transaction execution
	fakeStorage    Storage // Fake storage which constructed by caller for debugging purpose.

	// Cache flags.
	// When an object is marked suicided it will be delete from the trie
	// during the "update" phase of the state transition.
	dirtyCode bool // true if the code was updated
	suicided  bool
	deleted   bool

	dirtyNonceCount   uint
	dirtyBalanceCount uint
	dirtyCodeCount    uint
	dirtyStorageCount map[common.Hash]uint

	delta *deltaObject
	pair  *stateObject

	snap *cmptypes.AccountSnap

	isNewlyCreated bool
}

// empty returns whether the account is considered empty.
func (s *stateObject) empty() bool {
	return s.data.Nonce == 0 && s.data.Balance.Sign() == 0 && bytes.Equal(s.data.CodeHash, emptyCodeHash)
}

// Account is the Ethereum consensus representation of accounts.
// These objects are stored in the main account trie.
type Account struct {
	Nonce    uint64
	Balance  *big.Int
	Root     common.Hash // merkle root of the storage trie
	CodeHash []byte
}

func (a Account) Copy() Account {
	return Account{
		Nonce:    a.Nonce,
		Balance:  new(big.Int).Set(a.Balance),
		Root:     a.Root,
		CodeHash: a.CodeHash,
	}
}

// newObject creates a state object.
func newObject(db *StateDB, address common.Address, data Account) *stateObject {
	if data.Balance == nil {
		data.Balance = new(big.Int)
	}
	newlyCreated := false
	if data.CodeHash == nil {
		data.CodeHash = emptyCodeHash
		newlyCreated = true
	}
	if data.Root == (common.Hash{}) {
		data.Root = emptyRoot
	}
	return &stateObject{
		db:                db,
		address:           address,
		addrHash:          crypto.Keccak256Hash(address[:]),
		data:              data,
		originStorage:     make(Storage),
		pendingStorage:    make(Storage),
		dirtyStorage:      make(Storage),
		dirtyStorageCount: make(map[common.Hash]uint),
		isNewlyCreated:    newlyCreated,
	}
}

// EncodeRLP implements rlp.Encoder.
func (s *stateObject) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, s.data)
}

// setError remembers the first non-nil error it is called with.
func (s *stateObject) setError(err error) {
	if s.dbErr == nil {
		s.dbErr = err
	}
}

func (s *stateObject) markSuicided() {
	s.suicided = true
}

func (s *stateObject) touch() {
	if s.db.IgnoreJournalEntry {
		s.db.journal.addDirty(&s.address)
	} else {
		s.db.journal.append(touchChange{
			account: &s.address,
		})
	}
	if s.address == ripemd {
		// Explicitly put it in the dirty-cache, which is otherwise generated from
		// flattened journals.
		s.db.journal.dirty(s.address)
	}
}

func (s *stateObject) GetDatabase() Database {
	if s.db == nil {
		return nil
	}
	return s.db.db
}

func (s *stateObject) getTrie(db Database) Trie {
	if s.trie == nil {
		var err error
		s.trie, err = db.OpenStorageTrie(s.addrHash, s.data.Root)
		if err != nil {
			s.trie, _ = db.OpenStorageTrie(s.addrHash, common.Hash{})
			s.setError(fmt.Errorf("can't create storage trie: %v", err))
		}
	}
	return s.trie
}

// GetState retrieves a value from the account storage trie.
func (s *stateObject) GetState(db Database, key common.Hash) common.Hash {
	// If the fake storage is set, only lookup the state here(in the debugging mode)
	if s.fakeStorage != nil {
		return s.fakeStorage[key]
	}
	// If we have a dirty value for this state entry, return it
	value, dirty := s.dirtyStorage[key]
	if dirty {
		return value
	}
	// Otherwise return the entry's original value
	return s.GetCommittedState(db, key)
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (s *stateObject) GetCommittedState(db Database, key common.Hash) common.Hash {
	// If the fake storage is set, only lookup the state here(in the debugging mode)
	if s.fakeStorage != nil {
		return s.fakeStorage[key]
	}
	// If we have a pending write or clean cached, return that
	if value, pending := s.pendingStorage[key]; pending {
		return value
	}
	if value, cached := s.originStorage[key]; cached {
		return value
	}
	if s.isShared() {
		if value, cached := s.delta.originStorage[key]; cached {
			return value
		}
	}
	// Track the amount of time wasted on reading the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageReads += time.Since(start) }(time.Now())
	}
	// Otherwise load the value from the database
	enc, err := s.getTrie(db).TryGet(key[:])
	if err != nil {
		s.setError(err)
		return common.Hash{}
	}
	var value common.Hash
	if len(enc) > 0 {
		_, content, _, err := rlp.Split(enc)
		if err != nil {
			s.setError(err)
		}
		value.SetBytes(content)
	}
	if s.db.CalWarmupMiss {
		s.db.KeyWarmupMiss++
		if _, ok := s.db.AddrCreateWarmupMiss[s.address]; ok {
			s.db.KeyCreateWarmupMiss++
		}
		if s.db.WarmupMissDetail && !s.db.IsKeyWarmup(s.address, key) {
			s.db.KeyNoWarmup++
		}
	}
	if s.isShared() {
		s.delta.originStorage[key] = value
	} else {
		s.originStorage[key] = value
	}
	return value
}

// GetOriginStorage retrieves the whole origin storage map
// The return storage is read-only!!
func (s *stateObject) GetOriginStorage() Storage {
	return s.originStorage
}

func (s *stateObject) GetPendingStorage() Storage {
	return s.pendingStorage
}

// SetState updates a value in account storage.
func (s *stateObject) SetState(db Database, key, value common.Hash) {
	// If the fake storage is set, put the temporary state update here.
	if s.fakeStorage != nil {
		s.fakeStorage[key] = value
		return
	}
	// If the new value is the same as old, don't set
	prev := s.GetState(db, key)

	// For recording the full write set in a corner case, we disable this same-value optimization
	// The corner case is that, a storage item is written before it is read.
	// Even if the new value happens to be the same as the old value, we have to include it in the write set.

	//if prev == value {
	//	return
	//}

	if s.db.IgnoreJournalEntry {
		s.db.journal.addDirty(&s.address)
	} else {
		// New value is different, update and journal the change
		s.db.journal.append(storageChange{
			account:    &s.address,
			key:        key,
			prevalue:   prev,
			aftervalue: value,
		})
	}
	if s.db.IsRWMode() {
		s.dirtyStorageCount[key]++
	}
	s.setState(key, value)
}

// SetStorage replaces the entire state storage with the given one.
//
// After this function is called, all original state will be ignored and state
// lookup only happens in the fake state storage.
//
// Note this function should only be used for debugging purpose.
func (s *stateObject) SetStorage(storage map[common.Hash]common.Hash) {
	// Allocate fake storage if it's nil.
	if s.fakeStorage == nil {
		s.fakeStorage = make(Storage)
	}
	for key, value := range storage {
		s.fakeStorage[key] = value
	}
	// Don't bother journal since this function should only be used for
	// debugging and the `fake` storage won't be committed to database.
}

func (s *stateObject) setState(key, value common.Hash) {
	s.dirtyStorage[key] = value
}

// finalise moves all dirty storage slots into the pending area to be hashed or
// committed later. It is invoked at the end of every transaction.
func (s *stateObject) finalise() {
	isShared := s.isShared()
	for key, value := range s.dirtyStorage {
		if isShared {
			s.delta.pendingStorage[key] = value
		} else {
			s.pendingStorage[key] = value
		}
	}
	if len(s.dirtyStorage) > 0 {
		s.dirtyStorage = make(Storage)
	}
	s.isNewlyCreated = false
}

// updateTrie writes cached storage modifications into the object's storage trie.
func (s *stateObject) updateTrie(db Database) Trie {
	// Make sure all dirty slots are finalized into the pending storage area
	s.finalise()

	// Track the amount of time wasted on updating the storge trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageUpdates += time.Since(start) }(time.Now())
	}
	// Insert all the pending updates into the trie
	tr := s.getTrie(db)
	for key, value := range s.pendingStorage {
		// Skip noop changes, persist actual changes
		if value == s.originStorage[key] {
			continue
		}
		s.originStorage[key] = value

		if (value == common.Hash{}) {
			s.setError(tr.TryDelete(key[:]))
			continue
		}
		// Encoding []byte cannot fail, ok to ignore the error.
		v, _ := rlp.EncodeToBytes(common.TrimLeftZeroes(value[:]))
		s.setError(tr.TryUpdate(key[:], v))
	}
	if len(s.pendingStorage) > 0 {
		s.pendingStorage = make(Storage)
	}
	return tr
}

// UpdateRoot sets the trie root to the current root hash of
func (s *stateObject) updateRoot(db Database) {
	s.updateTrie(db)

	// Track the amount of time wasted on hashing the storge trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageHashes += time.Since(start) }(time.Now())
	}

	//if s.db.IsParallelHasher {
	//	s.trie.UseParallelHasher(true)
	//	defer s.trie.UseParallelHasher(false)
	//}

	s.data.Root = s.trie.Hash()
}

// CommitTrie the storage trie of the object to db.
// This updates the trie root.
func (s *stateObject) CommitTrie(db Database) error {
	s.updateTrie(db)
	if s.dbErr != nil {
		return s.dbErr
	}
	// Track the amount of time wasted on committing the storge trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageCommits += time.Since(start) }(time.Now())
	}

	//if s.db.IsParallelHasher {
	//	s.trie.UseParallelHasher(true)
	//	defer s.trie.UseParallelHasher(false)
	//}
	root, err := s.trie.Commit(nil)
	if err == nil {
		s.data.Root = root
	}
	return err
}

// AddBalance removes amount from c's balance.
// It is used to add funds to the destination account of a transfer.
func (s *stateObject) AddBalance(amount *big.Int) {
	// EIP158: We must check emptiness for the objects such that the account
	// clearing (0,0,0 objects) can take effect.
	if amount.Sign() == 0 {
		if s.empty() {
			s.touch()
		}

		return
	}
	s.SetBalance(new(big.Int).Add(s.Balance(), amount))
}

// SubBalance removes amount from c's balance.
// It is used to remove funds from the origin account of a transfer.
func (s *stateObject) SubBalance(amount *big.Int) {
	if amount.Sign() == 0 {
		return
	}
	s.SetBalance(new(big.Int).Sub(s.Balance(), amount))
}

func (s *stateObject) SetBalance(amount *big.Int) {
	if s.db.IgnoreJournalEntry {
		s.db.journal.addDirty(&s.address)
	} else {
		s.db.journal.append(balanceChange{
			account: &s.address,
			prev:    new(big.Int).Set(s.data.Balance),
			after:   new(big.Int).Set(amount),
		})
	}
	s.dirtyBalanceCount++
	s.setBalance(amount)
}

func (s *stateObject) setBalance(amount *big.Int) {
	s.data.Balance = amount
}

// Return the gas back to the origin. Used by the Virtual machine or Closures
func (s *stateObject) ReturnGas(gas *big.Int) {}

func (s *stateObject) deepCopy(db *StateDB) *stateObject {
	stateObject := newObject(db, s.address, s.data)
	if s.trie != nil {
		stateObject.trie = db.db.CopyTrie(s.trie)
	}
	stateObject.code = s.code
	stateObject.dirtyStorage = s.dirtyStorage.Copy()
	stateObject.originStorage = s.originStorage.Copy()
	stateObject.pendingStorage = s.pendingStorage.Copy()
	stateObject.suicided = s.suicided
	stateObject.dirtyCode = s.dirtyCode
	stateObject.deleted = s.deleted

	stateObject.dirtyNonceCount = s.dirtyNonceCount
	stateObject.dirtyBalanceCount = s.dirtyBalanceCount
	stateObject.dirtyCodeCount = s.dirtyCodeCount
	stateObject.dirtyStorageCount = make(map[common.Hash]uint)
	for k, v := range s.dirtyStorageCount {
		stateObject.dirtyStorageCount[k] = v
	}

	if s.snap != nil {
		stateSnap := *s.snap
		stateObject.snap = &stateSnap
	}

	stateObject.isNewlyCreated = s.isNewlyCreated

	return stateObject
}

func (s *stateObject) shareCopy(db *StateDB) {
	obj := &stateObject{
		address:           s.address,
		addrHash:          s.addrHash,
		data:              s.data.Copy(),
		db:                db,
		code:              s.code,
		originStorage:     s.originStorage,
		pendingStorage:    s.pendingStorage,
		dirtyStorage:      make(Storage),
		dirtyCode:         s.dirtyCode,
		suicided:          s.suicided,
		deleted:           s.deleted,
		dirtyStorageCount: make(map[common.Hash]uint),
		delta:             s.db.GetNewDeltaObject(),
		isNewlyCreated:    s.isNewlyCreated,
	}
	if s.trie != nil {
		obj.trie = db.db.CopyTrie(s.trie)
	}
	s.pair, obj.pair = obj, s
}

func (s *stateObject) isShared() bool {
	return s.db.IsShared()
}

func (s *stateObject) isInPrimary() bool {
	return s.db.IsPrimary()
}

func (s *stateObject) addDelta() {
	if s.isShared() && s.delta == nil {
		s.delta = s.db.GetNewDeltaObject() //newDeltaObject()
	}
}

func (s *stateObject) updatePair() {
	obj := s.pair
	obj.data.Nonce = s.data.Nonce
	if obj.data.Balance.Cmp(s.data.Balance) != 0 {
		//obj.data.Balance = new(big.Int).Set(s.data.Balance)
		//obj.data.Balance.Set(s.data.Balance)
		obj.data.Balance = s.db.GetNewBigInt().Set(s.data.Balance)
	}
	obj.data.CodeHash = s.data.CodeHash
	obj.code = s.code
	if len(obj.delta.originStorage) > 0 {
		//obj.delta.originStorage = make(Storage)
		obj.delta.originStorage = s.db.GetNewOriginStorage()
	}
	if len(obj.delta.pendingStorage) > 0 {
		//obj.delta.pendingStorage = make(Storage, 100)
		obj.delta.pendingStorage = s.db.GetNewPendingStorage()
	}
	if len(obj.dirtyStorage) > 0 {
		//obj.dirtyStorage = make(Storage)
		obj.dirtyStorage = s.db.GetNewOriginStorage()
	}
	obj.dirtyCode = s.dirtyCode
	obj.suicided = s.suicided
	obj.deleted = s.deleted
	obj.dirtyNonceCount = 0
	obj.dirtyBalanceCount = 0
	obj.dirtyCodeCount = 0
	if len(obj.dirtyStorageCount) > 0 {
		obj.dirtyStorageCount = make(map[common.Hash]uint)
	}
}

func (s *stateObject) UpdateDelta() {
	s.updateOriginStorage(s.delta.originStorage)
	//s.delta.originStorage = make(Storage)
	if len(s.delta.originStorage) > 0 {
		s.delta.originStorage = s.db.GetNewOriginStorage()
	}
	if len(s.delta.pendingStorage) > 0 {
		for addr, value := range s.delta.pendingStorage {
			s.pendingStorage[addr] = value
		}
		//s.delta.pendingStorage = make(Storage, 100)
		s.delta.pendingStorage = s.db.GetNewPendingStorage()
	}
}

func (s *stateObject) updateOriginStorage(storage Storage) {
	if len(storage) == 0 {
		return
	}
	//if len(storage) > len(s.originStorage) {
	//	s.originStorage, storage = storage, s.originStorage
	//	s.pair.originStorage = s.originStorage
	//}
	for addr, value := range storage {
		s.originStorage[addr] = value
	}
}

func (s *stateObject) GetPair() *stateObject {
	return s.pair
}

//
// Attribute accessors
//

// Returns the address of the contract/account
func (s *stateObject) Address() common.Address {
	return s.address
}

// Code returns the contract code associated with this object, if any.
func (s *stateObject) Code(db Database) []byte {
	if s.code != nil {
		return s.code
	}
	if bytes.Equal(s.CodeHash(), emptyCodeHash) {
		return nil
	}
	code, err := db.ContractCode(s.addrHash, common.BytesToHash(s.CodeHash()))
	if err != nil {
		s.setError(fmt.Errorf("can't load code hash %x: %v", s.CodeHash(), err))
	}
	s.code = code
	return code
}

func (s *stateObject) SetCode(codeHash common.Hash, code []byte) {
	if s.db.IgnoreJournalEntry {
		s.db.journal.addDirty(&s.address)
	} else {
		prevcode := s.Code(s.db.db)
		s.db.journal.append(codeChange{
			account:   &s.address,
			prevhash:  s.CodeHash(),
			prevcode:  prevcode,
			afterhash: &codeHash,
			aftercode: code,
		})
	}

	s.dirtyCodeCount++
	s.setCode(codeHash, code)
}

func (s *stateObject) setCode(codeHash common.Hash, code []byte) {
	s.code = code
	s.data.CodeHash = codeHash[:]
	s.dirtyCode = true
}

func (s *stateObject) SetNonce(nonce uint64) {
	if s.db.IgnoreJournalEntry {
		s.db.journal.addDirty(&s.address)
	} else {
		s.db.journal.append(nonceChange{
			account: &s.address,
			prev:    s.data.Nonce,
			after:   nonce,
		})
	}
	s.dirtyNonceCount++
	s.setNonce(nonce)
}

func (s *stateObject) setNonce(nonce uint64) {
	s.data.Nonce = nonce
}

func (s *stateObject) CodeHash() []byte {
	return s.data.CodeHash
}

func (s *stateObject) Balance() *big.Int {
	return s.data.Balance
}

func (s *stateObject) Nonce() uint64 {
	return s.data.Nonce
}

// TODO need to be remove
func (s *stateObject) Suicide() bool {
	return s.suicided
}

func (s *stateObject) hasDirtyWrite() bool {
	if s.dirtyBalanceCount > 0 || s.dirtyNonceCount > 0 || s.dirtyCodeCount > 0 {
		return true
	}
	for _, v := range s.dirtyStorageCount {
		if v > 0 {
			return true
		}
	}
	return false
}

func (s *stateObject) hasDirtyAccountWrite() bool {
	if s.dirtyBalanceCount > 0 || s.dirtyNonceCount > 0 || s.dirtyCodeCount > 0 {
		return true
	}
	return false
}

// Never called, but must be present to allow stateObject to be used
// as a vm.Account interface that also satisfies the vm.ContractRef
// interface. Interfaces are awesome.
func (s *stateObject) Value() *big.Int {
	panic("Value on stateObject should never be called")
}
