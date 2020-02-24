package state

import (
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
)

type ReadState struct {
	Balance          *big.Int
	Nonce            *uint64
	CodeHash         *common.Hash
	Exist            *bool
	Empty            *bool
	Storage          Storage
	CommittedStorage Storage
}

type ReadChain struct {
	Blockhash  map[uint64]common.Hash
	Coinbase   *common.Address
	Timestamp  *big.Int
	Number     *big.Int
	Difficulty *big.Int
	GasLimit   *uint64
}

type WriteState struct {
	Balance      *big.Int
	Nonce        *uint64
	Code         *Code
	DirtyStorage Storage
	Suicided     *bool
}

type ReadStates map[common.Address]*ReadState
type WriteStates map[common.Address]*WriteState

type RWRecorder interface {
	_Exist(addr common.Address, so *stateObject, ret bool)
	_Empty(addr common.Address, so *stateObject, ret bool)
	_GetBalance(addr common.Address, so *stateObject, ret *big.Int)
	_GetNonce(addr common.Address, so *stateObject, ret uint64)
	_GetCode(addr common.Address, so *stateObject, ret []byte)
	_GetCodeSize(addr common.Address, so *stateObject, ret int)
	_GetCodeHash(addr common.Address, so *stateObject, ret common.Hash)
	_GetState(addr common.Address, hash common.Hash, so *stateObject, ret common.Hash)
	_GetCommittedState(addr common.Address, hash common.Hash, so *stateObject, ret common.Hash)
	_AddBalance(addr common.Address, so *stateObject)
	_SubBalance(addr common.Address, so *stateObject)
	RWClear()
	//RWDump() (map[common.Address]*ReadState, ReadChain, map[common.Address]*WriteState, *cmptypes.ReadDetail, map[common.Address]*stateObject)
	RWDump() (ReadStates, ReadChain, WriteStates, *cmptypes.ReadDetail)
	WObjectDump() map[common.Address]*stateObject

	UpdateRHeader(field cmptypes.Field, val interface{})
	UpdateRBlockhash(num uint64, val common.Hash)
	UpdateRAccount(addr common.Address, field cmptypes.Field, val interface{})
	UpdateRStorage(addr common.Address, field cmptypes.Field, input common.Hash, val common.Hash)

	UpdateSuicide(addr common.Address)
	UpdateDirtyStateObject(so *stateObject)
	UpdateWObject(addr common.Address, object *stateObject)
}

type emptyRWRecorder struct {
}

func (h emptyRWRecorder) UpdateRHeader(field cmptypes.Field, val interface{}) {
}

func (h emptyRWRecorder) UpdateRBlockhash(num uint64, val common.Hash) {
}

func (h emptyRWRecorder) UpdateRAccount(addr common.Address, field cmptypes.Field, val interface{}) {
}

func (h emptyRWRecorder) UpdateRStorage(addr common.Address, field cmptypes.Field, input common.Hash, val common.Hash) {
}

func (h emptyRWRecorder) RWClear() {
}

func (h emptyRWRecorder) RWDump() (ReadStates, ReadChain, WriteStates, *cmptypes.ReadDetail) {
	panic("Enable RWRecord mode to use RWDump!")
}

func (h emptyRWRecorder) WObjectDump() map[common.Address]*stateObject {
	panic("Enable RWRecord mode to use WObjectDump!")
}

func (h emptyRWRecorder) _Exist(addr common.Address, so *stateObject, ret bool) {
}

func (h emptyRWRecorder) _Empty(addr common.Address, so *stateObject, ret bool) {
}

func (h emptyRWRecorder) _GetBalance(addr common.Address, so *stateObject, ret *big.Int) {
}

func (h emptyRWRecorder) _GetNonce(addr common.Address, so *stateObject, ret uint64) {
}

func (h emptyRWRecorder) _GetCode(addr common.Address, so *stateObject, ret []byte) {
}

func (h emptyRWRecorder) _GetCodeSize(addr common.Address, so *stateObject, ret int) {
}

func (h emptyRWRecorder) _GetCodeHash(addr common.Address, so *stateObject, ret common.Hash) {
}

func (h emptyRWRecorder) _GetState(addr common.Address, hash common.Hash, so *stateObject, ret common.Hash) {
}

func (h emptyRWRecorder) _GetCommittedState(addr common.Address, hash common.Hash, so *stateObject, ret common.Hash) {
}

func (h emptyRWRecorder) _AddBalance(addr common.Address, so *stateObject) {
}

func (h emptyRWRecorder) _SubBalance(addr common.Address, so *stateObject) {
}

func (h emptyRWRecorder) UpdateSuicide(addr common.Address) {
}

func (h emptyRWRecorder) UpdateDirtyStateObject(so *stateObject) {
}

func (h emptyRWRecorder) UpdateWObject(addr common.Address, object *stateObject) {
}

type rwRecorderImpl struct {
	statedb    *StateDB
	RState     ReadStates
	RChain     ReadChain
	WState     WriteStates
	ReadDetail *cmptypes.ReadDetail
	WObject    map[common.Address]*stateObject
}

func (h *rwRecorderImpl) _Exist(addr common.Address, so *stateObject, ret bool) {
	if so == nil || !so.hasDirtyWrite() {
		h.UpdateRAccount(addr, cmptypes.Exist, ret)
	}
}

func (h *rwRecorderImpl) _Empty(addr common.Address, so *stateObject, ret bool) {
	if so == nil || !so.hasDirtyWrite() {
		h.UpdateRAccount(addr, cmptypes.Empty, ret)
	}
}

func (h *rwRecorderImpl) _GetBalance(addr common.Address, so *stateObject, ret *big.Int) {
	if so == nil || so.dirtyBalanceCount == 0 {
		h.UpdateRAccount(addr, cmptypes.Balance, ret)
	}
}

func (h *rwRecorderImpl) _GetNonce(addr common.Address, so *stateObject, ret uint64) {
	if so == nil || so.dirtyNonceCount == 0 {
		h.UpdateRAccount(addr, cmptypes.Nonce, ret)
	}
}

func (h *rwRecorderImpl) _GetCode(addr common.Address, so *stateObject, ret []byte) {
	if so == nil || so.dirtyCodeCount == 0 {
		h.UpdateRCodeXXX(so, addr)
	}
}

func (h *rwRecorderImpl) _GetCodeSize(addr common.Address, so *stateObject, ret int) {
	if so == nil || so.dirtyCodeCount == 0 {
		h.UpdateRCodeXXX(so, addr)
	}
}

func (h *rwRecorderImpl) _GetCodeHash(addr common.Address, so *stateObject, ret common.Hash) {
	if so == nil || so.dirtyCodeCount == 0 {
		h.UpdateRAccount(addr, cmptypes.CodeHash, ret)
	}
}

func (h *rwRecorderImpl) _GetState(addr common.Address, hash common.Hash, so *stateObject, ret common.Hash) {
	if so == nil || (so.dirtyStorageCount[hash] == 0) {
		h.UpdateRStorage(addr, cmptypes.Storage, hash, ret)
	}
}

func (h *rwRecorderImpl) _GetCommittedState(addr common.Address, hash common.Hash, so *stateObject, ret common.Hash) {
	h.UpdateRStorage(addr, cmptypes.CommittedStorage, hash, ret)
}

func (h *rwRecorderImpl) _AddBalance(addr common.Address, so *stateObject) {
	h.UpdateRBalance(so, addr)
}

func (h *rwRecorderImpl) _SubBalance(addr common.Address, so *stateObject) {
	h.UpdateRBalance(so, addr)
}

func (h *rwRecorderImpl) UpdateSuicide(addr common.Address) {
	h.updateWField(addr, cmptypes.Suicided, true)
}

func (h *rwRecorderImpl) UpdateDirtyStateObject(so *stateObject) {
	addr := so.address
	if so.dirtyNonceCount > 0 {
		so.dirtyNonceCount = 0
		h.updateWField(addr, cmptypes.Nonce, so.data.Nonce)
	}
	if so.dirtyBalanceCount > 0 {
		so.dirtyBalanceCount = 0
		h.updateWField(addr, cmptypes.Balance, new(big.Int).Set(so.data.Balance))
	}
	if so.dirtyCodeCount > 0 {
		so.dirtyCodeCount = 0
		h.updateWField(addr, cmptypes.Code, so.code)
	}
	for k, v := range so.dirtyStorage {
		if so.dirtyStorageCount[k] > 0 {
			h.updateWStorage(addr, k, v)
		}
		delete(so.dirtyStorageCount, k)
	}
}

func newRWHook(db *StateDB) *rwRecorderImpl {
	return &rwRecorderImpl{
		statedb:    db,
		RState:     make(ReadStates),
		RChain:     ReadChain{},
		WState:     make(WriteStates),
		ReadDetail: cmptypes.NewReadDetail(),
		WObject:    make(map[common.Address]*stateObject),
	}
}

func (h *rwRecorderImpl) DirtyDump() map[string]interface{} {
	tmpMap := make(map[string]interface{})
	tmpMap["RState"] = h.RState
	tmpMap["RChain"] = h.RChain
	tmpMap["WState"] = h.WState
	return tmpMap
}

func (h *rwRecorderImpl) RWDump() (ReadStates, ReadChain, WriteStates, *cmptypes.ReadDetail) {
	return h.RState, h.RChain, h.WState, h.ReadDetail
}

func (h *rwRecorderImpl) WObjectDump() map[common.Address]*stateObject {
	return h.WObject
}

// no need
func (h *rwRecorderImpl) RWClear() {
	h.RState = make(ReadStates)
	h.RChain = ReadChain{}
	h.WState = make(WriteStates)
	h.ReadDetail = cmptypes.NewReadDetail()
	h.WObject = make(map[common.Address]*stateObject)
}

func (h *rwRecorderImpl) UpdateRHeader(field cmptypes.Field, val interface{}) {
	var value interface{}
	// Make sure all kinds of value should be simple types instead of struct or slice
	switch field {
	case cmptypes.Coinbase:
		if h.RChain.Coinbase == nil {
			coinbase := val.(common.Address)
			h.RChain.Coinbase = &coinbase
			value = coinbase
		}
	case cmptypes.Timestamp:
		if h.RChain.Timestamp == nil {
			h.RChain.Timestamp = val.(*big.Int)
			value = val.(*big.Int).Uint64() // Time in `Header` in block.go is uint64
		}
	case cmptypes.Number:
		if h.RChain.Number == nil {
			h.RChain.Number = val.(*big.Int)
			value = val.(*big.Int).Uint64() // Number in `Header` in block.go is uint64
		}
	case cmptypes.Difficulty:
		if h.RChain.Difficulty == nil {
			h.RChain.Difficulty = val.(*big.Int)
			value = common.BigToHash(val.(*big.Int)) // Difficulty in `Header` in block.go is *big.Int
		}
	case cmptypes.GasLimit:
		if h.RChain.GasLimit == nil {
			gasLimit := val.(uint64)
			h.RChain.GasLimit = &gasLimit
			value = gasLimit // GasLimit in `Header` in block.go is uint64
		}
	}

	if value != nil {
		if h.ReadDetail.IsBlockSensitive == false {
			h.ReadDetail.IsBlockSensitive = true
		}
		addLoc := cmptypes.AddrLocation{Field: field}
		newAlv := &cmptypes.AddrLocValue{AddLoc: &addLoc, Value: value}
		h.ReadDetail.ReadDetailSeq = append(h.ReadDetail.ReadDetailSeq, newAlv)
		h.ReadDetail.ReadAddressAndBlockSeq = append(h.ReadDetail.ReadAddressAndBlockSeq, newAlv)
	}

}

func (h *rwRecorderImpl) UpdateRBlockhash(num uint64, val common.Hash) {
	if h.RChain.Blockhash == nil {
		h.RChain.Blockhash = make(map[uint64]common.Hash)
	}
	if _, ok := h.RChain.Blockhash[num]; !ok {
		h.RChain.Blockhash[num] = val

		//if h.ReadDetail.IsBlockSensitive == false{
		//	h.ReadDetail.IsBlockSensitive = true
		//}
		addLoc := cmptypes.AddrLocation{Field: cmptypes.Blockhash, Loc: num}
		newAlv := &cmptypes.AddrLocValue{AddLoc: &addLoc, Value: val}
		h.ReadDetail.ReadDetailSeq = append(h.ReadDetail.ReadDetailSeq, newAlv)
	}
}

func (h *rwRecorderImpl) UpdateRAccount(addr common.Address, field cmptypes.Field, val interface{}) {
	if _, ok := h.RState[addr]; !ok {
		h.RState[addr] = new(ReadState)

		readDep := cmptypes.AddrLocation{Address: addr, Field: cmptypes.Dependence}
		readDepValue := &cmptypes.AddrLocValue{AddLoc: &readDep, Value: h.statedb.GetTxDepByAccount(addr)}
		h.ReadDetail.ReadAddressAndBlockSeq = append(h.ReadDetail.ReadAddressAndBlockSeq, readDepValue)
	}
	var value interface{}

	state := h.RState[addr]
	switch field {
	case cmptypes.Balance:
		if state.Balance == nil {
			state.Balance = val.(*big.Int)
			value = common.BigToHash(val.(*big.Int)) // convert complex type to simple type: bytes[32]
		}
	case cmptypes.Nonce:
		if state.Nonce == nil {
			nonce := val.(uint64)
			state.Nonce = &nonce
			value = nonce
		}
	case cmptypes.CodeHash:
		if state.CodeHash == nil {
			codeHash := val.(common.Hash)
			state.CodeHash = &codeHash
			value = codeHash
		}
	case cmptypes.Exist:
		if state.Exist == nil {
			exist := val.(bool)
			state.Exist = &exist
			value = exist
		}
	case cmptypes.Empty:
		if state.Empty == nil {
			empty := val.(bool)
			state.Empty = &empty
			value = empty
		}
	}

	if value != nil {
		addLoc := cmptypes.AddrLocation{Address: addr, Field: field}
		h.ReadDetail.ReadDetailSeq = append(h.ReadDetail.ReadDetailSeq, &cmptypes.AddrLocValue{AddLoc: &addLoc, Value: value})
	}
}

func (h *rwRecorderImpl) UpdateRBalance(so *stateObject, addr common.Address) {
	var ret *big.Int
	if so != nil {
		ret = so.Balance()
	} else {
		ret = common.Big0
	}
	if so == nil || so.dirtyBalanceCount == 0 {
		h.UpdateRAccount(addr, cmptypes.Balance, ret)
	}
}

func (h *rwRecorderImpl) UpdateRCodeXXX(so *stateObject, addr common.Address) {
	var ret common.Hash
	if so == nil {
		ret = common.Hash{}
	} else {
		ret = common.BytesToHash(so.CodeHash())
	}
	if so == nil || so.dirtyCodeCount == 0 {
		h.UpdateRAccount(addr, cmptypes.CodeHash, ret)
	}
}

func (h *rwRecorderImpl) UpdateRStorage(addr common.Address, field cmptypes.Field, key common.Hash, val common.Hash) {
	if _, ok := h.RState[addr]; !ok {
		h.RState[addr] = new(ReadState)

		readDep := cmptypes.AddrLocation{Address: addr, Field: cmptypes.Dependence}
		readDepValue := &cmptypes.AddrLocValue{AddLoc: &readDep, Value: h.statedb.GetTxDepByAccount(addr)}
		h.ReadDetail.ReadAddressAndBlockSeq = append(h.ReadDetail.ReadAddressAndBlockSeq, readDepValue)
	}
	state := h.RState[addr]
	if field == cmptypes.Storage {
		if state.Storage == nil {
			state.Storage = make(Storage)
		}
		if _, ok := state.Storage[key]; !ok {
			state.Storage[key] = val

			addLoc := cmptypes.AddrLocation{Address: addr, Field: field, Loc: key}
			newAlv := &cmptypes.AddrLocValue{AddLoc: &addLoc, Value: val}
			h.ReadDetail.ReadDetailSeq = append(h.ReadDetail.ReadDetailSeq, newAlv)

		}
	}
	if field == cmptypes.CommittedStorage {
		if state.CommittedStorage == nil {
			state.CommittedStorage = make(Storage)
		}
		if _, ok := state.CommittedStorage[key]; !ok {
			state.CommittedStorage[key] = val

			addLoc := &cmptypes.AddrLocation{Address: addr, Field: field, Loc: key}
			newAlv := &cmptypes.AddrLocValue{AddLoc: addLoc, Value: val}
			h.ReadDetail.ReadDetailSeq = append(h.ReadDetail.ReadDetailSeq, newAlv)
		}
	}
}

func (h *rwRecorderImpl) updateWField(addr common.Address, field cmptypes.Field, val interface{}) {
	if _, ok := h.WState[addr]; !ok {
		h.WState[addr] = new(WriteState)
	}
	state := h.WState[addr]
	switch field {
	case cmptypes.Balance:
		state.Balance = val.(*big.Int)
	case cmptypes.Nonce:
		nonce := val.(uint64)
		state.Nonce = &nonce
	case cmptypes.Code:
		code := val.(Code)
		state.Code = &code
	case cmptypes.Suicided:
		suicided := val.(bool)
		state.Suicided = &suicided
	}
}

func (h *rwRecorderImpl) updateWStorage(addr common.Address, key common.Hash, val common.Hash) {
	if _, ok := h.WState[addr]; !ok {
		h.WState[addr] = new(WriteState)
	}
	if h.WState[addr].DirtyStorage == nil {
		h.WState[addr].DirtyStorage = make(Storage)
	}
	h.WState[addr].DirtyStorage[key] = val
}

func (h *rwRecorderImpl) UpdateWObject(addr common.Address, object *stateObject) {
	cpy := object.deepCopy(object.db)
	cpy.delta = newDeltaObject()
	cpy.Code(cpy.db.db)
	cpy.shareCopy(cpy.db)
	h.WObject[addr] = cpy
}
