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
	UpdateSuicide(addr common.Address)
	UpdateDirtyStateObject(so *stateObject)
	RWClear()
	RWDump() (ReadStates, ReadChain, WriteStates, []common.Address)
	WObjectDump() map[common.Address]*stateObject

	UpdateRHeader(field cmptypes.Field, val interface{})
	UpdateRBlockhash(num uint64, val common.Hash)
	UpdateRAccount(addr common.Address, field cmptypes.Field, val interface{})
	UpdateRStorage(addr common.Address, field cmptypes.Field, input common.Hash, val common.Hash)
	UpdateWField(addr common.Address, field cmptypes.Field, val interface{})
	UpdateWStorage(addr common.Address, input common.Hash, val common.Hash)
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

func (h emptyRWRecorder) UpdateWField(addr common.Address, field cmptypes.Field, val interface{}) {
}

func (h emptyRWRecorder) UpdateWStorage(addr common.Address, input common.Hash, val common.Hash) {
}

func (h emptyRWRecorder) RWClear() {
}

func (h emptyRWRecorder) RWDump() (ReadStates, ReadChain, WriteStates, []common.Address) {
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
	RState          ReadStates
	RChain          ReadChain
	WState          WriteStates
	RStateAddresses []common.Address
	WObject         map[common.Address]*stateObject
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
	h.UpdateWField(addr, cmptypes.Suicided, true)
}

func (h *rwRecorderImpl) UpdateDirtyStateObject(so *stateObject) {
	addr := so.address
	if so.dirtyNonceCount > 0 {
		so.dirtyNonceCount = 0
		h.UpdateWField(addr, cmptypes.Nonce, so.data.Nonce)
	}
	if so.dirtyBalanceCount > 0 {
		so.dirtyBalanceCount = 0
		h.UpdateWField(addr, cmptypes.Balance, new(big.Int).Set(so.data.Balance))
	}
	if so.dirtyCodeCount > 0 {
		so.dirtyCodeCount = 0
		h.UpdateWField(addr, cmptypes.Code, so.code)
	}
	for k, v := range so.dirtyStorage {
		if so.dirtyStorageCount[k] > 0 {
			h.UpdateWStorage(addr, k, v)
		}
		delete(so.dirtyStorageCount, k)
	}
}

func newRWHook() *rwRecorderImpl {
	return &rwRecorderImpl{
		RState:          make(ReadStates),
		RChain:          ReadChain{},
		WState:          make(WriteStates),
		RStateAddresses: []common.Address{},
		WObject:         make(map[common.Address]*stateObject),
	}
}

func (h *rwRecorderImpl) DirtyDump() map[string]interface{} {
	tmpMap := make(map[string]interface{})
	tmpMap["RState"] = h.RState
	tmpMap["RChain"] = h.RChain
	tmpMap["WState"] = h.WState
	return tmpMap
}

func (h *rwRecorderImpl) RWDump() (ReadStates, ReadChain, WriteStates, []common.Address) {
	return h.RState, h.RChain, h.WState, h.RStateAddresses
}

func (h *rwRecorderImpl) WObjectDump() map[common.Address]*stateObject {
	return h.WObject
}

// no need
func (h *rwRecorderImpl) RWClear() {
	h.RState = make(ReadStates)
	h.RChain = ReadChain{}
	h.WState = make(WriteStates)
	h.RStateAddresses = []common.Address{}
	h.WObject = make(map[common.Address]*stateObject)
}

func (h *rwRecorderImpl) UpdateRHeader(field cmptypes.Field, val interface{}) {
	switch field {
	case cmptypes.Coinbase:
		coinbase := val.(common.Address)
		h.RChain.Coinbase = &coinbase
	case cmptypes.Timestamp:
		h.RChain.Timestamp = val.(*big.Int)
	case cmptypes.Number:
		h.RChain.Number = val.(*big.Int)
	case cmptypes.Difficulty:
		h.RChain.Difficulty = val.(*big.Int)
	case cmptypes.GasLimit:
		gasLimit := val.(uint64)
		h.RChain.GasLimit = &gasLimit
	}
}

func (h *rwRecorderImpl) UpdateRBlockhash(num uint64, val common.Hash) {
	if h.RChain.Blockhash == nil {
		h.RChain.Blockhash = make(map[uint64]common.Hash)
	}
	if _, ok := h.RChain.Blockhash[num]; !ok {
		h.RChain.Blockhash[num] = val
	}
}

func (h *rwRecorderImpl) UpdateRAccount(addr common.Address, field cmptypes.Field, val interface{}) {
	if _, ok := h.RState[addr]; !ok {
		h.RState[addr] = new(ReadState)
		h.RStateAddresses = append(h.RStateAddresses, addr)
	}
	state := h.RState[addr]
	switch field {
	case cmptypes.Balance:
		state.Balance = val.(*big.Int)
	case cmptypes.Nonce:
		nonce := val.(uint64)
		state.Nonce = &nonce
	case cmptypes.CodeHash:
		codeHash := val.(common.Hash)
		state.CodeHash = &codeHash
	case cmptypes.Exist:
		exist := val.(bool)
		state.Exist = &exist
	case cmptypes.Empty:
		empty := val.(bool)
		state.Empty = &empty
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
		h.RStateAddresses = append(h.RStateAddresses, addr)
	}
	state := h.RState[addr]
	if field == cmptypes.Storage {
		if state.Storage == nil {
			state.Storage = make(Storage)
		}
		if _, ok := state.Storage[key]; !ok {
			state.Storage[key] = val
		}
	}
	if field == cmptypes.CommittedStorage {
		if state.CommittedStorage == nil {
			state.CommittedStorage = make(Storage)
		}
		if _, ok := state.CommittedStorage[key]; !ok {
			state.CommittedStorage[key] = val
		}
	}
}

func (h *rwRecorderImpl) UpdateWField(addr common.Address, field cmptypes.Field, val interface{}) {
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

func (h *rwRecorderImpl) UpdateWStorage(addr common.Address, key common.Hash, val common.Hash) {
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
