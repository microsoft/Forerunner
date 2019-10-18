package state

import (
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
)

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
	RWDump() (
		map[common.Address]map[cmptypes.Field]interface{},
		map[cmptypes.Field]interface{},
		map[common.Address]map[cmptypes.Field]interface{})

	UpdateRHeader(field cmptypes.Field, val interface{})
	UpdateRBlockhash(num uint64, val common.Hash)
	UpdateRAccount(addr common.Address, field cmptypes.Field, val interface{})
	UpdateRStorage(addr common.Address, field cmptypes.Field, input common.Hash, val common.Hash)
	UpdateWField(addr common.Address, field cmptypes.Field, val interface{})
	UpdateWStorage(addr common.Address, input common.Hash, val common.Hash)
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

func (h emptyRWRecorder) RWDump() (
	map[common.Address]map[cmptypes.Field]interface{},
	map[cmptypes.Field]interface{},
	map[common.Address]map[cmptypes.Field]interface{}) {
	panic("Enable RWRecord mode to use RWDump!")
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

type rwRecorderImpl struct {
	RState map[common.Address]map[cmptypes.Field]interface{}
	RChain map[cmptypes.Field]interface{}
	WState map[common.Address]map[cmptypes.Field]interface{}
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
		RState: make(map[common.Address]map[cmptypes.Field]interface{}),
		RChain: make(map[cmptypes.Field]interface{}),
		WState: make(map[common.Address]map[cmptypes.Field]interface{}),
	}
}

func (h *rwRecorderImpl) DirtyDump() map[string]interface{} {
	tmpMap := make(map[string]interface{})
	tmpMap["RState"] = h.RState
	tmpMap["RChain"] = h.RChain
	tmpMap["WState"] = h.WState
	return tmpMap
}

func (h *rwRecorderImpl) RWDump() (map[common.Address]map[cmptypes.Field]interface{},
	map[cmptypes.Field]interface{}, map[common.Address]map[cmptypes.Field]interface{}) {
	return h.RState, h.RChain, h.WState
}

// no need
func (h *rwRecorderImpl) RWClear() {
	h.RState = make(map[common.Address]map[cmptypes.Field]interface{})
	h.RChain = make(map[cmptypes.Field]interface{})
	h.WState = make(map[common.Address]map[cmptypes.Field]interface{})
}

func (h *rwRecorderImpl) UpdateRHeader(field cmptypes.Field, val interface{}) {
	if _, ok := h.RChain[field]; !ok {
		h.RChain[field] = val
	}
}

func (h *rwRecorderImpl) UpdateRBlockhash(num uint64, val common.Hash) {
	if _, ok := h.RChain[cmptypes.Blockhash]; !ok {
		h.RChain[cmptypes.Blockhash] = make(map[uint64]common.Hash)
	}
	blockhashMap := h.RChain[cmptypes.Blockhash].(map[uint64]common.Hash)
	if _, ok := blockhashMap[num]; !ok {
		blockhashMap[num] = val
	}
}

func (h *rwRecorderImpl) UpdateRAccount(addr common.Address, field cmptypes.Field, val interface{}) {
	if _, ok := h.RState[addr]; !ok {
		h.RState[addr] = make(map[cmptypes.Field]interface{})
	}

	if _, ok := h.RState[addr][field]; !ok {
		h.RState[addr][field] = val
	}
	//else {
	//	if _, ok := val.(*debugInfo); !ok {
	//		if _, ok := x.(*debugInfo); ok {
	//			self.RState[addr][field] = val
	//		}
	//	}
	//}
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

func (h *rwRecorderImpl) UpdateRStorage(addr common.Address, field cmptypes.Field, input common.Hash, val common.Hash) {
	if _, ok := h.RState[addr]; !ok {
		h.RState[addr] = make(map[cmptypes.Field]interface{})
	}
	if _, ok := h.RState[addr][field]; !ok {
		h.RState[addr][field] = make(map[common.Hash]common.Hash)
	}
	storage := h.RState[addr][field].(map[common.Hash]common.Hash)
	if _, ok := storage[input]; !ok {
		storage[input] = val
	}
}

func (h *rwRecorderImpl) UpdateWField(addr common.Address, field cmptypes.Field, val interface{}) {
	if _, ok := h.WState[addr]; !ok {
		h.WState[addr] = make(map[cmptypes.Field]interface{})
	}
	h.WState[addr][field] = val
}

func (h *rwRecorderImpl) UpdateWStorage(addr common.Address, input common.Hash, val common.Hash) {
	if _, ok := h.WState[addr]; !ok {
		h.WState[addr] = make(map[cmptypes.Field]interface{})
	}
	if _, ok := h.WState[addr][cmptypes.DirtyStorage]; !ok {
		h.WState[addr][cmptypes.DirtyStorage] = make(map[common.Hash]common.Hash)
	}
	storage := h.WState[addr][cmptypes.DirtyStorage].(map[common.Hash]common.Hash)
	storage[input] = val
}
