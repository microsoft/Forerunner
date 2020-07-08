package cmpreuse

import (
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"strconv"
)

type StateIDM struct {
	mapping map[common.Hash]uint32
	mutable bool
}

func NewStateIDM() *StateIDM {
	return &StateIDM{
		mapping: make(map[common.Hash]uint32),
	}
}

type TracerStateObject struct {
	//	Account map[StringID] *Variable
	//Balance *Variable
	//Nonce *Variable
	//Code  *Variable
	//CodeHash *Variable
	//CodeSize *Variable
	//Empty *Variable
	//Exist *Variable
	Storage        map[interface{}]*Variable
	CommittedState map[common.Hash]*Variable
	HasNonConstKey bool
	StateIDs       *StateIDM
	StateIDMVar    *Variable
	GuardedKeyVars map[*Variable]struct{}
}

func NewTracerStateObject() *TracerStateObject {
	return &TracerStateObject{
		Storage:        make(map[interface{}]*Variable),
		CommittedState: make(map[common.Hash]*Variable),
		StateIDs:       NewStateIDM(),
		GuardedKeyVars: make(map[*Variable]struct{}),
	}
}

type AddrIDM struct {
	mapping map[common.Address]uint32
	mutable bool
}

func NewAddrIDM() *AddrIDM {
	return &AddrIDM{
		mapping: make(map[common.Address]uint32),
	}
}

type TracerWorldState struct {
	WorldState            map[common.Address]*TracerStateObject
	SnapshotInitialValues []map[common.Address]*TracerStateObject
	HasNonConstAddr       bool
	AddrIDs               *AddrIDM
	AddrIDMVar            *Variable
	GuardedAddrVars       map[*Variable]bool
}

func NewTracerWorldState() *TracerWorldState {
	siv := make([]map[common.Address]*TracerStateObject, 1)
	siv[0] = make(map[common.Address]*TracerStateObject)
	return &TracerWorldState{
		WorldState:            make(map[common.Address]*TracerStateObject),
		SnapshotInitialValues: siv,
		AddrIDs:               NewAddrIDM(),
		GuardedAddrVars:       make(map[*Variable]bool),
	}
}

func (ws *TracerWorldState) Snapshot() uint {
	ws.SnapshotInitialValues = append(ws.SnapshotInitialValues, make(map[common.Address]*TracerStateObject))
	return uint(len(ws.SnapshotInitialValues) - 1)
}

func (ws *TracerWorldState) RevertToSnapshot(id uint) {
	sLen := uint(len(ws.SnapshotInitialValues))
	if id == 0 {
		panic("Revert too much!")
	}
	if id >= sLen {
		panic("Revert into the future!")
	}
	for i := sLen - 1; i >= id; i-- {
		iv := ws.SnapshotInitialValues[i]
		for addr, so := range iv {
			for k, v := range so.Storage {
				ws.WorldState[addr].Storage[k] = v
			}
		}
	}
	ws.SnapshotInitialValues = ws.SnapshotInitialValues[:id]
}

func (ws *TracerWorldState) TWLoad(variant StringID, vars ...*Variable) *Variable {
	addr := vars[0].BAddress()
	ws.guardAddr(addr, vars[0])
	if variant == ACCOUNT_COMMITTED_STATE {
		return ws.loadCommittedState(addr, vars[1])
	}
	if variant == ACCOUNT_STATE {
		return ws.loadState(addr, vars[1])
	}

	return ws.loadAccount(addr, variant)
}

func (ws *TracerWorldState) SetLoadValue(variant StringID, output *Variable, vars ...*Variable) {
	addr := vars[0].BAddress()
	ws.guardAddr(addr, vars[0])
	if variant == ACCOUNT_STATE {
		ws.setLoadStateValue(addr, vars[1], output)
		return
	}
	if variant == ACCOUNT_COMMITTED_STATE {
		ws.setLoadCommittedStateValue(addr, vars[1], output)
		return
	}
	ws.setLoadAccountValue(addr, variant, output)
	return
}

func (ws *TracerWorldState) loadAccount(addr common.Address, field StringID) *Variable {
	so := ws.WorldState[addr]
	if so != nil {
		return so.Storage[field]
	}
	return nil
}

func (ws *TracerWorldState) setLoadAccountValue(addr common.Address, field StringID, v *Variable) {
	so := ws.WorldState[addr]
	if so == nil {
		so = NewTracerStateObject()
		ws.WorldState[addr] = so
	}
	if so.Storage[field] != nil {
		panic(fmt.Sprintf("Should never set load value twice!"))
	}
	so.Storage[field] = v
}

func (ws *TracerWorldState) loadState(addr common.Address, keyVar *Variable) *Variable {
	key := keyVar.BHash()
	so := ws.WorldState[addr]
	if so != nil {
		val := so.Storage[key]
		if val != nil {
			ws.guardStateKey(key, keyVar, val, so, false)
		}
		return val
	}
	return nil
}

func (ws *TracerWorldState) setLoadStateValue(addr common.Address, keyVar *Variable, v *Variable) {
	key := keyVar.BHash()
	so := ws.WorldState[addr]
	if so == nil {
		so = NewTracerStateObject()
		ws.WorldState[addr] = so
	}

	if so.Storage[key] != nil {
		panic(fmt.Sprintf("Should never set load value twice!"))
	}
	so.Storage[key] = v
	if so.CommittedState[key] != nil {
		panic("CommittedState and State not synced!")
	}
	so.CommittedState[key] = v
	ws.guardStateKey(key, keyVar, v, so, false)
}

func (ws *TracerWorldState) loadCommittedState(addr common.Address, keyVar *Variable) *Variable {
	key := keyVar.BHash()
	so := ws.WorldState[addr]
	if so != nil {
		val := so.CommittedState[key]
		if val != nil {
			ws.guardStateKey(key, keyVar, val, so, false)
		}
		return val
	}
	return nil
}

func (ws *TracerWorldState) setLoadCommittedStateValue(addr common.Address, keyVar, v *Variable) {
	key := keyVar.BHash()
	so := ws.WorldState[addr]
	if so == nil {
		so = NewTracerStateObject()
		ws.WorldState[addr] = so
	}

	if so.CommittedState[key] != nil {
		panic(fmt.Sprintf("Should never set load committedState twice!"))
	}
	so.CommittedState[key] = v
	if so.Storage[key] == nil {
		so.Storage[key] = v
	}
	ws.guardStateKey(key, keyVar, v, so, false)
}

func (ws *TracerWorldState) TWStore(variant StringID, vars ...*Variable) {
	addr := common.BigToAddress(vars[0].BigInt())
	ws.guardAddr(addr, vars[0])
	var key interface{}
	key = variant

	v := vars[0].tracer.Bool_true
	if key == ACCOUNT_STATE {
		key = vars[1].BHash()
		v = vars[2]
	} else if variant != ACCOUNT_SUICIDE {
		v = vars[1]
	}
	so := ws.WorldState[addr]
	if so == nil {
		panic("Store before Load Object")
	}
	prevV := so.Storage[key]
	if prevV == nil {
		if variant == ACCOUNT_STATE || variant == ACCOUNT_BALANCE || variant == ACCOUNT_NONCE {
			panic("Store before Load key")
		}
	}

	so.Storage[key] = v

	snap := ws.SnapshotInitialValues[len(ws.SnapshotInitialValues)-1]
	po := snap[addr]
	if po == nil {
		po = NewTracerStateObject()
		snap[addr] = po
	}
	if po.Storage[key] == nil {
		po.Storage[key] = prevV
	}

	if variant == ACCOUNT_STATE {
		ws.guardStateKey(key.(common.Hash), vars[1], vars[2], so, true)
	}

}

func (ws *TracerWorldState) guardStateKey(hashKey common.Hash, keyVar *Variable, valVar *Variable, so *TracerStateObject, isStore bool) {
	if _, ok := so.GuardedKeyVars[keyVar]; ok {
		return
	}else{
		so.GuardedKeyVars[keyVar] = struct{}{}
	}
	if so.HasNonConstKey {
		gk := so.StateIDMVar.GetStateValueID(keyVar).NGuard("state_key")
		if gk.Uint32() == 0 || isStore {
			if isStore {
				cmptypes.MyAssert(gk.Uint32() != 0, "read should be before store")
			}
			so.StateIDMVar = so.StateIDMVar.SetStateValueID(keyVar, keyVar.tracer.ConstVarWithName(valVar.id, "sID"+strconv.Itoa(int(valVar.id))))
		}
	} else {
		if keyVar.IsConst() {
			if _, ok := so.StateIDs.mapping[hashKey]; !ok || isStore {
				if isStore {
					cmptypes.MyAssert(ok, "read should be before store")
				}
				so.StateIDs.mapping[hashKey] = valVar.id
			}
		} else {
			cmptypes.MyAssert(so.StateIDMVar == nil)
			so.StateIDMVar = keyVar.tracer.ConstVarWithName(so.StateIDs, "initSIDM")
			gk := so.StateIDMVar.GetStateValueID(keyVar).NGuard("state_key")
			if gk.Uint32() == 0 || isStore {
				if isStore {
					cmptypes.MyAssert(gk.Uint32() != 0, "read should be before store")
				}
				so.StateIDMVar = so.StateIDMVar.SetStateValueID(keyVar, keyVar.tracer.ConstVarWithName(valVar.id, "sID"+strconv.Itoa(int(valVar.id))))
			}
			so.HasNonConstKey = true
		}

	}
}

func (ws *TracerWorldState) guardAddr(addr common.Address, addrVar *Variable) {
	if ws.GuardedAddrVars[addrVar] {
		return
	} else {
		ws.GuardedAddrVars[addrVar] = true
	}
	if ws.HasNonConstAddr {
		ak := ws.AddrIDMVar.GetAddrID(addrVar).NGuard("account_addr")
		if ak.Uint32() == 0 {
			ws.AddrIDMVar = ws.AddrIDMVar.SetAddrID(addrVar, addrVar.tracer.ConstVarWithName(addrVar.id, "aID"+strconv.Itoa(int(addrVar.id))))
		}
	} else {
		if addrVar.IsConst() {
			if _, ok := ws.AddrIDs.mapping[addr]; !ok {
				ws.AddrIDs.mapping[addr] = addrVar.id
			}
		} else {
			cmptypes.MyAssert(ws.AddrIDMVar == nil)
			ws.AddrIDMVar = addrVar.tracer.ConstVarWithName(ws.AddrIDs, "initAIDM")
			ak := ws.AddrIDMVar.GetAddrID(addrVar).NGuard("account_addr")
			if ak.Uint32() == 0 {
				ws.AddrIDMVar = ws.AddrIDMVar.SetAddrID(addrVar, addrVar.tracer.ConstVarWithName(addrVar.id, "aID"+strconv.Itoa(int(addrVar.id))))
			}
			ws.HasNonConstAddr = true
		}
	}
}
