// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

package cmpreuse

import (
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"github.com/ethereum/go-ethereum/optipreplayer/config"
	"math/big"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type STrace struct {
	Stats            []*Statement
	CrosscheckStats  []*Statement
	RAlloc           map[uint32]uint
	RegisterFileSize uint
	LastUse          map[uint]uint
	debugOut         DebugOutFunc

	VariableInputFieldDependencies              map[uint32]InputDependence // VariableID => sorted VariableID List
	SNodeInputFieldDependencies                 map[uint]InputDependence   // SNodeIndex => sorted VariableID List
	VariableInputAccountDependencies            map[uint32]InputDependence // VariableID => sorted AccountID List
	SNodeInputAccountDependencies               map[uint]InputDependence   // SNodeIndex => sorted AccountID List
	RLNodeIndices                               []uint                     // read/load node indices
	RLNodeSet                                   map[uint]bool
	ANodeIndices                                []uint
	ANodeSet                                    map[uint]bool
	ANodeIndexToAccountIndex                    map[uint]uint
	AccountIndexToAddressOrRId                  map[uint]interface{} //common.Address or uint
	StoreAccountIndices                         []uint
	StoreAccountIndexToStoreFieldDependencies   map[uint][]uint
	StoreAccountIndexToStoreAccountDependencies map[uint][]uint
	StoreAccountIndexToStoreVarRegisterIndices  map[uint][]uint
	RLNodeIndexToAccountIndex                   map[uint]uint
	RLNodeIndexToFieldIndex                     map[uint]uint
	FieldIndexToRLNodeIndex                     map[uint]uint
	TLNodeIndices                               []uint
	TLNodeSet                                   map[uint]bool
	FirstStoreNodeIndex                         uint
	JSPIndices                                  []uint
	JSPSet                                      map[uint]bool
	StoreNodeIndexToStoreAccountIndex           map[uint]uint
	computed                                    bool
	DebugBuffer                                 *DebugBuffer
	TraceDuration                               time.Duration
	SpecializationStats                         *SpecializationStats
}

func NewSTrace(stats []*Statement, debugOut DebugOutFunc, buffer *DebugBuffer, specializedStats *SpecializationStats, perfLogOn bool, msg *types.Message) *STrace {
	st := &STrace{SpecializationStats: specializedStats}
	PrintStatementsWithWriteOut(stats, debugOut)
	beforeEliminatedStoreStateCount := len(stats)
	stats = ExecutePassAndPrintWithOut(EliminateRevertedStore, stats, debugOut, false)
	stats = ExecutePassAndPrintWithOut(EliminateRedundantStore, stats, debugOut, false)
	stats = ExecutePassAndPrintWithOut(EliminateSuicidedStore, stats, debugOut, false)
	st.SpecializationStats.EliminatedStateOpCount += beforeEliminatedStoreStateCount - len(stats)

	st.CrosscheckStats = stats

	st.SpecializationStats.EliminatedUnusedOpCount = len(stats)
	stats = ExecutePassAndPrintWithOut(EliminateUnusedStatement, stats, debugOut, false)
	st.SpecializationStats.EliminatedUnusedOpCount -= len(stats)
	stats = ExecutePassAndPrintWithOut(PushAllStoresToBottom, stats, debugOut, false)
	stats = ExecutePassAndPrintWithOut(PushLoadReadComputationDownToFirstUse, stats, debugOut, false)
	st.Stats = stats
	st.MeasureSpecializationStats(perfLogOn, msg)
	rmapping, highestRegisterIndex := AllocateVirtualRegisterForNonConstVariables(st.Stats)
	st.RAlloc = rmapping
	st.RegisterFileSize = highestRegisterIndex + 1
	st.debugOut = debugOut
	if debugOut != nil {
		st.DebugBuffer = buffer
	}

	if debugOut != nil {
		st.ComputeDepAndJumpInfo()
		debugOut("AfterStraceInit\n")
		debugOut("StraceSummary: statCount: %v, RLNodeCount: %v, ANodeCount: %v, TLNodeCount %v, JSPCount %v\n",
			len(st.Stats), len(st.RLNodeIndices), len(st.ANodeIndices), len(st.TLNodeSet), len(st.JSPIndices))
		for i, s := range st.Stats {
			PrintStatementsWithWriteOutAndRegisterAnnotation([]*Statement{s}, &(st.RAlloc), debugOut)
			nodeIndex := uint(i)
			infoPrefix := "       "
			if dep, ok := st.SNodeInputFieldDependencies[nodeIndex]; ok {
				debugOut(infoPrefix+"FieldDep: %v\n", dep)
			}
			if dep, ok := st.SNodeInputAccountDependencies[nodeIndex]; ok {
				debugOut(infoPrefix+"AccountDep: %v\n", dep)
			}
			debugOut(infoPrefix+"RLNode: %v, ANode: %v, TLNode: %v, JSP %v\n",
				st.RLNodeSet[nodeIndex], st.ANodeSet[nodeIndex], st.TLNodeSet[nodeIndex], st.JSPSet[nodeIndex])
		}
	}
	return st
}


//var TraceLogFile    *os.File
//var TraceLogChan    chan string = make(chan string, 100000)
//
//func StartTraceLogWriter(){
//
//}

func (st *STrace) MeasureSpecializationStats(perfLogOn bool, msg *types.Message) {
	lastGuardIndex := -1
	fastPathUsedVar := make(map[uint32]struct{})
	for i, s := range reversedStatements(st.Stats) {
		if s.IsGasOp {
			st.SpecializationStats.GeneratedGasOpCount++
		} else if s.op == OP_ConcatBytes {
			st.SpecializationStats.GeneratedConcatOpCount++
		} else if s.op.IsStateDependencyOp() {
			st.SpecializationStats.GeneratedStateDependencyOpCount++
		}
		if s.op.IsGuard() && lastGuardIndex == -1 {
			lastGuardIndex = len(st.Stats) - i - 1
		}

		if lastGuardIndex != -1 { // fast path
			for _, v := range s.inputs {
				fastPathUsedVar[v.id] = struct{}{}
			}
		} else { // constraint
			if s.output != nil {
				if _, ok := fastPathUsedVar[s.output.id]; ok {
					st.SpecializationStats.FastPathConstraintCommonNodeCount++
					for _, v := range s.inputs {
						fastPathUsedVar[v.id] = struct{}{}
					}
				}
			}
		}

	}
	st.SpecializationStats.ConstraintNodeCount = lastGuardIndex + 1
	st.SpecializationStats.FastPathNodeCount = len(st.Stats) - (lastGuardIndex + 1)

	if perfLogOn {

	}
}

func (st *STrace) ShallowCopyAndExtendTrace() *STrace {
	newTrace := *st
	nt := &newTrace
	if nt.debugOut != nil {
		cmptypes.MyAssert(st.DebugBuffer != nil)
		buffer := NewDebugBuffer(st.DebugBuffer)
		nt.DebugBuffer = buffer
		nt.debugOut = func(fmtStr string, args ...interface{}) {
			buffer.AppendLog(fmtStr, args...)
		}
	}
	return nt
}

func (st *STrace) ComputeDepAndJumpInfo() {
	if st.computed {
		return
	}
	st.computed = true
	st.CalculateInputDependencies()
	st.FindOutJumpStartingPoints()
}

func (st *STrace) GenerateRWRecord(db *state.StateDB, registers *RegisterFile) *cache.RWRecord {
	rwHook := state.NewRWHook(db)
	for _, nIndex := range st.RLNodeIndices {
		s := st.Stats[nIndex]
		if s.op.isReadOp {
			switch s.op.config.variant {
			case BLOCK_COINBASE:
				rwHook.UpdateRHeader(cmptypes.Coinbase, GetStatementOutputMultiTypeVal(s, registers, st.RAlloc).GetAddress())
			case BLOCK_TIMESTAMP:
				rwHook.UpdateRHeader(cmptypes.Timestamp, GetStatementOutputMultiTypeVal(s, registers, st.RAlloc).GetBigInt(nil))
			case BLOCK_NUMBER:
				rwHook.UpdateRHeader(cmptypes.Number, GetStatementOutputMultiTypeVal(s, registers, st.RAlloc).GetBigInt(nil))
			case BLOCK_DIFFICULTY:
				rwHook.UpdateRHeader(cmptypes.Difficulty, GetStatementOutputMultiTypeVal(s, registers, st.RAlloc).GetBigInt(nil))
			case BLOCK_GASLIMIT:
				rwHook.UpdateRHeader(cmptypes.GasLimit, GetStatementOutputMultiTypeVal(s, registers, st.RAlloc).GetBigInt(nil).Uint64())
			case BLOCK_HASH:
				bNum := GetStatementInputMultiTypeVal(s, 0, registers, st.RAlloc).GetBigInt(nil).Uint64()
				bHash := GetStatementOutputMultiTypeVal(s, registers, st.RAlloc).GetHash()
				rwHook.UpdateRBlockhash(bNum, bHash)
			default:
				panic("Unknown fRead variant")
			}
		} else if s.op.isLoadOp {
			addr := GetStatementInputMultiTypeVal(s, 0, registers, st.RAlloc).GetAddress()
			val := GetStatementOutputVal(s, registers, st.RAlloc)
			switch s.op.config.variant {
			case ACCOUNT_NONCE:
				rwHook.UpdateRAccount(addr, cmptypes.Nonce, val)
			case ACCOUNT_BALANCE:
				rwHook.UpdateRAccount(addr, cmptypes.Balance, val.(*MultiTypedValue).GetBigInt(nil))
			case ACCOUNT_EXIST:
				rwHook.UpdateRAccount(addr, cmptypes.Exist, val)
			case ACCOUNT_EMPTY:
				rwHook.UpdateRAccount(addr, cmptypes.Empty, val)
			case ACCOUNT_CODEHASH:
				rwHook.UpdateRAccount(addr, cmptypes.CodeHash, val.(*MultiTypedValue).GetHash())
			case ACCOUNT_CODESIZE:
				// code size is replaced by a previous codehash check
			case ACCOUNT_CODE:
				// code is replaced by a previous codehash check
			case ACCOUNT_STATE:
				key := GetStatementInputMultiTypeVal(s, 1, registers, st.RAlloc).GetHash()
				rwHook.UpdateRStorage(addr, cmptypes.Storage, key, val.(*MultiTypedValue).GetHash())
			case ACCOUNT_COMMITTED_STATE:
				key := GetStatementInputMultiTypeVal(s, 1, registers, st.RAlloc).GetHash()
				rwHook.UpdateRStorage(addr, cmptypes.CommittedStorage, key, val.(*MultiTypedValue).GetHash())
			default:
				panic("Unknown fLoad variant!")
			}
		} else {
			panic("Not RL nodes!")
		}
	}

	var failed bool

	for sIndex := st.FirstStoreNodeIndex; sIndex < uint(len(st.Stats)); sIndex++ {
		s := st.Stats[sIndex]
		if s.op.IsLog() {
			continue
		}
		if s.op.IsVirtual() {
			if s.op.config.variant == VIRTUAL_FAILED {
				failed = GetStatementInputVal(s, 0, registers, st.RAlloc).(bool)
			}
			continue
		}
		addr := GetStatementInputMultiTypeVal(s, 0, registers, st.RAlloc).GetAddress()
		switch s.op.config.variant {
		case ACCOUNT_NONCE:
			val := GetStatementInputVal(s, 1, registers, st.RAlloc)
			rwHook.UpdateWField(addr, cmptypes.Nonce, val)
		case ACCOUNT_BALANCE:
			val := GetStatementInputMultiTypeVal(s, 1, registers, st.RAlloc).GetBigInt(nil)
			rwHook.UpdateWField(addr, cmptypes.Balance, val)
		case ACCOUNT_CODE:
			val := GetStatementInputVal(s, 1, registers, st.RAlloc)
			rwHook.UpdateWField(addr, cmptypes.Code, state.Code(val.([]byte)))
		case ACCOUNT_STATE:
			key := GetStatementInputMultiTypeVal(s, 1, registers, st.RAlloc).GetHash()
			val := GetStatementInputMultiTypeVal(s, 2, registers, st.RAlloc).GetHash()
			rwHook.UpdateWStorage(addr, key, val)
		case ACCOUNT_SUICIDE:
			rwHook.UpdateSuicide(addr)
		case ACCOUNT_LOG:
			panic("Should not see log")
		case VIRTUAL_FAILED:
			panic("Should not see Failed")
		case VIRTUAL_GASUSED:
			panic("Should not see GasUsed")
		default:
			panic("Unknown fLoad variant!")
		}
	}

	RState, RChain, WState, ReadDetail := rwHook.RWDump()
	return cache.NewRWRecord(RState, RChain, WState, ReadDetail, failed)
}

func GetNonConstVariablesLastUseInfo(stats []*Statement) (lastUse map[uint32]uint, kills [][]uint32) {
	lastUse = make(map[uint32]uint)
	kills = make([][]uint32, len(stats))
	for i, s := range stats {
		for _, v := range s.inputs {
			if !v.IsConst() {
				lastUse[v.id] = uint(i)
			}
		}
		if s.output != nil && !s.output.IsConst() {
			lastUse[s.output.id] = uint(i)
		}
	}

	for vid, sid := range lastUse {
		kills[sid] = append(kills[sid], vid)
	}

	return lastUse, kills
}

type InputDependence []uint

func (dep InputDependence) IsSuperSetOf(other InputDependence) bool {
	for len(dep) >= len(other) && len(other) > 0 {
		if dep[0] < other[0] {
			dep = dep[1:]
		} else if dep[0] == other[0] {
			dep = dep[1:]
			other = other[1:]
		} else { // dep[0] > other[0], which mean other have a vid that dep does not have
			return false
		}
	}

	if len(dep) < len(other) {
		return false
	}
	return true
}

func (dep InputDependence) Equals(other InputDependence) bool {
	if len(dep) != len(other) {
		return false
	}
	return dep.IsSuperSetOf(other)
}

func (dep InputDependence) SetDiffWithPreallocation(other, ret InputDependence) InputDependence {
	AssertIncreasingDep(dep)
	AssertIncreasingDep(other)
	if ret == nil {
		ret = make(InputDependence, 0, len(dep))
	} else {
		cmptypes.MyAssert(len(ret) == 0)
	}

	depSize := len(dep)
	otherSize := len(other)
	depIndex := 0
	otherIndex := 0

	for depIndex < depSize && otherIndex < otherSize {
		d1 := dep[depIndex]
		d2 := other[otherIndex]
		if d1 < d2 {
			ret = append(ret, d1)
			depIndex++
		} else if d1 == d2 {
			depIndex++
			otherIndex++
		} else { // d1 > d2
			otherIndex++
		}
	}

	if depIndex < depSize {
		ret = append(ret, dep[depIndex:depSize]...)
	}

	//for len(dep) > 0 && len(other) > 0 {
	//	d1 := dep[0]
	//	d2 := other[0]
	//	if d1 < d2 {
	//		ret = append(ret, d1)
	//		dep = dep[1:]
	//	} else if d1 == d2 {
	//		dep = dep[1:]
	//		other = other[1:]
	//	} else { // d1 > d2
	//		other = other[1:]
	//	}
	//}
	//
	//if len(dep) > 0 {
	//	ret = append(ret, dep...)
	//}

	AssertIncreasingDep(ret)
	return ret
}

func (dep InputDependence) SetDiff(other InputDependence) InputDependence {
	return dep.SetDiffWithPreallocation(other, nil)
}

var DEBUG_TRACER = false

func AssertIncreasingDep(dep InputDependence) {
	if DEBUG_TRACER {
		for i := 1; i < len(dep); i++ {
			//cmptypes.MyAssert(dep[i-1] < dep[i], "wrong dep: %v", dep)
			cmptypes.MyAssert(dep[i-1] < dep[i])
		}
	}
}

func MergeInputDependenciesWithPreallocation(ret InputDependence, dependencies ...InputDependence) InputDependence {
	if len(dependencies) == 0 {
		return nil
	}

	totalCount := 0
	for _, d := range dependencies {
		AssertIncreasingDep(d)
		totalCount += len(d)
	}

	if ret == nil {
		ret = make(InputDependence, 0, totalCount)
	} else {
		cmptypes.MyAssert(len(ret) == 0)
	}

	MAX_UINT := ^uint(0)
	for totalCount > 0 {
		minDep := -1
		minVId := MAX_UINT
		// Find the smallest VID
		for i, d := range dependencies {
			if len(d) > 0 {
				if d[0] < minVId {
					minVId = d[0]
					minDep = i
				}
			}
		}

		cmptypes.MyAssert(minDep != -1 && minVId != MAX_UINT)

		// append it to ret if not already appended
		lenRet := len(ret)
		if lenRet == 0 {
			ret = append(ret, minVId)
		} else {
			cmptypes.MyAssert(ret[lenRet-1] <= minVId)
			if ret[lenRet-1] < minVId {
				ret = append(ret, minVId)
			}
		}

		// remove the minVid from minDep
		dependencies[minDep] = dependencies[minDep][1:]
		totalCount--
	}

	AssertIncreasingDep(ret)
	return ret
}

func MergeInputDependencies(dependencies ...InputDependence) InputDependence {
	return MergeInputDependenciesWithPreallocation(nil, dependencies...)
}

func (st *STrace) CalculateInputDependencies() {
	variableInputFieldDependencies := make(map[uint32]InputDependence)   // VariableID => sorted VariableID List
	sNodeInputFieldDependencies := make(map[uint]InputDependence)        // SNodeIndex => sorted VariableID List
	variableInputAccountDependencies := make(map[uint32]InputDependence) // VariableID => sorted AccountID List
	sNodeInputAccountDependencies := make(map[uint]InputDependence)      // SNodeIndex => sorted AccountID List

	rLNodeIndices := make([]uint, 0, 10) // read/load node indices
	rLNodeSet := make(map[uint]bool)
	aNodeIndices := make([]uint, 0, 10) // ANode = Read or an account's first Load op
	aNodeSet := make(map[uint]bool)
	aNodeIndexToAccountIndex := make(map[uint]uint)
	tLNodeIndices := make([]uint, 0, 10) // load node, which is not ANode, but whose previous input op is a read or a load of different account
	tLNodeSet := make(map[uint]bool)
	addrToAccountId := make(map[common.Address]uint)   // addr ==> account_id
	accountIdToAddrOrRId := make(map[uint]interface{}) // uint ==> common.Address or register_id
	rlNodeToAccountIndex := make(map[uint]uint)        // read/load node to account_id
	rLNodeToFieldIndex := make(map[uint]uint)          // read/load node to field_id
	fieldIndexToRLNodeIndex := make(map[uint]uint)

	storeAccountIndexToStoreFieldDependencies := make(map[uint][]uint)
	storeAccountIndexToStoreAccountDependencies := make(map[uint][]uint)
	storeAccountIndexToStoreVarRegisterIndices := make(map[uint][]uint)
	storeAccountIndices := []uint(nil)
	storeAccountProcessed := make(map[uint]bool)
	storeNodeIndexToStoreAccountIndex := make(map[uint]uint)

	for i, s := range st.Stats {
		if s.op.isStoreOp {
			if st.FirstStoreNodeIndex == 0 {
				cmptypes.MyAssert(i > 0)
				st.FirstStoreNodeIndex = uint(i)
			}
			if !(s.op.IsLog()) && !(s.op.IsVirtual()) {
				addr := s.inputs[0].BAddress()
				addrAccountIndex := addrToAccountId[addr]
				cmptypes.MyAssert(addrAccountIndex > 0)

				storeNodeIndexToStoreAccountIndex[uint(i)] = addrAccountIndex

				if !storeAccountProcessed[addrAccountIndex] {
					storeAccountProcessed[addrAccountIndex] = true
					storeAccountIndices = append(storeAccountIndices, addrAccountIndex)
				}

				for _, v := range s.inputs {
					if v.IsConst() {
						continue
					}
					valueVarID := v.id
					storeVarIds := storeAccountIndexToStoreVarRegisterIndices[addrAccountIndex]
					// this is not dependency. we misuse it to make sure the varRegisterIndices are unique and sorted
					storeVarIds = MergeInputDependencies(storeVarIds, []uint{st.RAlloc[valueVarID]})
					storeAccountIndexToStoreVarRegisterIndices[addrAccountIndex] = storeVarIds

					valFieldDependencies, fok := variableInputFieldDependencies[valueVarID]
					cmptypes.MyAssert(fok)
					fDeps := storeAccountIndexToStoreFieldDependencies[addrAccountIndex]
					fDeps = MergeInputDependencies(fDeps, valFieldDependencies)
					storeAccountIndexToStoreFieldDependencies[addrAccountIndex] = fDeps

					valAccountDependencies, aok := variableInputAccountDependencies[valueVarID]
					cmptypes.MyAssert(aok)
					aDeps := storeAccountIndexToStoreAccountDependencies[addrAccountIndex]
					aDeps = MergeInputDependencies(aDeps, valAccountDependencies)
					storeAccountIndexToStoreAccountDependencies[addrAccountIndex] = aDeps
				}
			}
			continue
		}
		nodeIndex := uint(i)
		vid := s.output.id
		_, ok := variableInputFieldDependencies[vid]
		cmptypes.MyAssert(!ok)
		if s.op.isLoadOp || s.op.isReadOp {
			rLNodeIndices = append(rLNodeIndices, nodeIndex)
			rLNodeSet[nodeIndex] = true
			fieldIndex := uint(len(rLNodeIndices))
			rLNodeToFieldIndex[nodeIndex] = fieldIndex
			fieldIndexToRLNodeIndex[fieldIndex] = nodeIndex

			isANode := false
			aIndex := uint(len(aNodeIndices)) + 1
			if s.op.isReadOp {
				isANode = true
			} else {
				addr := s.inputs[0].BAddress()
				if a, ok := addrToAccountId[addr]; ok {
					aIndex = a
				} else {
					isANode = true
					addrToAccountId[addr] = aIndex
					if s.inputs[0].IsConst() {
						accountIdToAddrOrRId[aIndex] = addr
					} else {
						rid := st.RAlloc[s.inputs[0].id]
						cmptypes.MyAssert(rid != 0)
						accountIdToAddrOrRId[aIndex] = rid
					}
				}
			}
			rlNodeToAccountIndex[nodeIndex] = aIndex
			if isANode {
				aNodeIndices = append(aNodeIndices, nodeIndex)
				aNodeSet[nodeIndex] = true
				aNodeIndexToAccountIndex[nodeIndex] = aIndex
			} else if s.op.isLoadOp {
				istLNode := false
				cmptypes.MyAssert(len(rLNodeIndices) >= 2)
				prevIndex := rLNodeIndices[len(rLNodeIndices)-2]
				prevS := st.Stats[prevIndex]
				if prevS.op.isReadOp {
					istLNode = true
				} else {
					addr := s.inputs[0].BAddress()
					prevAddr := prevS.inputs[0].BAddress()
					if addr != prevAddr {
						istLNode = true
					}
				}
				if istLNode {
					tLNodeIndices = append(tLNodeIndices, nodeIndex)
					tLNodeSet[nodeIndex] = true
				}

			}

			fieldIndex, ok := rLNodeToFieldIndex[nodeIndex]
			cmptypes.MyAssert(ok)

			inputFieldDependency := make(InputDependence, 0)
			inputAccountDependency := make(InputDependence, 0)
			for i, v := range s.inputs {
				// skip the addr variable
				if s.op.isLoadOp && i == 0 {
					continue
				}
				if v.IsConst() {
					continue
				}
				inputFieldDependency = MergeInputDependencies(variableInputFieldDependencies[v.id], inputFieldDependency)
				cmptypes.MyAssert(len(inputFieldDependency) > 0)
				inputAccountDependency = MergeInputDependencies(variableInputAccountDependencies[v.id], inputAccountDependency)
				cmptypes.MyAssert(len(inputAccountDependency) > 0)
			}

			//fDeps := MergeInputDependencies(inputFieldDependency, InputDependence{fieldIndex})
			variableInputFieldDependencies[vid] = InputDependence{fieldIndex} //fDeps // load/read output depend on itself
			sNodeInputFieldDependencies[nodeIndex] = inputFieldDependency     //fDeps

			aDeps := MergeInputDependencies(inputAccountDependency, InputDependence{aIndex})
			variableInputAccountDependencies[vid] = aDeps    //MergeInputDependencies(inputAccountDependency, InputDependence{aIndex})
			sNodeInputAccountDependencies[nodeIndex] = aDeps //inputAccountDependency

		} else {
			inputFieldDeps := make([]InputDependence, 0, 2)
			inputAccountDeps := make([]InputDependence, 0, 2)
			for _, v := range s.inputs {
				if !v.IsConst() {
					dep, ok := variableInputFieldDependencies[v.id]
					cmptypes.MyAssert(ok)
					inputFieldDeps = append(inputFieldDeps, dep)
					dep, ok = variableInputAccountDependencies[v.id]
					cmptypes.MyAssert(ok)
					inputAccountDeps = append(inputAccountDeps, dep)
				}
			}
			cmptypes.MyAssert(len(inputFieldDeps) > 0)
			outputFieldDep := MergeInputDependencies(inputFieldDeps...)
			outputAccountDep := MergeInputDependencies(inputAccountDeps...)
			if !s.op.IsGuard() {
				cmptypes.MyAssert(!s.output.IsConst())
				variableInputFieldDependencies[vid] = outputFieldDep
				variableInputAccountDependencies[vid] = outputAccountDep
			}
			sNodeInputFieldDependencies[nodeIndex] = outputFieldDep
			sNodeInputAccountDependencies[nodeIndex] = outputAccountDep
		}
	}

	st.VariableInputFieldDependencies = variableInputFieldDependencies
	st.SNodeInputFieldDependencies = sNodeInputFieldDependencies
	st.VariableInputAccountDependencies = variableInputAccountDependencies
	st.SNodeInputAccountDependencies = sNodeInputAccountDependencies
	st.RLNodeIndices = rLNodeIndices
	st.RLNodeSet = rLNodeSet
	st.ANodeIndices = aNodeIndices
	st.ANodeSet = aNodeSet
	st.ANodeIndexToAccountIndex = aNodeIndexToAccountIndex
	st.RLNodeIndexToAccountIndex = rlNodeToAccountIndex
	st.RLNodeIndexToFieldIndex = rLNodeToFieldIndex
	st.FieldIndexToRLNodeIndex = fieldIndexToRLNodeIndex
	st.TLNodeIndices = tLNodeIndices
	st.TLNodeSet = tLNodeSet
	st.AccountIndexToAddressOrRId = accountIdToAddrOrRId
	st.StoreAccountIndices = storeAccountIndices
	st.StoreAccountIndexToStoreFieldDependencies = storeAccountIndexToStoreFieldDependencies
	st.StoreAccountIndexToStoreAccountDependencies = storeAccountIndexToStoreAccountDependencies
	st.StoreAccountIndexToStoreVarRegisterIndices = storeAccountIndexToStoreVarRegisterIndices
	st.StoreNodeIndexToStoreAccountIndex = storeNodeIndexToStoreAccountIndex
}

func (st *STrace) FindOutJumpStartingPoints() {
	aNodeSet := st.ANodeSet
	// JSP (jump starting point) = a snode we could potentially Old by matching previous read/loads
	//  and potentially Old one or more subsequent ops
	// It cannot be a ANode, which can never be Oldped
	// Any non-A-Load is a JSP
	// Any Compute/Guard which follows a ANode or non-A-Load  is a JSP
	// Any Compute/Guard which follows another Compute/Guard is a JSP as along as its dependence set is not a superset of the previous one

	jSPIndices := make([]uint, 0, len(st.RLNodeIndices))
	jSPSet := make(map[uint]bool, len(st.RLNodeIndices))

	for i, s := range st.Stats {
		if s.op.isStoreOp {
			break
		}
		nodeIndex := uint(i)

		isJSP := false

		if s.op.isReadOp || s.op.isLoadOp {
			if aNodeSet[nodeIndex] {
				continue
			}
			// Any non-A-Load is a JSP
			isJSP = true
		} else { // node is a Compute/Guard
			cmptypes.MyAssert(nodeIndex > 0)
			prevIndex := nodeIndex - 1
			prevS := st.Stats[prevIndex]

			if aNodeSet[prevIndex] || prevS.op.isLoadOp {
				// Any Compute/Guard which follows a ANode or non-A-Load  is a JSP
				isJSP = true
			} else { // prev is non-a-Load guard or compute
				// Any Compute/Guard which follows another Compute/Guard is a JSP as along as its dependence set is not a superset of the previous one
				currentDep := st.SNodeInputFieldDependencies[nodeIndex]
				prevDep := st.SNodeInputFieldDependencies[prevIndex]
				if !currentDep.IsSuperSetOf(prevDep) { //!currentDep.IsSuperSetOf(prevDep) {
					isJSP = true
				}
			}
		}
		if isJSP {
			jSPIndices = append(jSPIndices, nodeIndex)
			jSPSet[nodeIndex] = true
		}
	}

	st.JSPIndices = jSPIndices
	st.JSPSet = jSPSet
}

func AllocateVirtualRegisterForNonConstVariables(stats []*Statement) (registerMapping map[uint32]uint, highestIndex uint) {
	// index 0 is reserved
	highestIndex = uint(0)
	registerMapping = make(map[uint32]uint)

	//lastUse, _ = GetNonConstVariablesLastUseInfo(stats)

	assign := func(vid uint32) (rid uint) {
		highestIndex++
		rid = highestIndex
		return
	}

	for _, s := range stats {
		for _, v := range s.inputs {
			if !v.IsConst() {
				if _, ok := registerMapping[v.id]; !ok {
					panic(fmt.Sprintf("%v of %v not assigned before use!", v.Name(), s.SimpleNameString()))
				}
			}
		}
		if s.output != nil && !s.output.IsConst() {
			vid := s.output.id
			if _, ok := registerMapping[vid]; ok {
				panic(fmt.Sprintf("Assign output twice %v\n", s.SimpleNameString()))
			} else {
				registerMapping[vid] = assign(vid)
			}
		}
	}

	return
}

type ISRefCountNode interface {
	GetRefCount() uint
	AddRef() uint           // add a ref and return the new ref count
	RemoveRef(value uint64) // remove a ref
}

type SRefCount struct {
	refCount uint
}

func (rf *SRefCount) GetRefCount() uint {
	return rf.refCount
}

func (rf *SRefCount) AddRef() uint {
	rf.refCount++
	return rf.refCount
}

func (rf *SRefCount) SRemoveRef() uint {
	cmptypes.MyAssert(rf.refCount > 0)
	rf.refCount--
	return rf.refCount
}

type TrieRoundRefNodes struct {
	RefNodes    []ISRefCountNode
	RoundID     uint64
	Round       *cache.PreplayResult
	RoundPathID uintptr
	Next        *TrieRoundRefNodes
}

type AccountNodeJumpDef struct {
	Dest                 *AccountSearchTrieNode
	RegisterSnapshot     *RegisterFile
	FieldHistorySnapshot *RegisterFile
	InJumpNode           *AccountSearchTrieNode
	InJumpAVId           uint
	SRefCount
}

func (jd *AccountNodeJumpDef) RemoveRef(roundID uint64) {
	newRefCount := jd.SRemoveRef()
	if newRefCount == 0 {
		if jd.InJumpNode != nil && jd.InJumpNode.GetRefCount() != 0 {
			cmptypes.MyAssert(jd.InJumpAVId != 0)
			cmptypes.MyAssert(jd.InJumpNode.JumpTable[jd.InJumpAVId] == jd)
			jd.InJumpNode.JumpTable[jd.InJumpAVId] = nil
		}
	}
}

type AccountSearchTrieNode struct {
	LRNode    *SNode
	JumpTable []*AccountNodeJumpDef
	Exit      *FieldSearchTrieNode
	Rounds    []*cache.PreplayResult
	AddrLoc   *cmptypes.AddrLocation
	IsStore   bool
	IsLoad    bool
	IsRead    bool
	SRefCount
	BigSize
}

func NewAccountSearchTrieNode(node *SNode) *AccountSearchTrieNode {
	return &AccountSearchTrieNode{
		LRNode:  node,
		IsStore: node.Op.isStoreOp,
		IsLoad:  node.Op.isLoadOp,
		IsRead:  node.Op.isReadOp,
	}
}

func (n *AccountSearchTrieNode) RemoveRef(roundID uint64) {
	n.SRemoveRef()
	if n.Rounds != nil {
		_removeRound(n.Rounds, roundID)
	}
}

func (n *AccountSearchTrieNode) getOrCreateJumpDef(jt []*AccountNodeJumpDef, vid uint) ([]*AccountNodeJumpDef, *AccountNodeJumpDef) {
	cmptypes.MyAssert(vid > 0)
	targetLength := vid + 1
	increase := int(targetLength) - len(jt)
	for i := 0; i < increase; i++ {
		jt = append(jt, nil)
	}
	jd := jt[vid]
	if jd == nil {
		jd = &AccountNodeJumpDef{}
		jt[vid] = jd
		jd.InJumpNode = n
		jd.InJumpAVId = vid
	}
	return jt, jd
}

func (n *AccountSearchTrieNode) GetOrCreateAJumpDef(vid uint) (ret *AccountNodeJumpDef) {
	n.JumpTable, ret = n.getOrCreateJumpDef(n.JumpTable, vid)
	return
}

func (n *AccountSearchTrieNode) GetAJumpDef(vid uint) (ret *AccountNodeJumpDef) {
	if vid == 0 {
		return nil
	}
	if vid >= uint(len(n.JumpTable)) {
		return nil
	}
	return n.JumpTable[vid]
}

func (f *AccountSearchTrieNode) SetAddrLoc(registers *RegisterFile) {
	cmptypes.MyAssert(f.LRNode.IsRLNode)
	cmptypes.MyAssert(f.AddrLoc == nil)
	f.AddrLoc = f.getAddrLoc(registers)
}

func (f *AccountSearchTrieNode) CheckAddrLoc(registers *RegisterFile) {
	cmptypes.MyAssert(f.AddrLoc != nil)
	addrLoc := f.getAddrLoc(registers)
	if addrLoc.Field != f.AddrLoc.Field {
		cmptypes.MyAssert(false, "Unequal Field %v %v", addrLoc.Field, f.AddrLoc.Field)
	}
	if addrLoc.Address != f.AddrLoc.Address {
		cmptypes.MyAssert(false, "Unequal Address %v %v", addrLoc.Address, f.AddrLoc.Address)
	}
	if addrLoc.Loc != f.AddrLoc.Loc {
		cmptypes.MyAssert(false, "Unequal Loc %v %v", addrLoc.Loc, f.AddrLoc.Loc)
	}
}

func (f *AccountSearchTrieNode) getAddrLoc(registers *RegisterFile) *cmptypes.AddrLocation {
	if f.LRNode.Op.isReadOp {
		switch f.LRNode.Op.config.variant {
		case BLOCK_COINBASE:
			return &cmptypes.AddrLocation{Field: cmptypes.Coinbase}
		case BLOCK_TIMESTAMP:
			return &cmptypes.AddrLocation{Field: cmptypes.Timestamp}
		case BLOCK_NUMBER:
			return &cmptypes.AddrLocation{Field: cmptypes.Number}
		case BLOCK_DIFFICULTY:
			return &cmptypes.AddrLocation{Field: cmptypes.Difficulty}
		case BLOCK_GASLIMIT:
			return &cmptypes.AddrLocation{Field: cmptypes.GasLimit}
		case BLOCK_HASH:
			cmptypes.MyAssert(len(f.LRNode.InputVals) == 1)
			num := f.LRNode.GetInputVal(0, registers).(*MultiTypedValue).GetBigInt(nil).Uint64()
			return &cmptypes.AddrLocation{Field: cmptypes.Blockhash, Loc: num}
		default:
			panic("Unknown fRead variant")
		}

	} else if f.LRNode.Op.isLoadOp {
		cmptypes.MyAssert(len(f.LRNode.InputVals) > 0)
		addr := f.LRNode.GetInputVal(0, registers).(*MultiTypedValue).GetAddress()

		switch f.LRNode.Op.config.variant {
		case ACCOUNT_NONCE:
			return &cmptypes.AddrLocation{Field: cmptypes.Nonce, Address: addr}
		case ACCOUNT_BALANCE:
			return &cmptypes.AddrLocation{Field: cmptypes.Balance, Address: addr}
		case ACCOUNT_EXIST:
			return &cmptypes.AddrLocation{Field: cmptypes.Exist, Address: addr}
		case ACCOUNT_EMPTY:
			return &cmptypes.AddrLocation{Field: cmptypes.Empty, Address: addr}
		case ACCOUNT_CODEHASH:
			return &cmptypes.AddrLocation{Field: cmptypes.CodeHash, Address: addr}
		case ACCOUNT_CODESIZE:
			return &cmptypes.AddrLocation{Field: cmptypes.CodeSize, Address: addr}
		case ACCOUNT_CODE:
			return &cmptypes.AddrLocation{Field: cmptypes.Code, Address: addr}
		case ACCOUNT_STATE:
			cmptypes.MyAssert(len(f.LRNode.InputVals) == 2)
			return &cmptypes.AddrLocation{Field: cmptypes.Storage, Address: addr}
		case ACCOUNT_COMMITTED_STATE:
			cmptypes.MyAssert(len(f.LRNode.InputVals) == 2)
			return &cmptypes.AddrLocation{Field: cmptypes.CommittedStorage, Address: addr}
		default:
			panic("Unknown fLoad variant!")
		}

	} else {
		cmptypes.MyAssert(false, "Should never reach here")
	}
	return nil
}

type FieldNodeJumpDef struct {
	Dest                *FieldSearchTrieNode
	RegisterSnapshot    *RegisterFile
	FieldHistorySegment RegisterSegment
	InJumpNode          *FieldSearchTrieNode
	InJumpVId           uint
	IsAJump             bool
	SRefCount
}

func (jd *FieldNodeJumpDef) RemoveRef(roundID uint64) {
	newRefCount := jd.SRemoveRef()
	if newRefCount == 0 {
		if jd.InJumpNode != nil && jd.InJumpNode.GetRefCount() != 0 {
			cmptypes.MyAssert(jd.InJumpVId != 0)
			jt := jd.InJumpNode.AJumpTable
			if !jd.IsAJump {
				jt = jd.InJumpNode.FJumpTable
			}
			cmptypes.MyAssert(jt[jd.InJumpVId] != nil)
			cmptypes.MyAssert(jt[jd.InJumpVId] == jd)
			jt[jd.InJumpVId] = nil
		}
	}
}

type FieldSearchTrieNode struct {
	LRNode     *SNode
	AJumpTable []*FieldNodeJumpDef
	FJumpTable []*FieldNodeJumpDef
	Exit       *SNode
	Rounds     []*cache.PreplayResult
	AddrLoc    *cmptypes.AddrLocation
	IsStore    bool
	IsLoad     bool
	IsRead     bool
	SRefCount
	BigSize
}

func NewFieldSearchTrieNode(node *SNode) *FieldSearchTrieNode {
	fn := &FieldSearchTrieNode{
		LRNode:  node,
		IsStore: node.Op.isStoreOp,
		IsLoad:  node.Op.isLoadOp,
		IsRead:  node.Op.isReadOp,
	}
	return fn
}

func (n *FieldSearchTrieNode) RemoveRef(roundID uint64) {
	n.SRemoveRef()
	if n.Rounds != nil {
		_removeRound(n.Rounds, roundID)
	}
}

func (f *FieldSearchTrieNode) GetOrCreateFJumpDef(fvid uint) (ret *FieldNodeJumpDef) {
	f.FJumpTable, ret = f.getOrCreateJumpDef(f.FJumpTable, fvid)
	return
}

func (f *FieldSearchTrieNode) GetOrCreateAJumpDef(avid uint) (ret *FieldNodeJumpDef) {
	f.AJumpTable, ret = f.getOrCreateJumpDef(f.AJumpTable, avid)
	ret.IsAJump = true
	return
}

func (f *FieldSearchTrieNode) GetAJumpDef(avid uint) (ret *FieldNodeJumpDef) {
	return f.getJumpDef(f.AJumpTable, avid)
}

func (f *FieldSearchTrieNode) GetFJumpDef(fvid uint) (ret *FieldNodeJumpDef) {
	return f.getJumpDef(f.FJumpTable, fvid)
}

func (f *FieldSearchTrieNode) getJumpDef(table []*FieldNodeJumpDef, vid uint) (ret *FieldNodeJumpDef) {
	if vid == 0 {
		return nil
	}
	if vid >= uint(len(table)) {
		return nil
	}
	return table[vid]
}

func (f *FieldSearchTrieNode) getOrCreateJumpDef(jt []*FieldNodeJumpDef, vid uint) ([]*FieldNodeJumpDef, *FieldNodeJumpDef) {
	cmptypes.MyAssert(vid > 0)
	targetLength := vid + 1
	increase := int(targetLength) - len(jt)
	for i := 0; i < increase; i++ {
		jt = append(jt, nil)
	}
	jd := jt[vid]
	if jd == nil {
		jd = &FieldNodeJumpDef{}
		jt[vid] = jd
		jd.InJumpNode = f
		jd.InJumpVId = vid
	}
	return jt, jd
}

func (f *FieldSearchTrieNode) SetAddrLoc(registers *RegisterFile) {
	cmptypes.MyAssert(f.LRNode.IsRLNode)
	cmptypes.MyAssert(f.AddrLoc == nil)
	f.AddrLoc = f.getAddrLoc(registers)
}

func (f *FieldSearchTrieNode) CheckAddrLoc(registers *RegisterFile) {
	cmptypes.MyAssert(f.AddrLoc != nil)
	addrLoc := f.getAddrLoc(registers)
	if addrLoc.Field != f.AddrLoc.Field {
		cmptypes.MyAssert(false, "Unequal Field %v %v", addrLoc.Field, f.AddrLoc.Field)
	}
	if addrLoc.Address != f.AddrLoc.Address {
		cmptypes.MyAssert(false, "Unequal Address %v %v", addrLoc.Address, f.AddrLoc.Address)
	}
	if addrLoc.Loc != f.AddrLoc.Loc {
		cmptypes.MyAssert(false, "Unequal Loc %v %v", addrLoc.Loc, f.AddrLoc.Loc)
	}
}

func (f *FieldSearchTrieNode) getAddrLoc(registers *RegisterFile) *cmptypes.AddrLocation {
	if f.LRNode.Op.isReadOp {
		switch f.LRNode.Op.config.variant {
		case BLOCK_COINBASE:
			return &cmptypes.AddrLocation{Field: cmptypes.Coinbase}
		case BLOCK_TIMESTAMP:
			return &cmptypes.AddrLocation{Field: cmptypes.Timestamp}
		case BLOCK_NUMBER:
			return &cmptypes.AddrLocation{Field: cmptypes.Number}
		case BLOCK_DIFFICULTY:
			return &cmptypes.AddrLocation{Field: cmptypes.Difficulty}
		case BLOCK_GASLIMIT:
			return &cmptypes.AddrLocation{Field: cmptypes.GasLimit}
		case BLOCK_HASH:
			cmptypes.MyAssert(len(f.LRNode.InputVals) == 1)
			num := f.LRNode.GetInputVal(0, registers).(*MultiTypedValue).GetBigInt(nil).Uint64()
			return &cmptypes.AddrLocation{Field: cmptypes.Blockhash, Loc: num}
		default:
			panic("Unknown fRead variant")
		}

	} else if f.LRNode.Op.isLoadOp {
		cmptypes.MyAssert(len(f.LRNode.InputVals) > 0)
		addr := f.LRNode.GetInputVal(0, registers).(*MultiTypedValue).GetAddress()

		switch f.LRNode.Op.config.variant {
		case ACCOUNT_NONCE:
			return &cmptypes.AddrLocation{Field: cmptypes.Nonce, Address: addr}
		case ACCOUNT_BALANCE:
			return &cmptypes.AddrLocation{Field: cmptypes.Balance, Address: addr}
		case ACCOUNT_EXIST:
			return &cmptypes.AddrLocation{Field: cmptypes.Exist, Address: addr}
		case ACCOUNT_EMPTY:
			return &cmptypes.AddrLocation{Field: cmptypes.Empty, Address: addr}
		case ACCOUNT_CODEHASH:
			return &cmptypes.AddrLocation{Field: cmptypes.CodeHash, Address: addr}
		case ACCOUNT_CODESIZE:
			return &cmptypes.AddrLocation{Field: cmptypes.CodeSize, Address: addr}
		case ACCOUNT_CODE:
			return &cmptypes.AddrLocation{Field: cmptypes.Code, Address: addr}
		case ACCOUNT_STATE:
			cmptypes.MyAssert(len(f.LRNode.InputVals) == 2)
			key := f.LRNode.GetInputVal(1, registers).(*MultiTypedValue).GetHash()
			return &cmptypes.AddrLocation{Field: cmptypes.Storage, Address: addr, Loc: key}
		case ACCOUNT_COMMITTED_STATE:
			cmptypes.MyAssert(len(f.LRNode.InputVals) == 2)
			key := f.LRNode.GetInputVal(1, registers).(*MultiTypedValue).GetHash()
			return &cmptypes.AddrLocation{Field: cmptypes.CommittedStorage, Address: addr, Loc: key}
		default:
			panic("Unknown fLoad variant!")
		}

	} else {
		cmptypes.MyAssert(false, "Should never reach here")
	}
	return nil
}

type OpNodeJumpDef struct {
	Dest                *SNode
	Next                *OpSearchTrieNode
	RegisterSegment     RegisterSegment
	FieldHistorySegment RegisterSegment
	InJumpNode          *OpSearchTrieNode
	InJumpKey           OpJumpKey
	InJumpVId           uint
	SRefCount
}

func (jd *OpNodeJumpDef) RemoveRef(roundID uint64) {
	newRefCount := jd.SRemoveRef()
	if newRefCount == 0 {
		if jd.InJumpNode != nil && jd.InJumpNode.GetRefCount() != 0 {
			if len(jd.InJumpNode.VIndices) == 1 {
				if DEBUG_TRACER {
					cmptypes.MyAssert(jd.InJumpKey == EMPTY_JUMP_KEY)
					cmptypes.MyAssert(len(jd.InJumpNode.JumpMap) == 0)
					cmptypes.MyAssert(jd.InJumpVId != 0)
					cmptypes.MyAssert(jd.InJumpNode.JumpTable[jd.InJumpVId] == jd)
				}
				jd.InJumpNode.JumpTable[jd.InJumpVId] = nil
			} else {
				if DEBUG_TRACER {
					cmptypes.MyAssert(jd.InJumpKey != EMPTY_JUMP_KEY)
					cmptypes.MyAssert(jd.InJumpNode.JumpMap[jd.InJumpKey] == jd)
					cmptypes.MyAssert(jd.InJumpVId == 0)
					cmptypes.MyAssert(jd.InJumpNode.JumpTable == nil)
				}
				delete(jd.InJumpNode.JumpMap, jd.InJumpKey)
			}
		}
	}
}

type BigSize struct {
	padding [0]byte
}

const (
	OP_JUMP_KEY_SIZE = 10
)

type OpJumpKey [OP_JUMP_KEY_SIZE]uint

var EMPTY_JUMP_KEY OpJumpKey

type OpSearchTrieNode struct {
	OpNode        *SNode
	IsAccountJump bool // false for field jump
	Exit          *OpNodeJumpDef
	VIndices      []uint
	JumpMap       map[OpJumpKey]*OpNodeJumpDef
	JumpTable     []*OpNodeJumpDef
	SRefCount
	BigSize
}

func NewOpSearchTrieNode(node *SNode) *OpSearchTrieNode {
	fn := &OpSearchTrieNode{
		OpNode:  node,
		JumpMap: make(map[OpJumpKey]*OpNodeJumpDef),
	}
	return fn
}

func (n *OpSearchTrieNode) RemoveRef(roundID uint64) {
	n.SRemoveRef()
}

func (f *OpSearchTrieNode) getOrCreateMapJumpDef(key OpJumpKey) (*OpNodeJumpDef, bool) {
	if len(f.VIndices) == 1 {
		vid := key[0]
		if DEBUG_TRACER {
			cmptypes.MyAssert(vid > 0)
			for i := 1; i < OP_JUMP_KEY_SIZE; i++ {
				cmptypes.MyAssert(key[i] == 0)
			}
		}
		targetLength := vid + 1
		jt := f.JumpTable
		increase := int(targetLength) - len(jt)
		for i := 0; i < increase; i++ {
			jt = append(jt, nil)
		}
		f.JumpTable = jt
		jd := jt[vid]
		if jd == nil {
			jd = &OpNodeJumpDef{}
			jt[vid] = jd
			jd.InJumpNode = f
			jd.InJumpVId = vid
			return jd, true
		}
		return jd, false
	}
	if jd, ok := f.JumpMap[key]; ok {
		return jd, false
	} else {
		jd = &OpNodeJumpDef{}
		f.JumpMap[key] = jd
		jd.InJumpNode = f
		jd.InJumpKey = key
		return jd, true
	}
}

func (f *OpSearchTrieNode) GetOrCreateMapJumpDef(key OpJumpKey) (ret *OpNodeJumpDef, newCreated bool) {
	return f.getOrCreateMapJumpDef(key)
}

func (f *OpSearchTrieNode) GetMapJumpDef(key *OpJumpKey) (ret *OpNodeJumpDef) {
	if len(f.VIndices) == 1 {
		fVId := (*key)[0]
		if fVId >= uint(len(f.JumpTable)) {
			return nil
		} else {
			return f.JumpTable[fVId]
		}
	}
	return f.JumpMap[*key]
}

type RegisterSegment []interface{}

func NewRegisterSegment(size uint) RegisterSegment {
	rf := make(RegisterSegment, size)
	return rf
}

func NewEmptyRegisterSegmentWithCap(cap uint) RegisterSegment {
	rf := make(RegisterSegment, 0, cap)
	return rf
}

func (rf RegisterSegment) Copy() RegisterSegment {
	cp := make(RegisterSegment, len(rf))
	copy(cp, rf)
	return cp
}

func (rf RegisterSegment) AssertEqual(other RegisterSegment) {
	//cmptypes.MyAssert(len(rf) == len(other))
	smaller := len(rf)
	if smaller > len(other) {
		smaller = len(other)
	}
	for i := 0; i < smaller; i++ {
		AssertEqualRegisterValue(rf[i], other[i])
	}
}

func (rf RegisterSegment) ApplyDelta(deltaVals []interface{}, deltaIndices []uint) {
	for i, index := range deltaIndices {
		rf[index] = deltaVals[i]
	}
}

func (rf RegisterSegment) ApplySnapshot(snapshot RegisterSegment) {
	copy(rf, snapshot)
}

func (rf RegisterSegment) ToRegisterFileSnapshot() *RegisterFile {
	return NewRegisterFileSnapshot(rf)
}

type RegisterFile struct {
	firstSection       RegisterSegment
	firstSectionSize   uint
	secondSection      RegisterSegment
	size               uint
	firstSectionSealed bool
	isSnapshot         bool
}

func NewRegisterFile(capacity uint) *RegisterFile {
	rf := &RegisterFile{
		firstSection:  NewEmptyRegisterSegmentWithCap(capacity),
		secondSection: NewEmptyRegisterSegmentWithCap(capacity),
	}
	rf.firstSection = append(rf.firstSection, 0)
	rf.firstSectionSize = 1
	rf.size = 1
	return rf
}

func NewRegisterFileSnapshot(seg RegisterSegment) *RegisterFile {
	return &RegisterFile{
		firstSection:       seg,
		firstSectionSize:   uint(len(seg)),
		secondSection:      nil,
		size:               uint(len(seg)),
		firstSectionSealed: true,
		isSnapshot:         true,
	}
}

func (rf *RegisterFile) Cap() uint {
	return uint(cap(rf.firstSection))
}

func (rf *RegisterFile) Get(index uint) interface{} {
	if index < rf.firstSectionSize {
		if DEBUG_TRACER {
			cmptypes.MyAssert(rf.firstSectionSize == uint(len(rf.firstSection)))
			cmptypes.MyAssert(rf.size == rf.firstSectionSize+uint(len(rf.secondSection)))
		}
		return rf.firstSection[index]
	} else {
		if DEBUG_TRACER {
			cmptypes.MyAssert(rf.isSnapshot == false)
			cmptypes.MyAssert(rf.firstSectionSize == uint(len(rf.firstSection)))
			cmptypes.MyAssert(rf.size == rf.firstSectionSize+uint(len(rf.secondSection)))
		}
		return rf.secondSection[index-rf.firstSectionSize]
	}
}

func (rf *RegisterFile) AppendRegister(index uint, val interface{}) {
	if !rf.firstSectionSealed {
		if DEBUG_TRACER {
			cmptypes.MyAssert(index == rf.size)
			cmptypes.MyAssert(rf.isSnapshot == false)
			cmptypes.MyAssert(rf.firstSectionSize == uint(len(rf.firstSection)))
			cmptypes.MyAssert(rf.size == rf.firstSectionSize+uint(len(rf.secondSection)))
		}
		rf.firstSection = append(rf.firstSection, val)
		rf.firstSectionSize++
		rf.size++
	} else {
		if DEBUG_TRACER {
			cmptypes.MyAssert(index == rf.size)
			cmptypes.MyAssert(rf.isSnapshot == false)
			cmptypes.MyAssert(rf.firstSectionSize == uint(len(rf.firstSection)))
			cmptypes.MyAssert(rf.size == rf.firstSectionSize+uint(len(rf.secondSection)))
		}
		rf.secondSection = append(rf.secondSection, val)
		rf.size++
	}
}

func (rf *RegisterFile) ApplySnapshot(snapshot *RegisterFile) {
	if DEBUG_TRACER {
		cmptypes.MyAssert(snapshot.firstSectionSealed && snapshot.size == snapshot.firstSectionSize)
		cmptypes.MyAssert(snapshot.isSnapshot == true)
		cmptypes.MyAssert(snapshot.secondSection == nil)
		cmptypes.MyAssert(rf.size == rf.firstSectionSize)
		cmptypes.MyAssert(rf.isSnapshot == false)
		cmptypes.MyAssert(rf.firstSectionSize == uint(len(rf.firstSection)))
		cmptypes.MyAssert(rf.size == rf.firstSectionSize+uint(len(rf.secondSection)))
	}
	rf.firstSection = snapshot.firstSection
	rf.firstSectionSize = snapshot.firstSectionSize
	rf.firstSectionSealed = snapshot.firstSectionSealed
	rf.size = snapshot.size
}

func (rf *RegisterFile) AppendSegment(index uint, seg RegisterSegment) {
	if DEBUG_TRACER {
		cmptypes.MyAssert(rf.size == index)
		cmptypes.MyAssert(rf.isSnapshot == false)
		cmptypes.MyAssert(rf.firstSectionSize == uint(len(rf.firstSection)))
		cmptypes.MyAssert(rf.size == rf.firstSectionSize+uint(len(rf.secondSection)))
	}
	if rf.firstSectionSealed {
		rf.secondSection = append(rf.secondSection, seg...)
		rf.size += uint(len(seg))
	} else {
		rf.firstSection = append(rf.firstSection, seg...)
		rf.firstSectionSize += uint(len(seg))
		rf.size += uint(len(seg))
	}
}

func (rf *RegisterFile) Slice(si uint, di uint) RegisterSegment {
	if DEBUG_TRACER {
		cmptypes.MyAssert(!rf.firstSectionSealed && rf.size == rf.firstSectionSize)
		cmptypes.MyAssert(si < rf.firstSectionSize)
		cmptypes.MyAssert(di <= rf.firstSectionSize)
		cmptypes.MyAssert(rf.isSnapshot == false)
		cmptypes.MyAssert(rf.firstSectionSize == uint(len(rf.firstSection)))
		cmptypes.MyAssert(rf.size == rf.firstSectionSize+uint(len(rf.secondSection)))
	}
	return rf.firstSection[si:di]
}

func (rf *RegisterFile) AssertSnapshotEqual(other *RegisterFile) {
	cmptypes.MyAssert(rf.size == other.size)
	cmptypes.MyAssert(rf.firstSectionSealed && other.firstSectionSealed)
	cmptypes.MyAssert(rf.isSnapshot)
	cmptypes.MyAssert(other.isSnapshot)

	for i := uint(1); i < rf.size; i++ {
		AssertEqualRegisterValue(rf.Get(i), other.Get(i))
	}
}

func AssertEqualRegisterValue(a, b interface{}) {
	switch ta := a.(type) {
	case *big.Int:
		panic("Should never see raw big.Int in register")
	case *MultiTypedValue:
		tb := b.(*MultiTypedValue)
		aStr := ta.GetCacheKey()
		bStr := tb.GetCacheKey()
		cmptypes.MyAssert(aStr == bStr, "%v, %v", a, b)
		return
	case []byte:
		tb := b.([]byte)
		cmptypes.MyAssert(string(ta) == string(tb), "%v : %v", a, b)
		return
	case *StateIDM:
		tb := b.(*StateIDM)
		cmptypes.MyAssert(len(ta.mapping) == len(tb.mapping), "%v : %v", a, b)
		for k, v := range ta.mapping {
			cmptypes.MyAssert(tb.mapping[k] == v, "%v : %v", a, b)
		}
		return
	case *AddrIDM:
		tb := b.(*AddrIDM)
		cmptypes.MyAssert(len(ta.mapping) == len(tb.mapping), "%v : %v", a, b)
		for k, v := range ta.mapping {
			cmptypes.MyAssert(tb.mapping[k] == v, "%v : %v", a, b)
		}
		return
	case *BlockHashNumIDM:
		tb := b.(*BlockHashNumIDM)
		cmptypes.MyAssert(len(ta.mapping) == len(tb.mapping), "%v : %v", a, b)
		for k, v := range ta.mapping {
			cmptypes.MyAssert(tb.mapping[k] == v, "%v : %v", a, b)
		}
		return
	}
	cmptypes.MyAssert(a == b, "%v : %v", a, b)
}

type RoundMapping struct {
	AccountDepsToRounds    map[string][]*cache.PreplayResult
	FieldDepsToRounds      map[string][]*cache.PreplayResult
	StoreValuesToRounds    map[string][]*cache.PreplayResult
	AccountDepsToFieldDeps map[string]string
	FieldDepsToStoreValues map[string]string
}

func GetDepsKey(depIndices []uint, history *RegisterFile, debug bool) (string, bool, []uint) {
	buf := strings.Builder{}
	var fvids []uint
	if debug {
		fvids = make([]uint, len(depIndices))
	}
	cmptypes.MyAssert(reflect.TypeOf(uint(0)).Size() == 8)
	for i, index := range depIndices {
		vId := history.Get(index).(uint)
		if vId == 0 {
			return "", false, nil
		} else {
			buf.Write(((*[8]byte)(unsafe.Pointer(&(vId))))[:])
			if debug {
				fvids[i] = vId
			}
		}
	}
	return buf.String(), true, fvids
}

func GetValsKey(valRegisterIds []uint, registers *RegisterFile) string {
	buf := strings.Builder{}
	buf.Grow(200)
	for _, rid := range valRegisterIds {
		v := registers.Get(rid)
		switch tv := v.(type) {
		case *big.Int:
			panic("Should never see raw big.Int")
		case *MultiTypedValue:
			buf.Write(tv.GetBytes())
		case []byte:
			buf.Write(tv)
		case uint64:
			buf.Write((*[8]byte)(unsafe.Pointer(&(tv)))[:])
		default:
			panic(fmt.Sprintf("Unknown val types %v", reflect.TypeOf(v).Name()))
		}
		buf.WriteString(";")
	}
	return buf.String()
}

type DepAVIdToRoundMapping map[uint]*RoundMapping

type StoreInfo struct {
	rounds                                      []*cache.PreplayResult
	AccountIndexToAddressOrRId                  map[uint]interface{} //common.Address
	StoreAccountIndices                         []uint
	StoreAccountIndexToStoreFieldDependencies   map[uint][]uint
	StoreAccountIndexToStoreAccountDependencies map[uint][]uint
	StoreAccountIndexToStoreVarRegisterIndices  map[uint][]uint
	StoreAccountIndexToAVIdToRoundMapping       map[uint]DepAVIdToRoundMapping
	StoreNodeIndexToAccountIndex                map[uint]uint
}

func NewStoreInfo() *StoreInfo {
	return &StoreInfo{StoreAccountIndexToAVIdToRoundMapping: make(map[uint]DepAVIdToRoundMapping)}
}

func (rr *StoreInfo) AddRound(round *cache.PreplayResult) {
	for _, r := range rr.rounds {
		if r == nil {
			continue
		}
		cmptypes.MyAssert(r.RoundID != round.RoundID)
		cmptypes.MyAssert(r.Receipt.GasUsed == round.Receipt.GasUsed)
		cmptypes.MyAssert(r.Receipt.Status == round.Receipt.Status)
		cmptypes.MyAssert(len(r.Receipt.Logs) == len(round.Receipt.Logs))
	}
	rr.rounds = append(rr.rounds, round)
}

func AssertAAMapEqual(a, b map[uint]interface{}) {
	cmptypes.MyAssert(len(a) == len(b))
	for k, va := range a {
		vb, ok := b[k]
		cmptypes.MyAssert(ok)
		cmptypes.MyAssert(vb == va)
	}
}

func (rr *StoreInfo) SetupRoundMapping(registers *RegisterFile, accounts *RegisterFile, fields *RegisterFile, round *cache.PreplayResult, debugOut DebugOutFunc) {
	debug := debugOut != nil
	for _, aIndex := range rr.StoreAccountIndices {
		aVId := accounts.Get(aIndex).(uint)
		cmptypes.MyAssert(aVId != 0)
		d2rm := rr.StoreAccountIndexToAVIdToRoundMapping[aIndex]
		if d2rm == nil {
			d2rm = make(DepAVIdToRoundMapping)
			rr.StoreAccountIndexToAVIdToRoundMapping[aIndex] = d2rm
		}
		rm := d2rm[aVId]
		if rm == nil {
			rm = &RoundMapping{
				AccountDepsToRounds:    make(map[string][]*cache.PreplayResult),
				FieldDepsToRounds:      make(map[string][]*cache.PreplayResult),
				StoreValuesToRounds:    make(map[string][]*cache.PreplayResult),
				AccountDepsToFieldDeps: make(map[string]string),
				FieldDepsToStoreValues: make(map[string]string),
			}
			d2rm[aVId] = rm
		}

		var addr common.Address
		addrOrRId := rr.AccountIndexToAddressOrRId[aIndex]
		switch a_rid := addrOrRId.(type) {
		case common.Address:
			addr = a_rid
		case uint:
			addr = registers.Get(a_rid).(*MultiTypedValue).GetAddress()
		default:
			panic(fmt.Sprintf("Unknow type %v", reflect.TypeOf(addrOrRId).Name()))
		}

		if true {
			accountDeps := rr.StoreAccountIndexToStoreAccountDependencies[aIndex]
			adk, aok, avids := GetDepsKey(accountDeps, accounts, debug)
			cmptypes.MyAssert(aok)
			arounds := rm.AccountDepsToRounds[adk]
			arounds = append(arounds, round)
			rm.AccountDepsToRounds[adk] = arounds

			if debugOut != nil {
				debugOut("AccountDepsToRound AIndex %v AVId %v addr %v ADeps %v with %v klen %v To Round %v\n",
					aIndex, aVId, addr.Hex(), accountDeps, avids, len(adk), round.RoundID)
			}
		}

		if true {
			fieldDeps := rr.StoreAccountIndexToStoreFieldDependencies[aIndex]
			afk, fok, fvids := GetDepsKey(fieldDeps, fields, debug)
			cmptypes.MyAssert(fok)
			frounds := rm.FieldDepsToRounds[afk]
			frounds = append(frounds, round)
			rm.FieldDepsToRounds[afk] = frounds

			//if bfk, bfok := rm.AccountDepsToFieldDeps[adk]; bfok {
			//	cmptypes.MyAssert(bfk == afk)
			//} else {
			//	rm.AccountDepsToFieldDeps[adk] = afk
			//}

			if debugOut != nil {
				debugOut("FieldDepsToRound AIndex %v AVId %v addr %v FDeps %v with %v klen %v To Round %v\n",
					aIndex, aVId, addr.Hex(), fieldDeps, fvids, len(afk), round.RoundID)
			}
		}

		if true {
			varRIndices := rr.StoreAccountIndexToStoreVarRegisterIndices[aIndex]
			vk := GetValsKey(varRIndices, registers)
			vrounds := rm.StoreValuesToRounds[vk]
			vrounds = append(vrounds, round)
			rm.StoreValuesToRounds[vk] = vrounds

			//if bvk, bvok := rm.FieldDepsToStoreValues[afk]; bvok {
			//	cmptypes.MyAssert(bvk == vk)
			//} else {
			//	rm.FieldDepsToStoreValues[afk] = vk
			//}

			if debugOut != nil {
				debugOut("FieldDepsToRound AIndex %v AVId %v addr %v varIndices %v klen %v To Round %v\n",
					aIndex, aVId, addr.Hex(), varRIndices, len(vk), round.RoundID)
			}
		}
	}
}

func (rr *StoreInfo) AddAccountMatchMapping(trace *STrace) {
	if rr.AccountIndexToAddressOrRId == nil {
		rr.AccountIndexToAddressOrRId = trace.AccountIndexToAddressOrRId
	} else {
		if DEBUG_TRACER {
			AssertAAMapEqual(rr.AccountIndexToAddressOrRId, trace.AccountIndexToAddressOrRId)
		}
	}

	if rr.StoreAccountIndices == nil {
		rr.StoreAccountIndices = trace.StoreAccountIndices
	} else {
		if DEBUG_TRACER {
			AssertUArrayEqual(rr.StoreAccountIndices, trace.StoreAccountIndices)
		}
	}

	if rr.StoreAccountIndexToStoreFieldDependencies == nil {
		rr.StoreAccountIndexToStoreFieldDependencies = trace.StoreAccountIndexToStoreFieldDependencies
	} else {
		if DEBUG_TRACER {
			AssertADMapEqual(rr.StoreAccountIndexToStoreFieldDependencies, trace.StoreAccountIndexToStoreFieldDependencies)
		}
	}

	if rr.StoreAccountIndexToStoreAccountDependencies == nil {
		rr.StoreAccountIndexToStoreAccountDependencies = trace.StoreAccountIndexToStoreAccountDependencies
	} else {
		if DEBUG_TRACER {
			AssertADMapEqual(rr.StoreAccountIndexToStoreAccountDependencies, trace.StoreAccountIndexToStoreAccountDependencies)
		}
	}

	if rr.StoreAccountIndexToStoreVarRegisterIndices == nil {
		rr.StoreAccountIndexToStoreVarRegisterIndices = trace.StoreAccountIndexToStoreVarRegisterIndices
	} else {
		if DEBUG_TRACER {
			AssertADMapEqual(rr.StoreAccountIndexToStoreVarRegisterIndices, trace.StoreAccountIndexToStoreVarRegisterIndices)
		}
	}

	if rr.StoreNodeIndexToAccountIndex == nil {
		rr.StoreNodeIndexToAccountIndex = trace.StoreNodeIndexToStoreAccountIndex
	} else {
		if DEBUG_TRACER {
			AssertUUMapEqual(rr.StoreNodeIndexToAccountIndex, trace.StoreNodeIndexToStoreAccountIndex)
		}
	}
}

func _removeRound(rounds []*cache.PreplayResult, roundId uint64) {
	for i, round := range rounds {
		if round != nil && round.RoundID == roundId {
			rounds[i] = nil
			return
		}
	}
}

func (rr *StoreInfo) ClearRound(roundId uint64) {
	_removeRound(rr.rounds, roundId)
	for _, a := range rr.StoreAccountIndexToAVIdToRoundMapping {
		for _, b := range a {
			for _, rs := range b.StoreValuesToRounds {
				_removeRound(rs, roundId)
			}
			for _, rs := range b.AccountDepsToRounds {
				_removeRound(rs, roundId)
			}
			for _, rs := range b.FieldDepsToRounds {
				_removeRound(rs, roundId)
			}
		}
	}

}

func AssertUUMapEqual(a map[uint]uint, b map[uint]uint) {
	cmptypes.MyAssert(len(a) == len(b))
	for k, va := range a {
		vb, ok := b[k]
		cmptypes.MyAssert(ok)
		cmptypes.MyAssert(va == vb)
	}
}

func AssertADMapEqual(a map[uint][]uint, b map[uint][]uint) {
	cmptypes.MyAssert(len(a) == len(b))
	for k, va := range a {
		vb, ok := b[k]
		cmptypes.MyAssert(ok)
		AssertUArrayEqual(va, vb)
	}
}

func AssertUArrayEqual(indices []uint, indices2 []uint) {
	cmptypes.MyAssert(len(indices) == len(indices2))
	for i, v := range indices {
		cmptypes.MyAssert(v == indices2[i])
	}
}

type SNode struct {
	Op                                     *OpDef
	OpSeq                                  uint64
	InputVals                              []interface{}
	InputRegisterIndices                   []uint
	OutputRegisterIndex                    uint
	Statement                              *Statement
	Next                                   *SNode
	GuardedNext                            map[interface{}]*SNode
	Seq                                    uint
	BeforeNextGuardNewRegisterSize         int
	FieldNodeType                          cmptypes.Field
	FieldValueToID                         map[interface{}]uint
	FastFieldValueToID                     interface{} //map[interface{}]uint
	AccountValueToID                       map[interface{}]uint
	FastAccountValueToID                   interface{} //
	IsANode                                bool
	IsTLNode                               bool
	IsRLNode                               bool
	IsJSPNode                              bool
	AccountIndex                           uint
	FieldIndex                             uint
	FieldDependencies                      InputDependence
	FieldDependenciesExcludingSelfAccount  InputDependence
	FieldDependencyStatements              []*Statement
	AccountDependencies                    InputDependence
	AccountDepedenciesExcludingSelfAccount InputDependence
	JumpHead                               *OpSearchTrieNode
	Prev                                   *SNode
	PrevGuardKey                           interface{}
	BeforeRegisterSize                     uint
	BeforeFieldHistorySize                 uint
	BeforeAccounHistorySize                uint
	roundResults                           *StoreInfo
	SRefCount
	BigSize
}

func NewSNode(s *Statement, nodeIndex uint, st *STrace, prev *SNode, prevGuardKey interface{}) *SNode {
	registerMapping := st.RAlloc
	n := &SNode{
		Op:                             s.op,
		OpSeq:                          s.opSeq,
		InputVals:                      make([]interface{}, len(s.inputs)),
		InputRegisterIndices:           make([]uint, len(s.inputs)),
		Statement:                      s,
		Next:                           nil,
		GuardedNext:                    nil,
		BeforeNextGuardNewRegisterSize: -1,
		Seq:                            nodeIndex,
		Prev:                           prev,
		PrevGuardKey:                   prevGuardKey,
	}

	if prev != nil && prev.OpSeq > n.OpSeq { // transformations might reorder statements
		n.OpSeq = prev.OpSeq
	}

	if !DEBUG_TRACER {
		n.Statement = nil
	}

	st.ComputeDepAndJumpInfo()

	if n.Op.isReadOp || n.Op.isLoadOp {
		n.FieldNodeType = n.Op.GetNodeType()
		if st.ANodeSet[nodeIndex] {
			n.AccountIndex = st.ANodeIndexToAccountIndex[nodeIndex]
			n.IsANode = true
			n.AccountValueToID = make(map[interface{}]uint)
			n.FastAccountValueToID = NewAccountValueToID(n.FieldNodeType)
		} else {
			n.AccountIndex = st.RLNodeIndexToAccountIndex[nodeIndex]
			n.IsTLNode = st.TLNodeSet[nodeIndex]
		}
		n.IsRLNode = true
		n.FieldIndex = st.RLNodeIndexToFieldIndex[nodeIndex]
		n.FastFieldValueToID = NewFieldValueToID(n.FieldNodeType)
		n.FieldValueToID = make(map[interface{}]uint)
	}

	if n.IsANode {
		cmptypes.MyAssert(n.AccountIndex != 0)
		n.BeforeAccounHistorySize = n.AccountIndex
	} else {
		if n.Prev.IsANode {
			n.BeforeAccounHistorySize = n.Prev.BeforeAccounHistorySize + 1
		} else {
			n.BeforeAccounHistorySize = n.Prev.BeforeAccounHistorySize
		}
	}

	if n.IsRLNode {
		cmptypes.MyAssert(n.FieldIndex != 0)
		n.BeforeFieldHistorySize = n.FieldIndex
	} else {
		if n.Prev.IsRLNode {
			n.BeforeFieldHistorySize = n.Prev.BeforeFieldHistorySize + 1
		} else {
			n.BeforeFieldHistorySize = n.Prev.BeforeFieldHistorySize
		}
	}

	n.FieldDependencies = st.SNodeInputFieldDependencies[nodeIndex]
	n.AccountDependencies = st.SNodeInputAccountDependencies[nodeIndex]
	if n.IsRLNode && !n.IsANode {
		selfFDs := make(InputDependence, 0, len(n.FieldDependencies))
		cmptypes.MyAssert(n.AccountIndex != 0)
		for _, fIndex := range n.FieldDependencies {
			nIndex := st.FieldIndexToRLNodeIndex[fIndex]
			aIndex := st.RLNodeIndexToAccountIndex[nIndex]
			cmptypes.MyAssert(aIndex != 0)
			if aIndex == n.AccountIndex {
				selfFDs = append(selfFDs, fIndex)
			}
		}
		n.FieldDependenciesExcludingSelfAccount = n.FieldDependencies.SetDiff(selfFDs)
		n.AccountDepedenciesExcludingSelfAccount = n.AccountDependencies.SetDiff([]uint{n.AccountIndex})
		//if len(n.FieldDependenciesExcludingSelfAccount) == 0 {
		//	cmptypes.MyAssert(len(n.AccountDepedenciesExcludingSelfAccount) == 0)
		//}
		if len(n.AccountDepedenciesExcludingSelfAccount) == 0 {
			cmptypes.MyAssert(len(n.FieldDependenciesExcludingSelfAccount) == 0)
		}
	}

	n.IsJSPNode = st.JSPSet[nodeIndex]

	for i, v := range s.inputs {
		if v.IsConst() {
			n.InputVals[i] = v.val
			//n.InputRegisterIndices[i] = 0
		} else {
			rid, ok := registerMapping[v.id]
			cmptypes.MyAssert(ok)
			n.InputRegisterIndices[i] = rid
		}
	}

	if s.output != nil && !s.output.IsConst() {
		rid, ok := registerMapping[s.output.id]
		cmptypes.MyAssert(ok)
		n.OutputRegisterIndex = rid
		n.BeforeRegisterSize = rid
	} else {
		cmptypes.MyAssert(n.Prev != nil)
		if n.Prev.OutputRegisterIndex == 0 {
			n.BeforeRegisterSize = n.Prev.BeforeRegisterSize
		} else {
			n.BeforeRegisterSize = n.Prev.BeforeRegisterSize + 1
		}
	}

	if n.Op.IsGuard() {
		n.GuardedNext = make(map[interface{}]*SNode)
		fStats := make([]*Statement, len(n.FieldDependencies))
		for i, fIndex := range n.FieldDependencies {
			fStats[i] = st.Stats[st.FieldIndexToRLNodeIndex[fIndex]]
		}
		n.FieldDependencyStatements = fStats
	}

	return n
}

func (n *SNode) GetInputVal(index uint, registers *RegisterFile) interface{} {
	rid := n.InputRegisterIndices[index]
	if rid == 0 {
		return n.InputVals[index]
	} else {
		return registers.Get(rid)
	}
}

func (n *SNode) RemoveRef(roundID uint64) {
	newRefCount := n.SRemoveRef()
	if newRefCount == 0 {
		if n.Prev != nil && n.Prev.GetRefCount() != 0 {
			cmptypes.MyAssert(n.PrevGuardKey != nil)
			_, ok := n.Prev.GuardedNext[n.PrevGuardKey]
			cmptypes.MyAssert(ok)
			delete(n.Prev.GuardedNext, n.PrevGuardKey)
		}
	}
	if n.roundResults != nil {
		n.roundResults.ClearRound(roundID)
	}
}

func (n *SNode) AddStoreInfo(round *cache.PreplayResult, trace *STrace) {
	cmptypes.MyAssert(n.Op.isStoreOp)
	if n.roundResults == nil {
		n.roundResults = NewStoreInfo()
	}
	n.roundResults.AddRound(round)
	n.roundResults.AddAccountMatchMapping(trace)
}

func (n *SNode) executeWithoutCreateMTV(env *ExecEnv, registers *RegisterFile) interface{} {
	env.inputs = n.InputVals
	for i, rid := range n.InputRegisterIndices {
		if rid != 0 {
			env.inputs[i] = registers.Get(rid)
		}
	}
	env.config = &n.Op.config
	return n.Op.impFuc(env)
}

func (n *SNode) Execute(env *ExecEnv, registers *RegisterFile) interface{} {
	return CreateMultiTypedValueIfNeed(n.executeWithoutCreateMTV(env, registers), env)
}

func (n *SNode) SimpleNameString() string {
	return n.Statement.SimpleNameString()
}

func (n *SNode) RegisterValueString(registers *RegisterFile) string {
	inputs := make([]interface{}, len(n.InputVals))
	copy(inputs, n.InputVals)
	for i, rindex := range n.InputRegisterIndices {
		if rindex != 0 {
			inputs[i] = registers.Get(rindex)
		}
		//inputs[i] = n.Statement.TypeConvert(n.Statement.inputs[i], inputs[i])
	}
	var output interface{}
	if n.Statement.output != nil {
		if n.OutputRegisterIndex != 0 {
			output = registers.Get(n.OutputRegisterIndex)
		} else {
			output = n.Statement.output.val
		}
		//output = n.Statement.TypeConvert(n.Statement.output, output)
	}

	return n.Statement.ValueString(inputs, output)
}

func (n *SNode) GetOrLoadAccountValueID(env *ExecEnv, registers *RegisterFile, accountHistory *RegisterFile, accountValHistory *RegisterFile) (uint, bool) {
	if DEBUG_TRACER {
		cmptypes.MyAssert(n.Op.isLoadOp || n.Op.isReadOp)
	}
	//cmptypes.MyAssert(n.IsANode)
	if accountHistory != nil {
		if n.AccountIndex < accountHistory.size {
			return accountHistory.Get(n.AccountIndex).(uint), false
		}
	}

	var vid uint
	var val interface{}
	if n.Op.isReadOp {
		val = n.Execute(env, registers)
		k := GetGuardKey(val)
		vid = n.AccountValueToID[k]
	} else {
		var addr common.Address
		addr = n.GetInputVal(0, registers).(*MultiTypedValue).GetAddress()
		//if n.InputRegisterIndices[0] == 0 {
		//	addr = n.InputVals[0].(*MultiTypedValue).GetAddress()
		//} else {
		//	addr = registers.Get(n.InputRegisterIndices[0]).(*MultiTypedValue).GetAddress()
		//}
		changedBy, _ := env.state.GetAccountDepValue(addr)
		val = changedBy
		if changedBy == cmptypes.DEFAULT_TXRESID {
			vid = 0
		} else {
			depHash := *changedBy.Hash()
			k := GetGuardKey(depHash)
			vid = n.AccountValueToID[k]
		}
	}

	if accountHistory != nil {
		if DEBUG_TRACER {
			cmptypes.MyAssert(accountHistory.firstSectionSealed == false)
		}
		accountHistory.AppendRegister(n.AccountIndex, vid)
	}
	if accountValHistory != nil {
		if n.Op.isLoadOp {
			a := val.(cmptypes.AccountDepValue)
			if false {
				panic(fmt.Sprintf("%v", a))
			}
		}
		accountValHistory.AppendRegister(n.AccountIndex, val)
	}
	return vid, true
}

func (n *SNode) FastGetOrLoadAccountValueID(env *ExecEnv, addrLoc *cmptypes.AddrLocation, accountHistory *RegisterFile, accountValHistory *RegisterFile) (uint, bool) {
	if DEBUG_TRACER {
		cmptypes.MyAssert(n.Op.isLoadOp || n.Op.isReadOp)
	}
	//cmptypes.MyAssert(n.IsANode)
	if n.AccountIndex < accountHistory.size {
		return accountHistory.Get(n.AccountIndex).(uint), false
	}

	if DEBUG_TRACER {
		cmptypes.MyAssert(n.Op.isLoadOp || n.Op.isReadOp)
		cmptypes.MyAssert(addrLoc.Field == n.FieldNodeType)
	}

	//var rawVal interface{}
	var vid uint
	var val interface{}

	switch n.FieldNodeType {
	case cmptypes.Coinbase:
		cb := env.header.Coinbase
		if accountValHistory != nil {
			val = NewAddressValue(cb, env)
		}
		vid = n.FastAccountValueToID.(AddressFieldValueToID)[cb]
	case cmptypes.Timestamp:
		time := env.header.Time
		if accountValHistory != nil {
			val = NewBigIntValue(env.GetNewBigInt().SetUint64(time), env)
		}
		//rawVal = time
		vid = n.FastAccountValueToID.(Uint64FieldValueToID)[time]
	case cmptypes.Number:
		number := env.header.Number.Uint64()
		if accountValHistory != nil {
			val = NewBigIntValue(env.GetNewBigInt().SetUint64(number), env)
		}
		//rawVal = env.header.Number
		vid = n.FastAccountValueToID.(Uint64FieldValueToID)[number]
	case cmptypes.Difficulty:
		difficulty := env.header.Difficulty.Uint64()
		if accountValHistory != nil {
			val = NewBigIntValue(env.GetNewBigInt().SetUint64(difficulty), env)
		}
		vid = n.FastAccountValueToID.(Uint64FieldValueToID)[difficulty]
	case cmptypes.GasLimit:
		gaslimit := env.header.GasLimit
		if accountValHistory != nil {
			val = NewBigIntValue(env.GetNewBigInt().SetUint64(gaslimit), env)
		}
		vid = n.FastAccountValueToID.(Uint64FieldValueToID)[gaslimit]
	case cmptypes.Blockhash:
		number := addrLoc.Loc.(uint64)
		curBn := env.header.Number.Uint64()
		value := common.Hash{}
		if curBn-number < 257 && number < curBn {
			value = env.getHash(number)
		}
		if accountValHistory != nil {
			val = NewHashValue(value, env)
		}
		//rawVal = value
		vid = n.FastAccountValueToID.(HashFieldValueToID)[value]
	case cmptypes.Exist, cmptypes.Empty, cmptypes.Balance, cmptypes.CodeHash, cmptypes.Storage, cmptypes.CommittedStorage, cmptypes.Nonce, cmptypes.CodeSize,
		cmptypes.Code:
		changedBy, _ := env.state.GetAccountDepValue(addrLoc.Address)
		if accountValHistory != nil {
			val = changedBy
		}
		if changedBy == cmptypes.DEFAULT_TXRESID {
			vid = 0
		} else {
			depHash := *changedBy.Hash()
			vid = n.FastAccountValueToID.(StringFieldValueToID)[depHash]
		}
	default:
		cmptypes.MyAssert(false, "Unknown FieldNodeType %v", n.FieldNodeType)
	}

	if DEBUG_TRACER {
		cmptypes.MyAssert(accountHistory.firstSectionSealed == false)
	}
	accountHistory.AppendRegister(n.AccountIndex, vid)
	if accountValHistory != nil {
		//if n.Op.isLoadOp {
		//	a := val.(cmptypes.AccountDepValue)
		//	if false {
		//		panic(fmt.Sprintf("%v", a))
		//	}
		//}
		accountValHistory.AppendRegister(n.AccountIndex, val)
	}
	return vid, true
}

func (n *SNode) LoadFieldValueID(env *ExecEnv, registers *RegisterFile, fieldHistory *RegisterFile) (uint, interface{}) {
	if DEBUG_TRACER {
		cmptypes.MyAssert(n.Op.isLoadOp || n.Op.isReadOp)
	}
	val := n.Execute(env, registers)
	k := GetGuardKey(val)
	vid := n.FieldValueToID[k]
	if fieldHistory != nil {
		fieldHistory.AppendRegister(n.FieldIndex, vid)
	}
	return vid, val
}

func (n *SNode) DispatchBasedFastLoadFieldValueID(env *ExecEnv, addrLoc *cmptypes.AddrLocation, fieldHistory *RegisterFile) (vid uint) {
	if DEBUG_TRACER {
		cmptypes.MyAssert(n.Op.isLoadOp || n.Op.isReadOp)
		cmptypes.MyAssert(addrLoc.Field == n.FieldNodeType)
	}

	f := dispatchFastLoadFieldFuncTable[addrLoc.Field]
	if f != nil {
		vid = f(env, addrLoc, n.FastFieldValueToID)
	} else {
		cmptypes.MyAssert(false, "Wrong field %d", addrLoc.Field)
	}

	fieldHistory.AppendRegister(n.FieldIndex, vid)
	return
}

func (n *SNode) FastLoadFieldValueID(env *ExecEnv, addrLoc *cmptypes.AddrLocation, fieldHistory *RegisterFile) (vid uint) {
	if DEBUG_TRACER {
		cmptypes.MyAssert(n.Op.isLoadOp || n.Op.isReadOp)
		cmptypes.MyAssert(addrLoc.Field == n.FieldNodeType)
	}

	//var rawVal interface{}

	switch n.FieldNodeType {
	case cmptypes.Coinbase:
		cb := env.header.Coinbase
		//rawVal = cb
		vid = n.FastFieldValueToID.(AddressFieldValueToID)[cb]
	case cmptypes.Timestamp:
		time := env.header.Time
		//rawVal = time
		vid = n.FastFieldValueToID.(Uint64FieldValueToID)[time]
	case cmptypes.Number:
		number := env.header.Number.Uint64()
		//rawVal = env.header.Number
		vid = n.FastFieldValueToID.(Uint64FieldValueToID)[number]
	case cmptypes.Difficulty:
		difficulty := env.header.Difficulty.Uint64()
		//rawVal = env.header.Difficulty
		vid = n.FastFieldValueToID.(Uint64FieldValueToID)[difficulty]
	case cmptypes.GasLimit:
		gaslimit := env.header.GasLimit
		//rawVal = gaslimit
		vid = n.FastFieldValueToID.(Uint64FieldValueToID)[gaslimit]
	case cmptypes.Blockhash:
		number := addrLoc.Loc.(uint64)
		curBn := env.header.Number.Uint64()
		value := common.Hash{}
		if curBn-number < 257 && number < curBn {
			value = env.getHash(number)
		}
		//rawVal = value
		vid = n.FastFieldValueToID.(HashFieldValueToID)[value]
	case cmptypes.Exist:
		value := env.state.Exist(addrLoc.Address)
		//rawVal = value
		vid = n.FastFieldValueToID.(BoolFieldValueToID)[value]
	case cmptypes.Empty:
		value := env.state.Empty(addrLoc.Address)
		//rawVal = value
		vid = n.FastFieldValueToID.(BoolFieldValueToID)[value]
	case cmptypes.Balance:
		balance := env.state.GetBalance(addrLoc.Address)
		k := FastBigToHash(balance)
		//rawVal = balance
		vid = n.FastFieldValueToID.(HashFieldValueToID)[k]
	case cmptypes.Nonce:
		value := env.state.GetNonce(addrLoc.Address)
		//rawVal = value
		vid = n.FastFieldValueToID.(Uint64FieldValueToID)[value]
	case cmptypes.CodeHash:
		value := env.state.GetCodeHash(addrLoc.Address)
		//rawVal = value
		vid = n.FastFieldValueToID.(HashFieldValueToID)[value]
	case cmptypes.CodeSize:
		value := env.state.GetCodeSize(addrLoc.Address)
		//rawVal = value
		vid = n.FastFieldValueToID.(Uint64FieldValueToID)[uint64(value)]
	case cmptypes.Code:
		value := env.state.GetCode(addrLoc.Address)
		k := ImmutableBytesToString(value)
		//rawVal = value
		vid = n.FastFieldValueToID.(StringFieldValueToID)[k]
	case cmptypes.Storage:
		value := env.state.GetState(addrLoc.Address, addrLoc.Loc.(common.Hash))
		//rawVal = value
		vid = n.FastFieldValueToID.(HashFieldValueToID)[value]
	case cmptypes.CommittedStorage:
		value := env.state.GetCommittedState(addrLoc.Address, addrLoc.Loc.(common.Hash))
		//rawVal = value
		vid = n.FastFieldValueToID.(HashFieldValueToID)[value]
	default:
		cmptypes.MyAssert(false, "Unknown FieldNodeType %v", n.FieldNodeType)
	}

	fieldHistory.AppendRegister(n.FieldIndex, vid)
	return
}

func (n *SNode) FastGetFVal(addrLoc *cmptypes.AddrLocation, env *ExecEnv) (val interface{}) {
	if DEBUG_TRACER {
		cmptypes.MyAssert(n.Op.isLoadOp || n.Op.isReadOp)
		cmptypes.MyAssert(addrLoc.Field == n.FieldNodeType)
	}

	switch n.FieldNodeType {
	case cmptypes.Coinbase:
		cb := env.header.Coinbase
		return NewAddressValue(cb, env)
	case cmptypes.Timestamp:
		time := env.header.Time
		return NewBigIntValue(env.GetNewBigInt().SetUint64(time), env)
	case cmptypes.Number:
		number := env.header.Number
		return NewBigIntValue(env.GetNewBigInt().Set(number), env)
	case cmptypes.Difficulty:
		difficulty := env.header.Difficulty
		return NewBigIntValue(env.GetNewBigInt().Set(difficulty), env)
	case cmptypes.GasLimit:
		gasLimit := env.header.GasLimit
		return NewBigIntValue(env.GetNewBigInt().SetUint64(gasLimit), env)
	case cmptypes.Blockhash:
		number := addrLoc.Loc.(uint64)
		curBn := env.header.Number.Uint64()
		value := common.Hash{}
		if curBn-number < 257 && number < curBn {
			value = env.getHash(number)
		}
		return NewHashValue(value, env)
	case cmptypes.Exist:
		value := env.state.Exist(addrLoc.Address)
		return value
	case cmptypes.Empty:
		value := env.state.Empty(addrLoc.Address)
		return value
	case cmptypes.Balance:
		balance := env.state.GetBalance(addrLoc.Address)
		return NewBigIntValue(env.CopyBig(balance), env)
	case cmptypes.Nonce:
		value := env.state.GetNonce(addrLoc.Address)
		return value
	case cmptypes.CodeHash:
		value := env.state.GetCodeHash(addrLoc.Address)
		return NewHashValue(value, env)
	case cmptypes.CodeSize:
		value := env.state.GetCodeSize(addrLoc.Address)
		return NewBigIntValue(env.IntToBig(value), env)
	case cmptypes.Code:
		value := env.state.GetCode(addrLoc.Address)
		return value
	case cmptypes.Storage:
		value := env.state.GetState(addrLoc.Address, addrLoc.Loc.(common.Hash))
		return NewHashValue(value, env)
	case cmptypes.CommittedStorage:
		value := env.state.GetCommittedState(addrLoc.Address, addrLoc.Loc.(common.Hash))
		return NewHashValue(value, env)
	default:
		cmptypes.MyAssert(false, "Unknown FieldNodeType %v", n.FieldNodeType)
	}
	return nil
}

func (n *SNode) GetOrCreateAccountValueID(val interface{}) uint {
	cmptypes.MyAssert(n.IsANode)
	val = GetGuardKey(val)
	if id, ok := n.AccountValueToID[val]; ok {
		return id
	} else {
		id := uint(len(n.AccountValueToID)) + 1
		n.AccountValueToID[val] = id
		return id
	}
}

func (n *SNode) FastGetOrCreateAccountValueID(val interface{}) uint {
	cmptypes.MyAssert(n.IsANode)
	switch n.FieldNodeType {
	case cmptypes.Coinbase:
		k := val.(*MultiTypedValue).GetAddress()
		aToID := n.FastAccountValueToID.(AddressFieldValueToID)
		if id, ok := aToID[k]; ok {
			return id
		} else {
			id := uint(len(aToID)) + 1
			aToID[k] = id
			return id
		}
	case cmptypes.Timestamp, cmptypes.Number, cmptypes.Difficulty, cmptypes.GasLimit:
		tv := val.(*MultiTypedValue).GetBigInt(nil).Uint64()
		aToID := n.FastAccountValueToID.(Uint64FieldValueToID)
		if id, ok := aToID[tv]; ok {
			return id
		} else {
			id := uint(len(aToID)) + 1
			aToID[tv] = id
			return id
		}
	case cmptypes.Blockhash:
		k := val.(*MultiTypedValue).GetHash()
		aToID := n.FastAccountValueToID.(HashFieldValueToID)
		if id, ok := aToID[k]; ok {
			return id
		} else {
			id := uint(len(aToID)) + 1
			aToID[k] = id
			return id
		}
	case cmptypes.Exist, cmptypes.Empty, cmptypes.Balance, cmptypes.CodeHash, cmptypes.Storage, cmptypes.CommittedStorage, cmptypes.Nonce, cmptypes.CodeSize,
		cmptypes.Code:
		k := val.(string)
		aToID := n.FastAccountValueToID.(StringFieldValueToID)
		if id, ok := aToID[k]; ok {
			return id
		} else {
			id := uint(len(aToID)) + 1
			aToID[k] = id
			return id
		}
	default:
		panic("Wrong Field")
	}
	return 0
}

func (n *SNode) GetOrCreateFieldValueID(val interface{}) uint {
	cmptypes.MyAssert(n.Op.isLoadOp || n.Op.isReadOp)
	val = GetGuardKey(val)
	if id, ok := n.FieldValueToID[val]; ok {
		return id
	} else {
		id := uint(len(n.FieldValueToID)) + 1
		n.FieldValueToID[val] = id
		return id
	}
}

func (n *SNode) FastGetOrCreateFieldValueID(val interface{}) uint {
	cmptypes.MyAssert(n.Op.isLoadOp || n.Op.isReadOp)

	switch n.FieldNodeType {
	case cmptypes.Coinbase:
		tv := val.(*MultiTypedValue).GetAddress()
		vToID := n.FastFieldValueToID.(AddressFieldValueToID)
		if id, ok := vToID[tv]; ok {
			return id
		} else {
			id := uint(len(vToID)) + 1
			vToID[tv] = id
			return id
		}
	case cmptypes.Timestamp, cmptypes.Number, cmptypes.Difficulty, cmptypes.GasLimit, cmptypes.CodeSize:
		tv := val.(*MultiTypedValue).GetBigInt(nil).Uint64()
		vToID := n.FastFieldValueToID.(Uint64FieldValueToID)
		if id, ok := vToID[tv]; ok {
			return id
		} else {
			id := uint(len(vToID)) + 1
			vToID[tv] = id
			return id
		}
	case cmptypes.Nonce:
		tv := val.(uint64)
		vToID := n.FastFieldValueToID.(Uint64FieldValueToID)
		if id, ok := vToID[tv]; ok {
			return id
		} else {
			id := uint(len(vToID)) + 1
			vToID[tv] = id
			return id
		}
	case cmptypes.Blockhash, cmptypes.Balance, cmptypes.CodeHash, cmptypes.Storage, cmptypes.CommittedStorage:
		tv := val.(*MultiTypedValue).GetHash()
		vToID := n.FastFieldValueToID.(HashFieldValueToID)
		if id, ok := vToID[tv]; ok {
			return id
		} else {
			id := uint(len(vToID)) + 1
			vToID[tv] = id
			return id
		}
	case cmptypes.Exist, cmptypes.Empty:
		tv := val.(bool)
		vToID := n.FastFieldValueToID.(BoolFieldValueToID)
		if id, ok := vToID[tv]; ok {
			return id
		} else {
			id := uint(len(vToID)) + 1
			vToID[tv] = id
			return id
		}
	case cmptypes.Code:
		tv := ImmutableBytesToString(val.([]byte))
		vToID := n.FastFieldValueToID.(StringFieldValueToID)
		if id, ok := vToID[tv]; ok {
			return id
		} else {
			id := uint(len(vToID)) + 1
			vToID[tv] = id
			return id
		}
	default:
		cmptypes.MyAssert(false, "Wrong Field %v", n.FieldNodeType)
	}
	return 0
}

func (n *SNode) AssertIsomorphic(s *Statement, nodeIndex uint, trace *STrace, params ...interface{}) {
	cmptypes.MyAssert(n.Op == s.op, params...)
	cmptypes.MyAssert(len(n.InputVals) == len(s.inputs), params...)
	cmptypes.MyAssert(n.Seq == nodeIndex, params...)
	if n.Op.IsGuard() {
		cmptypes.MyAssert(n.Statement.inputs[0].varType == s.inputs[0].varType, params...)
	} else {
		for i, v := range s.inputs {
			if v.IsConst() {
				cmptypes.MyAssert(n.InputRegisterIndices[i] == 0, params...)
				AssertEqualRegisterValue(n.InputVals[i], v.val)
			} else {
				registerMapping := trace.RAlloc
				rid, ok := registerMapping[v.id]
				cmptypes.MyAssert(ok, params...)
				cmptypes.MyAssert(n.InputRegisterIndices[i] == rid, params...)
			}
		}
	}
	cmptypes.MyAssert(n.IsANode == trace.ANodeSet[nodeIndex], params...)
	cmptypes.MyAssert(n.IsTLNode == trace.TLNodeSet[nodeIndex], params...)
	cmptypes.MyAssert(n.AccountIndex == trace.RLNodeIndexToAccountIndex[nodeIndex], params...)
	cmptypes.MyAssert(n.FieldIndex == trace.RLNodeIndexToFieldIndex[nodeIndex], params...)
	cmptypes.MyAssert(n.FieldDependencies.Equals(trace.SNodeInputFieldDependencies[nodeIndex]), params...)
	cmptypes.MyAssert(n.AccountDependencies.Equals(trace.SNodeInputAccountDependencies[nodeIndex]), params...)
	cmptypes.MyAssert(n.IsJSPNode == trace.JSPSet[nodeIndex], params...)
	if s.output == nil || s.output.IsConst() {
		cmptypes.MyAssert(n.OutputRegisterIndex == 0)
		cmptypes.MyAssert(n.Prev != nil)
		if n.Prev.OutputRegisterIndex == 0 {
			cmptypes.MyAssert(n.BeforeRegisterSize == n.Prev.BeforeRegisterSize)
		} else {
			cmptypes.MyAssert(n.BeforeRegisterSize == n.Prev.BeforeRegisterSize+1)
		}
	} else {
		cmptypes.MyAssert(n.OutputRegisterIndex != 0)
		cmptypes.MyAssert(n.OutputRegisterIndex == trace.RAlloc[s.output.id])
		cmptypes.MyAssert(n.OutputRegisterIndex == n.BeforeRegisterSize)
	}

	if n.IsRLNode && !n.IsANode {
		cmptypes.MyAssert(n.FieldDependenciesExcludingSelfAccount != nil)
		cmptypes.MyAssert(n.AccountDepedenciesExcludingSelfAccount != nil)
		selfFDs := make(InputDependence, 0, len(n.FieldDependencies))
		cmptypes.MyAssert(n.AccountIndex != 0)
		for _, fIndex := range n.FieldDependencies {
			nIndex := trace.FieldIndexToRLNodeIndex[fIndex]
			aIndex := trace.RLNodeIndexToAccountIndex[nIndex]
			cmptypes.MyAssert(aIndex != 0)
			if aIndex == n.AccountIndex {
				selfFDs = append(selfFDs, fIndex)
			}
		}
		AssertUArrayEqual(n.FieldDependenciesExcludingSelfAccount, n.FieldDependencies.SetDiff(selfFDs))
		AssertUArrayEqual(n.AccountDepedenciesExcludingSelfAccount, n.AccountDependencies.SetDiff([]uint{n.AccountIndex}))
	} else {
		cmptypes.MyAssert(n.FieldDependenciesExcludingSelfAccount == nil)
		cmptypes.MyAssert(n.AccountDepedenciesExcludingSelfAccount == nil)
	}
}

func GetGuardKey(a interface{}) interface{} {
	switch ta := a.(type) {
	case *big.Int:
		panic("Should never see raw big.Int")
	case *MultiTypedValue:
		return ta.GetGuardKey()
	case []byte:
		return ImmutableBytesToString(ta)
	case uint64:
		return ta
	case bool:
		return ta
	//case cmptypes.AccountSnap:
	//	return ta
	case common.Hash:
		return ta
	case uint32:
		return ta
	case int:
		return ta
	case string:
		return ta
	default:
		return nil
		//panic(fmt.Sprintf("Unknown type: %v", reflect.TypeOf(a).Name()))
	}
}

func (n *SNode) GetNextNode(registers *RegisterFile) *SNode {
	if n.Op.IsGuard() {
		k := registers.Get(n.InputRegisterIndices[0])
		if DEBUG_TRACER {
			cmptypes.MyAssert(k != nil)
		}
		gk := GetGuardKey(k)
		next := n.GuardedNext[gk]
		return next
	} else {
		return n.Next
	}
}

func (n *SNode) SetBeforeNextGuardNewRegisterCount() {
	if n.BeforeNextGuardNewRegisterSize >= 0 {
		return
	}
	node := n
	registerCount := 0
	for ; !node.Op.IsGuard() && !node.Op.isStoreOp; {
		if node.OutputRegisterIndex != 0 {
			registerCount++
		}
		node = node.Next
	}
	node = n
	for ; !node.Op.IsGuard() && !node.Op.isStoreOp; {
		cmptypes.MyAssert(node.BeforeNextGuardNewRegisterSize == -1)
		cmptypes.MyAssert(registerCount >= 0)
		node.BeforeNextGuardNewRegisterSize = registerCount
		if node.OutputRegisterIndex != 0 {
			registerCount -= 1
		}
		node = node.Next
	}
}

type JumpDef struct {
	RegisterSnapshot *RegisterFile
	//RegisterDeltaIndices []uint
	//RegisterDeltaVals    []interface{}
	RegisterSegment RegisterSegment
	Dest            *SNode
	outTrackId      uint
}

type TraceTrie struct {
	Head                  *SNode
	Tx                    *types.Transaction
	PathCount             uint
	RegisterSize          uint
	DebugOut              DebugOutFunc
	AccountHead           *AccountSearchTrieNode
	FieldHead             *FieldSearchTrieNode
	PreAllocatedExecEnvs  []*ExecEnv
	PreAllocatedHistory   []*RegisterFile
	PreAllocatedRegisters []*RegisterFile
	RoundRefNodesHead     *TrieRoundRefNodes
	RoundRefNodesTail     *TrieRoundRefNodes
	RefedRoundCount       uint
	TrieNodeCount         int64

	TotalSpecializationDuration time.Duration
	TotalMemoizationDuration    time.Duration
	TotalTraceCount             int64

	LargestDetailReadSetID int64
	RoundCountByPathID     map[uintptr]int
}

func NewTraceTrie(tx *types.Transaction) *TraceTrie {
	tt := &TraceTrie{
		Head:               nil,
		Tx:                 tx,
		PathCount:          0,
		RegisterSize:       0,
		RoundCountByPathID: make(map[uintptr]int),
	}
	return tt
}

func (tt *TraceTrie) Preallocate(size uint) {
	if len(tt.PreAllocatedExecEnvs) < 2 {
		for i := len(tt.PreAllocatedExecEnvs); i < 2; i++ {
			tt.PreAllocatedExecEnvs = append(tt.PreAllocatedExecEnvs, &ExecEnv{})
		}
	}

	for _, env := range tt.PreAllocatedExecEnvs {
		env.PreAllocateObjects(size)
	}

	if len(tt.PreAllocatedHistory) < 4 {
		for i := len(tt.PreAllocatedHistory); i < 4; i++ {
			tt.PreAllocatedHistory = append(tt.PreAllocatedHistory, NewRegisterFile(1000))
		}
	}

	if len(tt.PreAllocatedRegisters) < 2 {
		tt.PreAllocatedRegisters = append(tt.PreAllocatedRegisters, NewRegisterFile(size))
	}

	for i, rf := range tt.PreAllocatedRegisters {
		if rf.Cap() < size {
			tt.PreAllocatedRegisters[i] = NewRegisterFile(size)
		}
	}
}

func (tt *TraceTrie) GetNodeCount() int64 {
	return atomic.LoadInt64(&tt.TrieNodeCount)
}

func (tt *TraceTrie) GetNewExecEnv(db *state.StateDB, header *types.Header,
	getHashFunc vm.GetHashFunc, precompiles map[common.Address]vm.PrecompiledContract, globalCache *cache.GlobalCache, isBlockProcess bool) (env *ExecEnv) {
	if isBlockProcess && len(tt.PreAllocatedExecEnvs) > 0 {
		env = tt.PreAllocatedExecEnvs[0]
		tt.PreAllocatedExecEnvs = tt.PreAllocatedExecEnvs[1:]
	} else {
		env = NewExecEnv()
	}
	env.state = db
	env.header = header
	env.precompiles = precompiles
	env.isProcess = isBlockProcess
	env.getHash = getHashFunc
	return
}

func (tt *TraceTrie) GetNewHistory(isBlockProcess bool) (rf *RegisterFile) {
	if isBlockProcess && len(tt.PreAllocatedHistory) > 0 {
		rf = tt.PreAllocatedHistory[0]
		tt.PreAllocatedHistory = tt.PreAllocatedHistory[1:]
	} else {
		rf = NewRegisterFile(1000)
	}
	return
}

func (tt *TraceTrie) GetNewRegisters(isBlockProcess bool) (rf *RegisterFile) {
	if isBlockProcess && len(tt.PreAllocatedRegisters) > 0 {
		rf = tt.PreAllocatedRegisters[0]
		tt.PreAllocatedRegisters = tt.PreAllocatedRegisters[1:]
	} else {
		rf = NewRegisterFile(tt.RegisterSize)
	}
	return
}

func (tt *TraceTrie) UpdateTraceSize(size uint) {
	if size > tt.RegisterSize {
		tt.RegisterSize = size
	}
	tt.Preallocate(tt.RegisterSize)
}

func GetNodeIndexToAccountSnapMapping(trace *STrace, readDep []*cmptypes.AddrLocValue, result *TraceTrieSearchResult) (map[uint]interface{}, map[uint]string) {
	ret := make(map[uint]interface{})
	var strRet map[uint]string
	if DEBUG_TRACER {
		strRet = make(map[uint]string)
	}

	if result == nil {
		temp := make(map[string]interface{})
		strTemp := make(map[string]string)

		for _, alv := range readDep {
			var key string
			val := alv.Value
			switch alv.AddLoc.Field {
			case cmptypes.Dependence:
				key = alv.AddLoc.Address.Hex()
				if DEBUG_TRACER {
					strTemp[key] = val.(cmptypes.AccountDepValue).String()
				}
				val = *val.(cmptypes.AccountDepValue).Hash()
				temp[key] = val
			}
		}

		for _, nodeIndex := range trace.ANodeIndices {
			s := trace.Stats[nodeIndex]
			var key string
			if s.op.isLoadOp {
				key = s.inputs[0].BAddress().Hex()
				if DEBUG_TRACER {
					cmptypes.MyAssert(temp[key] != nil, "%v : %v", s.SimpleNameString(), key)
					strRet[nodeIndex] = strTemp[key]
				}
				ret[nodeIndex] = temp[key]
			}
		}
	} else {
		for _, nodeIndex := range trace.ANodeIndices {
			s := trace.Stats[nodeIndex]
			accountIndex := trace.ANodeIndexToAccountIndex[nodeIndex]
			if s.op.isLoadOp {
				val := result.accountValHistory.Get(accountIndex).(cmptypes.AccountDepValue)
				if DEBUG_TRACER {
					strRet[nodeIndex] = val.String()
				}
				ret[nodeIndex] = *val.Hash()
			}
		}
	}

	return ret, strRet
}

func (tt *TraceTrie) InsertTrace(trace *STrace, round *cache.PreplayResult, result *TraceTrieSearchResult, cfg *vm.MSRAVMConfig, reuseStatus *cmptypes.ReuseStatus) {
	noOverMatching := cfg.NoOverMatching
	noMemoization := cfg.NoMemoization
	gcMutex.Lock()
	defer gcMutex.Unlock()
	insertStartTime := time.Now()
	tt.DebugOut = trace.debugOut
	stats := trace.Stats
	if len(stats) == 0 {
		return
	}
	preHead := NewSNode(stats[0], 0, trace, nil, nil) // use temporarily to hold pointer to Head; only requirement: not a guard op
	preHead.Next = tt.Head
	n := preHead
	var gk interface{}
	if tt.DebugOut != nil {
		rs := reuseStatus.BaseStatus.String()
		if reuseStatus.BaseStatus == cmptypes.Hit {
			rs += "-" + reuseStatus.HitType.String()
			if reuseStatus.HitType == cmptypes.MixHit {
				rs += "-" + reuseStatus.MixStatus.MixHitType.String()
			} else if reuseStatus.HitType == cmptypes.TraceHit {
				rs += "-" + reuseStatus.TraceStatus.TraceHitType.String()
			}
		}
		tt.DebugOut("InsertTrace for round %v after %v\n", round.RoundID, rs)
	}
	currentTraceNodes := make([]*SNode, len(stats))
	refNodes := make([]ISRefCountNode, len(stats))
	newNodeCount := uint(0)
	for i, s := range stats {
		n, gk = tt.insertStatement(n, gk, s, uint(i), trace, result)
		cmptypes.MyAssert(n.Seq == uint(i))
		//n.Seq = uint(i)
		currentTraceNodes[i] = n
		refNodes[i] = n
		if n.GetRefCount() == 0 {
			newNodeCount++
		}
	}

	pathID := uintptr(unsafe.Pointer(n))

	cmptypes.MyAssert(tt.Head == nil || tt.Head == preHead.Next)
	tt.Head = preHead.Next

	firstStoreNode := currentTraceNodes[trace.FirstStoreNodeIndex]
	firstStoreNode.AddStoreInfo(round, trace)

	tt.TotalTraceCount += 1
	tt.TotalSpecializationDuration += trace.TraceDuration
	tt.TotalSpecializationDuration += time.Since(insertStartTime)

	memoizationStartTime := time.Now()

	var jiRefNodes []ISRefCountNode
	var jiNewNodeCount uint = 0
	if !noMemoization {
		ji := tt.setupJumps(trace, currentTraceNodes, round, result, noOverMatching)
		jiRefNodes = ji.refNodes
		jiNewNodeCount = ji.newNodeCount
	}

	tt.UpdateTraceSize(trace.RegisterFileSize)

	refNodes = append(refNodes, jiRefNodes...)

	tt.TrackRoundRefNodes(refNodes, newNodeCount+jiNewNodeCount, round, pathID)

	tt.TotalMemoizationDuration += time.Since(memoizationStartTime)
}

func (tt *TraceTrie) TrackRoundRefNodes(refNodes []ISRefCountNode, newNodeCount uint, round *cache.PreplayResult, roundPathID uintptr) {
	if newNodeCount > 0 {
		if tt.DebugOut != nil {
			tt.DebugOut("NewRound %v: %v New trieNodes created, round node count: %v, total round count: %v, total trie node count:%v\n",
				round.RoundID, newNodeCount, len(refNodes), tt.RefedRoundCount, tt.TrieNodeCount)
		}
		for _, n := range refNodes {
			rc := n.AddRef()
			cmptypes.MyAssert(rc > 0)
		}

		r := &TrieRoundRefNodes{
			RefNodes:    refNodes,
			RoundID:     round.RoundID,
			Round:       round,
			RoundPathID: roundPathID,
		}

		tt.RoundCountByPathID[roundPathID]++

		tt.RefedRoundCount++
		atomic.AddInt64(&tt.TrieNodeCount, int64(newNodeCount))
		if tt.RoundRefNodesHead == nil {
			cmptypes.MyAssert(tt.RoundRefNodesTail == nil)
			cmptypes.MyAssert(uint(len(refNodes)) == newNodeCount)
			tt.RoundRefNodesHead = r
			tt.RoundRefNodesTail = r
		} else {
			cmptypes.MyAssert(tt.RoundRefNodesTail.Next == nil)
			tt.RoundRefNodesTail.Next = r
			tt.RoundRefNodesTail = r
		}
		tt.GCRoundNodes()
	} else {
		if tt.DebugOut != nil {
			tt.DebugOut("RedundantRound %v: No New TrieNodes Created, %v existing nodes\n", round.RoundID, len(refNodes))
		}
	}
}

var gcMutex sync.Mutex

func (tt *TraceTrie) GCRoundNodes() {
	cmptypes.MyAssert(tt.RoundRefNodesHead != nil && tt.RoundRefNodesTail != nil)
	if tt.RefedRoundCount > uint(config.TXN_PREPLAY_ROUND_LIMIT*10+1) {
		for i := 0; i < 1; i++ {
			head := tt.RoundRefNodesHead
			removed := tt.RemoveRoundRef(head)
			roundCount := tt.RoundCountByPathID[head.RoundPathID]
			if roundCount < 1 {
				panic("roundCount is below 1")
			} else if roundCount == 1 {
				delete(tt.RoundCountByPathID, head.RoundPathID)
			} else {
				tt.RoundCountByPathID[head.RoundPathID]--
			}

			if tt.DebugOut != nil {
				tt.DebugOut("RemovedRound %v: %v trie nodes removed, %v trie nodes remain, %v rounds remain\n",
					head.RoundID, removed, tt.TrieNodeCount, tt.RefedRoundCount)
				if removed > 100000 {
					log.Info(fmt.Sprintf("RemovedRound %v for tx %v: %v trie nodes removed, %v trie nodes remain, %v rounds remain",
						head.RoundID, tt.Tx.Hash().Hex(), removed, tt.TrieNodeCount, tt.RefedRoundCount))
				}
			}
			tt.RoundRefNodesHead = head.Next
		}
		//runtime.GC()
		//m := new(runtime.MemStats)
		//runtime.ReadMemStats(m)
		//log.Info("+Read memory statistics",
		//	"HeapAlloc", common.StorageSize(m.HeapAlloc),
		//	"HeapSys", common.StorageSize(m.HeapSys),
		//	"HeadIdle", common.StorageSize(m.HeapIdle),
		//	"HeapInuse", common.StorageSize(m.HeapInuse),
		//	"NextGC", common.StorageSize(m.NextGC),
		//	"NnmGC", m.NumGC,
		//	"GCCPUFraction", fmt.Sprintf("%.3f%%", m.GCCPUFraction*100),
		//)
		//runtime.GC()
		//runtime.ReadMemStats(m)
		//log.Info("-Read memory statistics",
		//	"HeapAlloc", common.StorageSize(m.HeapAlloc),
		//	"HeapSys", common.StorageSize(m.HeapSys),
		//	"HeadIdle", common.StorageSize(m.HeapIdle),
		//	"HeapInuse", common.StorageSize(m.HeapInuse),
		//	"NextGC", common.StorageSize(m.NextGC),
		//	"NnmGC", m.NumGC,
		//	"GCCPUFraction", fmt.Sprintf("%.3f%%", m.GCCPUFraction*100),
		//)
	}
}

func (tt *TraceTrie) GetActiveRounds() []*cache.PreplayResult {
	rounds := make([]*cache.PreplayResult, 0, tt.RefedRoundCount)
	rr := tt.RoundRefNodesHead
	for rr != nil {
		rounds = append(rounds, rr.Round)
		rr = rr.Next
	}
	return rounds
}

func (tt *TraceTrie) GetActivePathCount() int64 {
	distinctPaths := make(map[uintptr]struct{})
	rr := tt.RoundRefNodesHead
	for rr != nil {
		distinctPaths[rr.RoundPathID] = struct{}{}
		rr = rr.Next
	}
	ret := len(distinctPaths)
	if ret != len(tt.RoundCountByPathID) {
		cmptypes.MyAssert(false, "unsynced path count %v %v", ret, len(tt.RoundCountByPathID))
	}
	return int64(len(tt.RoundCountByPathID))
}

func (tt *TraceTrie) RemoveRoundRef(rf *TrieRoundRefNodes) uint {
	removedNodes := int64(0)
	for _, n := range rf.RefNodes {
		n.RemoveRef(rf.RoundID)
		if n.GetRefCount() == 0 {
			removedNodes++
		}
	}
	tt.RefedRoundCount--
	atomic.AddInt64(&tt.TrieNodeCount, -removedNodes)
	cmptypes.MyAssert(tt.RefedRoundCount >= 0)
	cmptypes.MyAssert(atomic.LoadInt64(&tt.TrieNodeCount) >= 0)
	return uint(removedNodes)
}

type TraceTrieSearchResult struct {
	Node                         *SNode
	hit                          bool
	aborted                      bool
	registers                    *RegisterFile
	fieldHistory                 *RegisterFile
	accountHistory               *RegisterFile
	accountValHistory            *RegisterFile
	env                          *ExecEnv
	debugOut                     DebugOutFunc
	fOut                         *os.File
	accountLaneRounds            []*cache.PreplayResult
	fieldLaneRounds              []*cache.PreplayResult
	round                        *cache.PreplayResult
	storeInfo                    *StoreInfo
	accountIndexToAppliedWObject map[uint]bool
	TraceStatus                  *cmptypes.TraceStatus
}

func (r *TraceTrieSearchResult) GetAnyRound() *cache.PreplayResult {
	cmptypes.MyAssert(r.hit)
	cmptypes.MyAssert(r.Node.Op.isStoreOp)
	for _, round := range r.Node.roundResults.rounds {
		if round != nil {
			return round
		}
	}
	cmptypes.MyAssert(false) // should never reach here
	return nil
}

func GetFirstNonEmptyRound(rounds []*cache.PreplayResult) *cache.PreplayResult {
	if rounds == nil {
		panic("rounds should never be nil")
	}
	for _, round := range rounds {
		if round != nil {
			return round
		}
	}
	cmptypes.MyAssert(false) // should never reach here
	return nil
}

func (r *TraceTrieSearchResult) AllDepApplyStores(txPreplay *cache.TxPreplay, traceStatus *cmptypes.TraceStatus, abort func() bool,
	noOverMatching, isBlockProcess bool) (bool, cmptypes.TxResIDMap) {
	return r.ApplyStores(txPreplay, traceStatus, abort, noOverMatching, isBlockProcess)
}
func (r *TraceTrieSearchResult) PartialApplyStores(txPreplay *cache.TxPreplay, traceStatus *cmptypes.TraceStatus, abort func() bool,
	noOverMatching, isBlockProcess bool) (bool, cmptypes.TxResIDMap) {
	return r.ApplyStores(txPreplay, traceStatus, abort, noOverMatching, isBlockProcess)
}
func (r *TraceTrieSearchResult) OpApplyStores(txPreplay *cache.TxPreplay, traceStatus *cmptypes.TraceStatus, abort func() bool,
	noOverMatching, isBlockProcess bool) (bool, cmptypes.TxResIDMap) {
	return r.ApplyStores(txPreplay, traceStatus, abort, noOverMatching, isBlockProcess)
}

func (r *TraceTrieSearchResult) _ApplyStores(txPreplay *cache.TxPreplay, traceStatus *cmptypes.TraceStatus, abort func() bool,
	noOverMatching, isBlockProcess bool) (bool, cmptypes.TxResIDMap) {
	switch traceStatus.TraceHitType {
	case cmptypes.AllDepHit:
		return r.AllDepApplyStores(txPreplay, traceStatus, abort, noOverMatching, isBlockProcess)
	case cmptypes.PartialHit:
		return r.PartialApplyStores(txPreplay, traceStatus, abort, noOverMatching, isBlockProcess)
	case cmptypes.AllDetailHit:
		return r.ApplyStores(txPreplay, traceStatus, abort, noOverMatching, isBlockProcess)
	case cmptypes.OpHit:
		return r.OpApplyStores(txPreplay, traceStatus, abort, noOverMatching, isBlockProcess)
	default:
		panic("unknown trace hit type")
	}
	return r.ApplyStores(txPreplay, traceStatus, abort, noOverMatching, isBlockProcess)
}

func (r *TraceTrieSearchResult) ApplyStores(txPreplay *cache.TxPreplay, traceStatus *cmptypes.TraceStatus, abort func() bool,
	noOverMatching, isBlockProcess bool) (bool, cmptypes.TxResIDMap) {
	cmptypes.MyAssert(r.Node.Op.isStoreOp)

	resMap := r.applyWObjects(txPreplay, noOverMatching, isBlockProcess)

	traceStatus.AccountWrittenCount = uint64(len(r.accountIndexToAppliedWObject))
	for n := r.Node; n.Next != nil; n = n.Next {
		if abort != nil && abort() {
			return true, nil
		}
		if n.Op.IsVirtual() {
			continue
		}
		if n.Next == nil {
			traceStatus.HitPathId = uintptr(unsafe.Pointer(n))
		}

		if !n.Op.IsLog() {
			traceStatus.TotalNodes++ // we do not count Log nodes as writes
			traceStatus.FieldPotentialWrittenCount++
			aIndex := r.storeInfo.StoreNodeIndexToAccountIndex[n.Seq]
			cmptypes.MyAssert(aIndex > 0)
			if r.accountIndexToAppliedWObject[aIndex] {
				if r.debugOut != nil {
					r.debugOut("SkipStore: %v", n.SimpleNameString())
					r.debugOut("    %v", n.RegisterValueString(r.registers))
				}
				continue
			}
			traceStatus.ExecutedOutputNodes++
			traceStatus.FieldActualWrittenCount++
		} else {
			traceStatus.ExecutedLogNodes++
		}
		traceStatus.ExecutedNodes++
		if r.debugOut != nil {
			r.debugOut("ApplyStore: %v", n.SimpleNameString())
			r.debugOut("    %v", n.RegisterValueString(r.registers))
		}
		n.Execute(r.env, r.registers)
	}
	if r.fOut != nil {
		r.fOut.Close()
	}
	return false, resMap
}

func (r *TraceTrieSearchResult) applyWObjects(txPreplay *cache.TxPreplay, noOverMatching, isBlockProcess bool) cmptypes.TxResIDMap {
	cmptypes.MyAssert(r.accountIndexToAppliedWObject == nil)
	r.accountIndexToAppliedWObject = r.env.GetNewAIdToApplied()

	accountLaneHit := r.accountLaneRounds != nil
	rounds := r.accountLaneRounds
	resMap := r.env.GetNewResMap()
	for _, si := range r.storeInfo.StoreAccountIndices {
		addr := r.getAddress(si)
		resMap[addr] = cmptypes.DEFAULT_TXRESID

		if !noOverMatching {
			aVId := r.getAVId(si)
			if aVId != 0 {
				if !accountLaneHit {
					ar := r.storeInfo.StoreAccountIndexToAVIdToRoundMapping[si]
					rm := ar[aVId]
					if rm == nil {
						continue
					}
					rounds = r.getMatchedRounds(si, rounds, rm, aVId)
				}
				if rounds == nil {
					continue
				}
				r.applyObjectInRounds(txPreplay, rounds, addr, resMap, si, aVId, isBlockProcess)
			}
		}
	}
	return resMap
}

func (r *TraceTrieSearchResult) getAVId(si uint) uint {
	aVId := r.accountHistory.Get(si).(uint)
	return aVId
}

func (r *TraceTrieSearchResult) getAddress(si uint) common.Address {
	var addr common.Address
	addrOrRId := r.storeInfo.AccountIndexToAddressOrRId[si]
	switch a_rid := addrOrRId.(type) {
	case common.Address:
		addr = a_rid
	case uint:
		addr = r.registers.Get(a_rid).(*MultiTypedValue).GetAddress()
	default:
		panic(fmt.Sprintf("Unknow type %v", reflect.TypeOf(addrOrRId).Name()))
	}
	return addr
}

func (r *TraceTrieSearchResult) applyObjectInRounds(txPreplay *cache.TxPreplay, rounds []*cache.PreplayResult,
	addr common.Address, resMap cmptypes.TxResIDMap, si uint, aVId uint, isBlockProcess bool) {
	isAccountChangeSet := false
	roundCount := len(rounds)
	// early rounds and wobjects might have already been removed
	// iterate reversely to more likely find an unused wobjects
	for i := roundCount - 1; i >= 0; i-- { //_, round := range rounds {
		round := rounds[i]
		if round != nil {
			if !isAccountChangeSet {
				changed, fok := round.AccountChanges[addr]
				if !fok {
					panic("")
				}
				resMap[addr] = changed
				isAccountChangeSet = true
				if !isBlockProcess { // do not apply wobject for preplay
					break
				}
			}
			if wref, refOk := round.WObjectWeakRefs.GetMatchedRef(addr, txPreplay); refOk {
				if objHolder, hok := txPreplay.PreplayResults.GetAndDeleteHolder(wref); hok {
					if r.debugOut != nil {
						r.debugOut("Apply WObject from Round %v for Account %v AVID %v with Addr %v\n",
							round.RoundID, si, aVId, addr.Hex())
					}
					r.env.state.ApplyStateObject(objHolder.Obj)
					r.accountIndexToAppliedWObject[si] = true
					break
				}
			}
		}
	}
}

func (r *TraceTrieSearchResult) getMatchedRounds(si uint, rounds []*cache.PreplayResult, rm *RoundMapping, aVId uint) []*cache.PreplayResult {
	accountDeps := r.storeInfo.StoreAccountIndexToStoreAccountDependencies[si]
	debug := r.debugOut != nil
	dk, dok, dvids := GetDepsKey(accountDeps, r.accountHistory, debug)
	rounds = nil
	if dok {
		rounds = rm.AccountDepsToRounds[dk]
		if rounds != nil && debug {
			r.debugOut("Account %v AVId %v full match by AccountDeps %v of %v with keyLen %v\n", si, aVId, accountDeps, dvids, len(dk))
		}
	}
	if rounds == nil {
		fieldDeps := r.storeInfo.StoreAccountIndexToStoreFieldDependencies[si]
		fk, fok, fvids := GetDepsKey(fieldDeps, r.fieldHistory, debug)
		if fok {
			rounds = rm.FieldDepsToRounds[fk]
			if rounds != nil && debug {
				r.debugOut("Account %v AVId %v full match by FieldDeps %v of %v with keyLen %v\n", si, aVId, fieldDeps, fvids, len(fk))
			}
		}
	}
	if rounds == nil {
		varIndices := r.storeInfo.StoreAccountIndexToStoreVarRegisterIndices[si]
		vk := GetValsKey(varIndices, r.registers)
		rounds = rm.StoreValuesToRounds[vk]
		if rounds != nil && r.debugOut != nil {
			r.debugOut("Account %v AVId %v full match by RegisterVals %v with keyLen %v\n", si, aVId, varIndices, len(vk))
		}
	}
	return rounds
}

func (tt *TraceTrie) SearchTraceTrie(db *state.StateDB, header *types.Header, getHashFunc vm.GetHashFunc,
	precompiles map[common.Address]vm.PrecompiledContract, abort func() bool, cfg *vm.Config, isBlockProcess bool, globalCache *cache.GlobalCache) (result *TraceTrieSearchResult) {

	debug := isBlockProcess && cfg.MSRAVMSettings.ReuseTracerChecking && cfg.MSRAVMSettings.CmpReuseChecking
	noOverMatching := cfg.MSRAVMSettings.NoOverMatching

	var node *SNode
	debugOut, fOut, env, registers, fieldHistory, accountHistory := tt.initSearch(db, header, getHashFunc, precompiles, globalCache, debug, isBlockProcess)

	var accountValHistory *RegisterFile
	if !isBlockProcess {
		accountValHistory = tt.GetNewHistory(false)
	}

	var accountLaneRounds []*cache.PreplayResult
	var fieldLaneRounds []*cache.PreplayResult
	var (
		traceStatus = &cmptypes.TraceStatus{}
		accountHit  = false
		fieldHit    = false
		opHit       = false
	)

	var hit, aborted bool

	defer func() {
		var fieldPotentialReadCount uint64
		if node.IsRLNode {
			if hit { // hit should be on store op
				cmptypes.MyAssert(false)
			}
			fieldPotentialReadCount = uint64(node.FieldIndex) // FieldIndex starts from 1
		} else {
			if node.BeforeFieldHistorySize <= 1 {
				cmptypes.MyAssert(false)
			}
			fieldPotentialReadCount = uint64(node.BeforeFieldHistorySize) - 1
		}
		traceStatus.FieldPotentialReadCount = fieldPotentialReadCount
		traceStatus.TotalNodes = uint64(node.Seq)
		traceStatus.TotalOpNodes = node.OpSeq
		traceStatus.AvgSpecializationDurationInNanoSeconds = tt.TotalSpecializationDuration.Nanoseconds() / tt.TotalTraceCount
		traceStatus.AvgMemoizationDurationInNanoSeconds = tt.TotalMemoizationDuration.Nanoseconds() / tt.TotalTraceCount
		traceStatus.TotalTraceCount = tt.TotalTraceCount
		traceStatus.PathCount = len(tt.RoundCountByPathID)
		traceStatus.RoundCount = int(tt.RefedRoundCount)
		result = &TraceTrieSearchResult{
			Node:              node,
			hit:               hit,
			registers:         registers,
			fieldHistory:      fieldHistory,
			accountHistory:    accountHistory,
			accountValHistory: accountValHistory,
			env:               env,
			debugOut:          debugOut,
			fOut:              fOut,
			storeInfo:         node.roundResults,
			accountLaneRounds: accountLaneRounds,
			fieldLaneRounds:   fieldLaneRounds,
			aborted:           aborted,
			TraceStatus:       traceStatus,
		}
		result.TraceStatus.SearchResult = result
		if accountHit {
			traceStatus.TraceHitType = cmptypes.AllDepHit
			result.round = GetFirstNonEmptyRound(accountLaneRounds)
		} else if fieldHit {
			if (traceStatus.TotalAccountLaneJumps > traceStatus.TotalAccountLaneFailedJumps) ||
				(traceStatus.TotalFieldLaneAJumps > traceStatus.TotalFieldLaneFailedAJumps) {
				traceStatus.TraceHitType = cmptypes.PartialHit
			} else {
				traceStatus.TraceHitType = cmptypes.AllDetailHit
			}
			result.round = GetFirstNonEmptyRound(fieldLaneRounds)
		} else if opHit {
			traceStatus.TraceHitType = cmptypes.OpHit
			result.round = GetFirstNonEmptyRound(node.roundResults.rounds)
		}
	}()

	if debugOut != nil {
		debugOut("Start Search Trie for Tx %v\n", tt.Tx.Hash().Hex())
	}

	// start with the AccountLane
	var fNode *FieldSearchTrieNode

	if tt.AccountHead != nil {
		if noOverMatching {
			cmptypes.MyAssert(false, "Account Head should not exist without over matching!")
		}
		node, fNode, accountLaneRounds, aborted, hit = tt.searchAccountLane(tt.AccountHead,
			env, registers, accountHistory, accountValHistory, fieldHistory, traceStatus,
			debug, debugOut, abort)
		if hit {
			accountHit = true
		}
	} else {
		fNode = tt.FieldHead
		if fNode != nil {
			node = fNode.LRNode
		}
	}

	if aborted || hit {
		return
	}

	if fNode != nil {
		fNode, node, fieldLaneRounds, aborted, hit = tt.searchFieldLane(fNode, node,
			env, registers, accountHistory, accountValHistory, fieldHistory,
			traceStatus,
			debug, debugOut, abort, noOverMatching)
		if hit {
			fieldHit = true
		}
	} else {
		if node != nil {
			cmptypes.MyAssert(false, "node should be nil when FieldLane is not available")
		}
		node = tt.Head
	}

	if aborted || hit {
		return
	}

	// start the OpLane
	if debug {
		cmptypes.MyAssert(fNode == nil || fNode.Exit == node)
		cmptypes.MyAssert(!node.Op.isStoreOp)
	}

	node, aborted, hit = tt.searchOpLane(node,
		env, accountHistory, accountValHistory, fieldHistory, registers,
		traceStatus,
		debug, debugOut, abort, noOverMatching)
	if hit {
		opHit = true
	}
	return
}

func (tt *TraceTrie) searchOpLane(nStart *SNode,
	env *ExecEnv, accountHistory *RegisterFile, accountValHistory *RegisterFile, fieldHistory *RegisterFile, registers *RegisterFile,
	traceStatus *cmptypes.TraceStatus,
	debug bool, debugOut func(fmtStr string, params ...interface{}), abort func() bool,
	noOverMatching bool) (node *SNode, aborted, hit bool) {

	node = nStart
	for {
		if abort != nil && abort() {
			aborted = true
			break
		}
		jHead := node.JumpHead
		beforeJumpNode := node

		if jHead != nil {
			//aCheckCount := 0
			//fCheckCount := 0
			jNode := jHead
			// try to jump
			node = tt.OpLaneJump(debug, jNode, node, accountHistory, debugOut,
				fieldHistory, registers, beforeJumpNode, traceStatus,
				noOverMatching)
		}

		// if jump failed, execute
		if beforeJumpNode == node {
			if debugOut != nil {
				prefix := ""
				action := "Execute"
				if node.Op.isReadOp {
					action = "Read"
					prefix = "  "
				} else if node.Op.isLoadOp {
					action = "Load"
					prefix = "  "
				}
				debugOut(prefix+"OpLane %v #%v %v\n", action, node.Seq, node.Statement.SimpleNameString())
			}
			var fVal interface{}
			if !noOverMatching && node.IsANode {
				aVId, newAccount := node.GetOrLoadAccountValueID(env, registers, accountHistory, accountValHistory)
				if debug {
					cmptypes.MyAssert(newAccount)
				}
				if newAccount {
					if node.Op.isLoadOp {
						traceStatus.AccountReadCount++
						if aVId == 0 {
							traceStatus.AccountReadUnknownCount++
						}
					}
				}
			}

			traceStatus.ExecutedNodes++
			if node.IsRLNode {
				var fVId uint
				fVId, fVal = node.LoadFieldValueID(env, registers, fieldHistory)
				traceStatus.ExecutedInputNodes++
				traceStatus.FieldActualReadCount++
				if fVId == 0 {
					traceStatus.FieldReadUnknownCount++
					if node.Op.isReadOp {
						traceStatus.ChainReadUnknownCount++
					}
				}
				if node.Op.isReadOp {
					traceStatus.ExecutedChainInputNodes++
				}
			} else {
				fVal = node.Execute(env, registers)
			}

			if node.OutputRegisterIndex != 0 {
				registers.AppendRegister(node.OutputRegisterIndex, fVal)
			}

			node = node.GetNextNode(registers)
		} else {
			traceStatus.OpLaneSuccessfulJumps++
		}

		if node == nil {
			node = beforeJumpNode
			if debug {
				cmptypes.MyAssert(node.Op.IsGuard())
			}
			if debugOut != nil {
				stats := []*Statement(nil)
				for i, fIndex := range node.FieldDependencies {
					if fieldHistory.Get(fIndex).(uint) == 0 {
						stats = append(stats, node.FieldDependencyStatements[i])
					}
				}
				debugOut("OpLane Miss after reaching: #%v %v\n", node.Seq, node.Statement.SimpleNameString())
				debugOut("OpLane miss @ %v reason #%v for tx %v:\n", node.Statement.SimpleNameString(), len(stats), tt.Tx.Hash().Hex())
				for _, s := range stats {
					debugOut("  miss due to %v\n", s.SimpleNameString())
				}
			}
			hit = false
			break
		}

		if node.Op.isStoreOp {
			if debugOut != nil {
				debugOut("OpLane Hit by reaching: #%v %v", node.Seq, node.Statement.SimpleNameString())
			}
			hit = true
			break
		}
	}
	return
}

func (tt *TraceTrie) initSearch(db *state.StateDB, header *types.Header, getHashFunc vm.GetHashFunc,
	precompiles map[common.Address]vm.PrecompiledContract, globalCache *cache.GlobalCache,
	debug bool, isBlockProcess bool) (func(fmtStr string, params ...interface{}), *os.File, *ExecEnv, *RegisterFile, *RegisterFile, *RegisterFile) {

	var debugOut func(fmtStr string, params ...interface{})
	var fOut *os.File

	env := tt.GetNewExecEnv(db, header, getHashFunc, precompiles, globalCache, isBlockProcess)
	registers := tt.GetNewRegisters(isBlockProcess) //NewRegisterFile(tt.RegisterSize) // RegisterFile // := make(RegisterFile, tt.RegisterSize)
	fieldHistory := tt.GetNewHistory(isBlockProcess)
	accountHistory := tt.GetNewHistory(isBlockProcess)

	if debug {
		fOut, _ = os.OpenFile("/tmp/SearchTrieLog.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)

		debugOut = func(fmtStr string, params ...interface{}) {
			if len(fmtStr) > 0 && fmtStr[len(fmtStr)-1] != '\n' {
				fmtStr += "\n"
			}
			s := fmt.Sprintf(fmtStr, params...)
			fOut.WriteString(s)
		}
	}
	return debugOut, fOut, env, registers, fieldHistory, accountHistory
}

func (tt *TraceTrie) searchFieldLane(fStart *FieldSearchTrieNode, nStart *SNode,
	env *ExecEnv, registers *RegisterFile, accountHistory *RegisterFile, accountValHistory *RegisterFile, fieldHistory *RegisterFile,
	traceStatus *cmptypes.TraceStatus,
	debug bool, debugOut func(fmtStr string, params ...interface{}),
	abort func() bool, noOverMatching bool) (fNode *FieldSearchTrieNode, node *SNode, fieldLaneRounds []*cache.PreplayResult, aborted, ok bool) {

	if debug {
		cmptypes.MyAssert(fStart != nil)
	}

	var registerSnapshotToApply *RegisterFile

	fNode = fStart
	node = nStart

	// Now in the FieldLane
	for {
		//if abort != nil && abort() {
		//	aborted = true
		//	break
		//}
		if debug {
			cmptypes.MyAssert(fNode.LRNode == node)
		}

		beforeNode := node

		//if !noOverMatching && fNode.LRNode.Op.isLoadOp {
		if !noOverMatching && fNode.IsLoad {
			if DEBUG_TRACER {
				cmptypes.MyAssert(fNode.LRNode.Op.isLoadOp)
			}
			aVId, newAccount := fNode.LRNode.FastGetOrLoadAccountValueID(env, fNode.AddrLoc, accountHistory, accountValHistory)
			if DEBUG_TRACER && newAccount {
				_registers := registers
				if registerSnapshotToApply != nil {
					_registers = registerSnapshotToApply
				}
				_aVId, _newAccount := fNode.LRNode.GetOrLoadAccountValueID(env, _registers, nil, nil)
				addr := fNode.LRNode.GetInputVal(0, _registers).(*MultiTypedValue).GetAddress()
				changedBy, _ := env.state.GetAccountDepValue(addr)
				var vid, vid2 uint
				var depHash string
				if changedBy == cmptypes.DEFAULT_TXRESID {
					vid = 0
					vid2 = 0
				} else {
					depHash = *changedBy.Hash()
					k := GetGuardKey(depHash)
					vid = fNode.LRNode.AccountValueToID[k]
					vid2 = fNode.LRNode.FastAccountValueToID.(StringFieldValueToID)[depHash]
				}
				cmptypes.MyAssert(aVId == _aVId, "aVId mismatch %v %v variant %v addr %v _addr %v vid %v vid2 %v depHash %v", aVId, _aVId, *fNode.LRNode.Op.config.variant,
					fNode.AddrLoc.Address.Hex(), addr.Hex(), vid, vid2, depHash,
				)
				cmptypes.MyAssert(newAccount == _newAccount, "newAccount mismatch %v %v", newAccount, _newAccount)
			}

			if !node.IsANode {
				cmptypes.MyAssert(!newAccount)
			}
			if newAccount {
				traceStatus.AccountReadCount++
				if aVId == 0 {
					traceStatus.AccountReadUnknownCount++
				}
			}

			var jd *FieldNodeJumpDef
			if aVId != 0 {
				jd = fNode.GetAJumpDef(aVId)
				traceStatus.TotalFieldLaneAJumps++
				traceStatus.TotalFieldLaneAJumpKeys++
				if jd == nil {
					traceStatus.TotalFieldLaneFailedAJumps++
				}
			}
			if jd == nil {
				if debugOut != nil {
					debugOut("  FieldLane AJump Failed #%v %v on AIndex %v AVId %v\n", node.Seq, node.Statement.SimpleNameString(), node.AccountIndex, aVId)
				}
			} else {
				if debugOut != nil {
					debugOut("FieldLane AJump from (#%v to #%v) %v to %v on AIndex %v AVId %v, adding fields %v\n",
						node.Seq, jd.Dest.LRNode.Seq, node.Statement.SimpleNameString(),
						jd.Dest.LRNode.Statement.SimpleNameString(), node.AccountIndex, aVId, jd.FieldHistorySegment)
				}
				if debug {
					cmptypes.MyAssert(jd.RegisterSnapshot != nil)
				}
				//registers.ApplySnapshot(jd.RegisterSnapshot)
				registerSnapshotToApply = jd.RegisterSnapshot
				if debug {
					cmptypes.MyAssert(jd.FieldHistorySegment != nil)
					cmptypes.MyAssert(fieldHistory.size == node.FieldIndex, "%v, %v", fieldHistory.size, node.FieldIndex)
				}
				fieldHistory.AppendSegment(fNode.LRNode.FieldIndex, jd.FieldHistorySegment)
				fNode = jd.Dest
				node = fNode.LRNode
				if debug {
					//cmptypes.MyAssert(registers.size == node.BeforeRegisterSize)
					cmptypes.MyAssert(registerSnapshotToApply.size == node.BeforeRegisterSize)
					cmptypes.MyAssert(fieldHistory.size == node.BeforeFieldHistorySize)
				}
			}
		}

		if beforeNode == node {
			if !noOverMatching && node.Op.isReadOp {
				aVId, newAccount := fNode.LRNode.FastGetOrLoadAccountValueID(env, fNode.AddrLoc, accountHistory, accountValHistory)
				if DEBUG_TRACER {
					_registers := registers
					if registerSnapshotToApply != nil {
						_registers = registerSnapshotToApply
					}
					_aVId, _ := node.GetOrLoadAccountValueID(env, _registers, nil, nil)
					cmptypes.MyAssert(aVId == _aVId, "aVId mismatch %v %v", aVId, _aVId)
					if fNode != fStart && !newAccount {
						cmptypes.MyAssert(false)
					}
				}
			}

			var fVId uint
			var fVal interface{}
			fVId = fNode.LRNode.DispatchBasedFastLoadFieldValueID(env, fNode.AddrLoc, fieldHistory)

			var _fVId uint
			var _fVal interface{}
			if DEBUG_TRACER {
				_registers := registers
				if registerSnapshotToApply != nil {
					_registers = registerSnapshotToApply
				}
				_fVId, _fVal = fNode.LRNode.LoadFieldValueID(env, _registers, nil)
				cmptypes.MyAssert(fVId == _fVId, "fVId mismatch %v %v", fVId, _fVId)
			}

			//fVId, fVal = fNode.LRNode.LoadFieldValueID(env, registers, fieldHistory)

			if !(node == nStart && node.Op.isReadOp) { // if nStart is ReadOp, it is already counted in AccountLane
				traceStatus.ExecutedNodes++
				traceStatus.ExecutedInputNodes++
				if node.Op.isReadOp {
					traceStatus.ExecutedChainInputNodes++
				}
				traceStatus.FieldActualReadCount++
				if fVId == 0 {
					traceStatus.FieldReadUnknownCount++
					if node.Op.isReadOp {
						traceStatus.ChainReadUnknownCount++
					}
				}
			}

			var jd *FieldNodeJumpDef
			if fVId != 0 {
				jd = fNode.GetFJumpDef(fVId)
				traceStatus.TotalFieldLaneFJumps++
				traceStatus.TotalFieldLaneFJumpKeys++
				if jd == nil {
					traceStatus.TotalFieldLaneFailedFJumps++
				}
			}

			if jd == nil {
				if debugOut != nil {
					debugOut("  FieldLane FJump Failed #%v %v on FIndex %v FVId %v\n", node.Seq, node.Statement.SimpleNameString(), node.FieldIndex, fVId)
				}
				fVal = fNode.LRNode.FastGetFVal(fNode.AddrLoc, env)
				if DEBUG_TRACER {
					AssertEqualRegisterValue(fVal, _fVal)
				}
				if registerSnapshotToApply != nil {
					registers.ApplySnapshot(registerSnapshotToApply)
				}
				registers.AppendRegister(node.OutputRegisterIndex, fVal)
				node = fNode.Exit
				if debug {
					cmptypes.MyAssert(registers.size == node.BeforeRegisterSize)
				}
				break
			} else {
				if debugOut != nil {
					debugOut("FieldLane FJump from (#%v to #%v) %v to %v on FIndex %v FVId %v\n",
						node.Seq, jd.Dest.LRNode.Seq, node.Statement.SimpleNameString(),
						jd.Dest.LRNode.Statement.SimpleNameString(),
						node.FieldIndex, fVId)
				}
				//if debug {
				//	cmptypes.MyAssert(jd.RegisterSnapshot != nil)
				//}
				//registers.ApplySnapshot(jd.RegisterSnapshot)
				registerSnapshotToApply = jd.RegisterSnapshot
				fNode = jd.Dest
				node = fNode.LRNode
				if debug {
					//cmptypes.MyAssert(registers.size == node.BeforeRegisterSize)
					cmptypes.MyAssert(registerSnapshotToApply.size == node.BeforeRegisterSize)
				}
			}
		}

		//if fNode.LRNode.Op.isStoreOp {
		if fNode.IsStore {
			if DEBUG_TRACER {
				cmptypes.MyAssert(fNode.LRNode.Op.isStoreOp)
			}
			if debugOut != nil {
				debugOut("FieldLane Hit by reaching: #%v %v\n", node.Seq, node.Statement.SimpleNameString())
			}
			fieldLaneRounds = fNode.Rounds
			registers.ApplySnapshot(registerSnapshotToApply)
			ok = true
			break
		}
	}
	return
}

func (tt *TraceTrie) searchAccountLane(aNode *AccountSearchTrieNode,
	env *ExecEnv, registers *RegisterFile, accountHistory *RegisterFile, accountValHistory *RegisterFile, fieldHistory *RegisterFile,
	traceStatus *cmptypes.TraceStatus,
	debug bool, debugOut func(fmtStr string, params ...interface{}),
	abort func() bool) (node *SNode, fNode *FieldSearchTrieNode, accountLaneRounds []*cache.PreplayResult, aborted bool, ok bool) {
	node = aNode.LRNode
	var registerSnapshotToApply, fieldHistorySnapshotToApply *RegisterFile
	for {
		//if abort != nil && abort() {
		//	aborted = true
		//	break
		//}
		aVId, newAccount := aNode.LRNode.FastGetOrLoadAccountValueID(env, aNode.AddrLoc, accountHistory, accountValHistory)

		if DEBUG_TRACER {
			cmptypes.MyAssert(newAccount)
			_registers := registers
			if registerSnapshotToApply != nil {
				_registers = registerSnapshotToApply
			}
			_aVId, _newAccount := aNode.LRNode.GetOrLoadAccountValueID(env, _registers, nil, nil)
			cmptypes.MyAssert(aVId == _aVId, "aVId mismatch %v %v", aVId, _aVId)
			cmptypes.MyAssert(newAccount == _newAccount, "newAccount mismatch %v %v", newAccount, _newAccount)

			//aVId, newAccount := aNode.LRNode.GetOrLoadAccountValueID(env, _registers, accountHistory, accountValHistory)
		}
		if newAccount {
			if aNode.LRNode.Op.isReadOp {
				traceStatus.ExecutedNodes++
				traceStatus.ExecutedInputNodes++
				traceStatus.ExecutedChainInputNodes++
				traceStatus.FieldActualReadCount++ // for reading chain head fields
				if aVId == 0 {
					traceStatus.FieldReadUnknownCount++
					traceStatus.ChainReadUnknownCount++
				}
			} else {
				traceStatus.AccountReadCount++
				if aVId == 0 {
					traceStatus.AccountReadUnknownCount++
				}
			}
		}

		var jd *AccountNodeJumpDef
		if aVId != 0 {
			jd = aNode.GetAJumpDef(aVId)
			traceStatus.TotalAccountLaneJumps++
			traceStatus.TotalAccountLaneJumpKeys++
			if jd == nil {
				traceStatus.TotalAccountLaneFailedJumps++
			}
		}
		if jd == nil {
			if debugOut != nil {
				debugOut("  AccountLane Failed #%v %v on AIndex %v AVId %v\n", node.Seq, node.Statement.SimpleNameString(), node.AccountIndex, aVId)
			}
			if debug {
				cmptypes.MyAssert(aNode.Exit != nil)
			}
			fNode = aNode.Exit
			if debug {
				cmptypes.MyAssert(aNode.LRNode == fNode.LRNode)
			}
			if registerSnapshotToApply != nil {
				registers.ApplySnapshot(registerSnapshotToApply)
				fieldHistory.ApplySnapshot(fieldHistorySnapshotToApply)
			}
			break
		} else {
			if debugOut != nil {
				debugOut("AccountLane Jump from (#%v to #%v) %v to %v on AIndex %v AVId %v\n",
					node.Seq, jd.Dest.LRNode.Seq, node.Statement.SimpleNameString(), jd.Dest.LRNode.Statement.SimpleNameString(), node.AccountIndex, aVId)
			}
			if debug {
				cmptypes.MyAssert(jd.RegisterSnapshot != nil)
				cmptypes.MyAssert(jd.FieldHistorySnapshot != nil)
			}
			//registers.ApplySnapshot(jd.RegisterSnapshot)
			//fieldHistory.ApplySnapshot(jd.FieldHistorySnapshot)
			registerSnapshotToApply = jd.RegisterSnapshot
			fieldHistorySnapshotToApply = jd.FieldHistorySnapshot
			aNode = jd.Dest
			node = aNode.LRNode
			if debug {
				//cmptypes.MyAssert(registers.size == aNode.LRNode.BeforeRegisterSize)
				//if !aNode.LRNode.Op.isStoreOp {
				//	cmptypes.MyAssert(fieldHistory.size == aNode.LRNode.FieldIndex)
				//}
				cmptypes.MyAssert(registerSnapshotToApply.size == aNode.LRNode.BeforeRegisterSize)
				if !aNode.LRNode.Op.isStoreOp {
					cmptypes.MyAssert(fieldHistorySnapshotToApply.size == aNode.LRNode.FieldIndex)
				}
			}
		}
		//if aNode.LRNode.Op.isStoreOp {
		if aNode.IsStore {
			if debugOut != nil {
				debugOut("AccountLane Hit by reaching: #%v %v\n", node.Seq, node.Statement.SimpleNameString())
			}
			accountLaneRounds = aNode.Rounds
			if debug {
				cmptypes.MyAssert(len(accountLaneRounds) > 0)
			}
			registers.ApplySnapshot(registerSnapshotToApply)
			fieldHistory.ApplySnapshot(fieldHistorySnapshotToApply)

			ok = true
			break
		}
	}
	return
}

func GetOpJumKey(rf *RegisterFile, indices []uint) (key OpJumpKey, ok bool) {
	if DEBUG_TRACER {
		cmptypes.MyAssert(len(indices) > 0)
		cmptypes.MyAssert(len(indices) <= OP_JUMP_KEY_SIZE)
	}
	for i, index := range indices {
		v := rf.Get(index).(uint)
		if v == 0 {
			ok = false
			return
		}
		key[i] = v
	}
	ok = true
	return
}

func (tt *TraceTrie) OpLaneJump(debug bool, jNode *OpSearchTrieNode, node *SNode,
	accountHistory *RegisterFile, debugOut func(fmtStr string, params ...interface{}),
	fieldHistory *RegisterFile, registers *RegisterFile, beforeJumpNode *SNode,
	traceStatus *cmptypes.TraceStatus,
	noOverMatching bool) *SNode {
	var aCheckCount, fCheckCount int
	for {
		if debug {
			cmptypes.MyAssert(jNode.OpNode == node)
		}
		if jNode.IsAccountJump {
			if noOverMatching {
				cmptypes.MyAssert(false, "Should not have AccountJump when overmatching is disabled")
			}

			if debugOut != nil {
				aCheckCount += len(jNode.VIndices)
			}

			aKey, aOk := GetOpJumKey(accountHistory, jNode.VIndices)

			var jd *OpNodeJumpDef
			if aOk {
				jd = jNode.GetMapJumpDef(&aKey)
				traceStatus.TotalOpLaneAJumps++
				traceStatus.TotalFieldLaneFJumpKeys += uint64(len(jNode.VIndices))
				if jd == nil {
					traceStatus.TotalOpLaneFailedAJumps++
				}
			}
			if jd == nil {
				if debugOut != nil {
					debugOut("  OpLane ACheck Failed #%v %v on AIndex %v AVId %v\n", node.Seq, node.Statement.SimpleNameString(),
						jNode.VIndices, aKey[:len(jNode.VIndices)])
				}
				if jNode.Exit != nil {
					jNode = jNode.Exit.Next
					if debug {
						cmptypes.MyAssert(jNode != nil)
					}
				} else {
					jNode = nil
				}
			} else {
				if debugOut != nil {
					debugOut("  OpLane ACheck Passed #%v %v on AIndex %v AVId %v\n", node.Seq, node.Statement.SimpleNameString(),
						jNode.VIndices, aKey[:len(jNode.VIndices)])
				}

				if len(jd.FieldHistorySegment) > 0 {
					if DEBUG_TRACER {
						cmptypes.MyAssert(node.FieldIndex != 0)
						cmptypes.MyAssert(node.IsRLNode)
						cmptypes.MyAssert(node.BeforeFieldHistorySize == node.FieldIndex)
					}
					fieldHistory.AppendSegment(node.BeforeFieldHistorySize, jd.FieldHistorySegment)
				}
				if len(jd.RegisterSegment) > 0 {
					if debug {
						cmptypes.MyAssert(registers.size == node.BeforeRegisterSize, "%v, %v", registers.size, node.BeforeRegisterSize)
					}
					registers.AppendSegment(node.BeforeRegisterSize, jd.RegisterSegment)
				}

				jNode = jd.Next

				if jNode != nil {
					node = jNode.OpNode
					if debug {
						cmptypes.MyAssert(jd.Dest == nil)
					}
				} else if jd.Dest != nil {
					node = jd.Dest
				}

				if debug {
					cmptypes.MyAssert(registers.size == node.BeforeRegisterSize)
					cmptypes.MyAssert(fieldHistory.size == node.BeforeFieldHistorySize)
				}
			}
		} else {
			if debugOut != nil {
				fCheckCount += len(jNode.VIndices)
			}
			fKey, fOk := GetOpJumKey(fieldHistory, jNode.VIndices)
			var jd *OpNodeJumpDef
			if fOk {
				jd = jNode.GetMapJumpDef(&fKey)
				traceStatus.TotalOpLaneFJumps++
				traceStatus.TotalOpLaneFJumpKeys += uint64(len(jNode.VIndices))
				if jd == nil {
					traceStatus.TotalOpLaneFailedFJumps++
				}
			}
			if jd == nil {
				if debugOut != nil {
					debugOut("  OpLane FCheck Failed #%v %v on FIndex %v FVId %v\n", node.Seq, node.Statement.SimpleNameString(),
						jNode.VIndices, fKey[:len(jNode.VIndices)])
					cmptypes.MyAssert(jNode.Exit == nil)
				}
				jNode = nil
			} else {
				if debugOut != nil {
					debugOut("  OpLane FCheck Passed #%v %v on FIndex %v FVId %v\n", node.Seq, node.Statement.SimpleNameString(),
						jNode.VIndices, fKey[:len(jNode.VIndices)])
				}
				if node.IsJSPNode && node.Op.isLoadOp && len(node.FieldDependencies) > 0 && jd.Next == nil {
					if debug {
						cmptypes.MyAssert(len(jd.FieldHistorySegment) == 1)
						cmptypes.MyAssert(len(jd.RegisterSegment) == 1)
					}
					fieldHistory.AppendRegister(node.FieldIndex, jd.FieldHistorySegment[0])
				} else {
					if debug {
						cmptypes.MyAssert(jd.FieldHistorySegment == nil)
					}
				}
				if len(jd.RegisterSegment) > 0 {
					if debug {
						cmptypes.MyAssert(registers.size == node.BeforeRegisterSize, "%v, %v", registers.size, node.BeforeRegisterSize)
					}
					registers.AppendSegment(node.BeforeRegisterSize, jd.RegisterSegment)
				}

				jNode = jd.Next
				if jNode != nil {
					node = jNode.OpNode
					if debug {
						cmptypes.MyAssert(jd.Dest == nil)
					}
				} else if jd.Dest != nil {
					node = jd.Dest
				}

				if debug {
					cmptypes.MyAssert(registers.size == node.BeforeRegisterSize)
					cmptypes.MyAssert(fieldHistory.size == node.BeforeFieldHistorySize)
				}
			}
		}
		if jNode == nil {
			if debugOut != nil {
				beforeNode := beforeJumpNode
				if beforeNode != node {
					debugOut("OpLane Jump from (#%v to #%v) %v to %v with %v AChecks and %v FChecks\n",
						beforeNode.Seq, node.Seq, beforeNode.Statement.SimpleNameString(),
						node.Statement.SimpleNameString(), aCheckCount, fCheckCount)
				}
			}
			break
		}
	}
	return node
}

func GetStatementOutputMultiTypeVal(s *Statement, registers *RegisterFile, registerMapping map[uint32]uint) *MultiTypedValue {
	return GetStatementOutputVal(s, registers, registerMapping).(*MultiTypedValue)
}

func GetStatementOutputVal(s *Statement, registers *RegisterFile, registerMapping map[uint32]uint) interface{} {
	var val interface{}
	if registers != nil && s.output != nil && !s.output.IsConst() {
		rid := registerMapping[s.output.id]
		val = registers.Get(rid)
	} else {
		val = s.output.val
	}
	return val
}

func GetStatementInputMultiTypeVal(s *Statement, inputIndex int, registers *RegisterFile, registerMapping map[uint32]uint) *MultiTypedValue {
	return GetStatementInputVal(s, inputIndex, registers, registerMapping).(*MultiTypedValue)
}

func GetStatementInputVal(s *Statement, inputIndex int, registers *RegisterFile, registerMapping map[uint32]uint) interface{} {
	var val interface{}
	if registers != nil && !s.inputs[inputIndex].IsConst() {
		rid := registerMapping[s.inputs[inputIndex].id]
		val = registers.Get(rid)
	} else {
		val = s.inputs[inputIndex].val
	}
	return val
}

func (tt *TraceTrie) insertStatement(pNode *SNode, guardKey interface{}, s *Statement, nodeIndex uint, trace *STrace, result *TraceTrieSearchResult) (*SNode, interface{}) {
	registerMapping := &(trace.RAlloc)
	if pNode == nil {
		panic("pNode should never be nil")
		//if tt.DebugOut != nil {
		//	tt.DebugOut("NewHead %v\n", s.SimpleNameStringWithRegisterAnnotation(registerMapping))
		//}
		//return NewSNode(s, nodeIndex, trace, nil, nil), nil
	}

	node := pNode.Next
	if pNode.Op.IsGuard() {
		cmptypes.MyAssert(guardKey != nil)
		node = pNode.GuardedNext[guardKey]
	}

	if node == nil {
		if tt.DebugOut != nil {
			tt.DebugOut("NewNode %v\n", s.SimpleNameStringWithRegisterAnnotation(registerMapping))
		}
		node = NewSNode(s, nodeIndex, trace, pNode, guardKey)
		if pNode.Op.IsGuard() {
			if pNode.GuardedNext == nil {
				pNode.GuardedNext = make(map[interface{}]*SNode)
			}
			pNode.GuardedNext[guardKey] = node

			if tt.DebugOut != nil {
				tt.DebugOut("InsertAfter Guard %v on key %v\n", pNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping), guardKey)
			}
		} else {
			if tt.DebugOut != nil {
				tt.DebugOut("InsertAfter %v\n", pNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping))
			}
			pNode.Next = node
		}
	} else {
		if tt.DebugOut != nil {
			tt.DebugOut("OldNode %v\n", node.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping))
			node.AssertIsomorphic(s, nodeIndex, trace, "%v %v", node.Statement.SimpleNameString(), s.SimpleNameString())
		}
	}

	if node.Op.IsGuard() {
		var registers *RegisterFile
		if result != nil {
			registers = result.registers
		}
		return node, GetGuardKey(GetStatementOutputVal(s, registers, *registerMapping))
	} else {
		return node, nil
	}

}

func IsJumpTargets(op *OpDef) bool {
	return op.IsLoadOrStoreOrRead() && op != OP_LoadCode
}

func IsJumpStartingPoint(op *OpDef) bool {
	return op.isReadOp || (op.isLoadOp && op != OP_LoadCode)
}

type JumpInserter struct {
	registers                     *RegisterFile
	accountHistory                *RegisterFile
	fieldHistroy                  *RegisterFile
	nodeIndexToAccountValue       map[uint]interface{}
	snodes                        []*SNode
	fnodes                        []*FieldSearchTrieNode
	trace                         *STrace
	nodeIndexToAVId               map[uint]uint
	nodeIndexToFVId               map[uint]uint
	tt                            *TraceTrie
	aNodeFJds                     []*FieldNodeJumpDef
	round                         *cache.PreplayResult
	refNodes                      []ISRefCountNode
	newNodeCount                  uint
	noOverMatching                bool
	result                        *TraceTrieSearchResult
	nodeIndexToAccountValueString map[uint]string
}

func NewJumpInserter(tt *TraceTrie, trace *STrace, currentNodes []*SNode, round *cache.PreplayResult, result *TraceTrieSearchResult, noOverMatching bool) *JumpInserter {

	var nodeIndexToAccountValue map[uint]interface{}
	var nodeIndexToAccountValueString map[uint]string
	if !noOverMatching {
		nodeIndexToAccountValue, nodeIndexToAccountValueString = GetNodeIndexToAccountSnapMapping(trace, round.ReadDepSeq, result)
	}
	j := &JumpInserter{
		nodeIndexToAccountValue:       nodeIndexToAccountValue,
		nodeIndexToAccountValueString: nodeIndexToAccountValueString,
		snodes:                        currentNodes,
		trace:                         trace,
		nodeIndexToAVId:               make(map[uint]uint),
		nodeIndexToFVId:               make(map[uint]uint),
		tt:                            tt,
		round:                         round,
		noOverMatching:                noOverMatching,
		result:                        result,
	}
	j.InitAccountAndFieldValueIDs()
	return j
}

func (j *JumpInserter) ExecuteStats(startIndex, endIndex uint) {
	//MyAssert(!j.registers.isFileReadOnly)
	//MyAssert(!j.registers.isLastSegReadOnly)
	stats, registers, registerMapping := j.trace.Stats, j.registers, j.trace.RAlloc
	for i := startIndex; i < endIndex; i++ {
		s := stats[i]
		if s.output != nil && !s.output.IsConst() {
			rid := registerMapping[s.output.id]
			//registers.AppendRegister(rid, s.output.val)
			var resultRegisters *RegisterFile
			if j.result != nil {
				resultRegisters = j.result.registers
			}
			registers.AppendRegister(rid, GetStatementOutputVal(s, resultRegisters, registerMapping))
		}
	}
}

func (j *JumpInserter) AddRefNode(node ISRefCountNode) {
	j.refNodes = append(j.refNodes, node)
	if node.GetRefCount() == 0 {
		j.newNodeCount++
	}
}

func (j *JumpInserter) InitAccountAndFieldValueIDs() {
	tt := j.tt
	rmap := &(j.trace.RAlloc)
	accountHistory := NewRegisterFile(uint(len(j.trace.ANodeSet)))
	fieldHistory := NewRegisterFile(uint(len(j.trace.RLNodeIndices)))

	for _, nodeIndex := range j.trace.RLNodeIndices {
		node := j.snodes[nodeIndex]
		cmptypes.MyAssert(node.Op.isLoadOp || node.Op.isReadOp)
		s := j.trace.Stats[nodeIndex]
		var aVId, fVId uint
		if !j.noOverMatching {
			if node.IsANode {
				var aval interface{}
				var avalStr string
				var ok bool
				if node.Op.isLoadOp {
					aval, ok = j.nodeIndexToAccountValue[node.Seq]
					if DEBUG_TRACER {
						avalStr = j.nodeIndexToAccountValueString[node.Seq]
					}
					cmptypes.MyAssert(ok)
				} else {
					var resultRegisters *RegisterFile
					if j.result != nil {
						resultRegisters = j.result.registers
					}
					aval = GetStatementOutputVal(s, resultRegisters, *rmap)
					if DEBUG_TRACER {
						avalStr = fmt.Sprintf("%v", aval)
					}
					//aval = s.output.val
				}
				aVId = node.GetOrCreateAccountValueID(aval)
				accountHistory.AppendRegister(node.AccountIndex, aVId)
				_aVId := node.FastGetOrCreateAccountValueID(aval)
				if tt.DebugOut != nil {
					tt.DebugOut("AccountVID for %v of %v is %v\n", s.SimpleNameStringWithRegisterAnnotation(rmap), avalStr, aVId)
					cmptypes.MyAssert(aVId == _aVId, "AVID Missmatch %v %v", aVId, _aVId)
				} else {
					if _aVId != aVId {
						cmptypes.MyAssert(false, "AVID Mismatch %v %v", aVId, _aVId)
					}
				}
			} else {
				cmptypes.MyAssert(node.Op.isLoadOp)
				aVId = accountHistory.Get(node.AccountIndex).(uint)
				if tt.DebugOut != nil {
					tt.DebugOut("AccountVID for %v is %v\n", s.SimpleNameStringWithRegisterAnnotation(rmap), aVId)
				}
			}
		}

		//fVal := s.output.val
		var resultRegisters *RegisterFile
		if j.result != nil {
			resultRegisters = j.result.registers
		}
		fVal := GetStatementOutputVal(s, resultRegisters, *rmap)
		fVId = node.FastGetOrCreateFieldValueID(fVal)
		_fVId := node.GetOrCreateFieldValueID(fVal)
		if tt.DebugOut == nil {
			if _fVId != fVId {
				cmptypes.MyAssert(false, "FVID Mismatch %v %v", fVId, _fVId)
			}
		}

		fieldHistory.AppendRegister(node.FieldIndex, fVId)

		if tt.DebugOut != nil {
			tt.DebugOut("FieldVID for %v of %v is %v\n", s.SimpleNameStringWithRegisterAnnotation(rmap), fVal, fVId)
			cmptypes.MyAssert(fVId == _fVId, "FVID Missmatch %v %v", fVId, _fVId)
		}

		j.nodeIndexToAVId[nodeIndex] = aVId
		j.nodeIndexToFVId[nodeIndex] = fVId
	}
	j.accountHistory = accountHistory
	j.fieldHistroy = fieldHistory
}

func (j *JumpInserter) SetupFieldJumpLane() (lastFDes *FieldSearchTrieNode) {
	tt := j.tt
	snodes := j.snodes
	if tt.DebugOut != nil {
		tt.DebugOut("SetupJumps\n")
	}

	//beforeNewNodeCount := j.newNodeCount

	j.registers = NewRegisterFile(j.trace.RegisterFileSize)
	registerMapping := &(j.trace.RAlloc)
	//accountHistory := NewRegisterFile(uint(len(j.trace.ANodeSet)))
	//fieldHistory := NewRegisterFile(uint(len(j.trace.RLNodeIndices)))

	// set up Field Search Track
	fSrc := tt.FieldHead
	if fSrc == nil {
		fSrc = NewFieldSearchTrieNode(snodes[0])
		fSrc.SetAddrLoc(j.registers)
		fSrc.Exit = snodes[1]
		tt.FieldHead = fSrc
	} else {
		cmptypes.MyAssert(fSrc.LRNode == snodes[0])
		cmptypes.MyAssert(fSrc.Exit == snodes[1])
		fSrc.CheckAddrLoc(j.registers)
	}
	j.AddRefNode(fSrc)

	fNodes := make([]*FieldSearchTrieNode, 1, len(j.trace.RLNodeIndices)+1)
	fNodes[0] = fSrc
	fNodeJds := make([]*FieldNodeJumpDef, 1, len(j.trace.RLNodeIndices)+1)
	fNodeJds[0] = nil
	aNodeJds := make([]*FieldNodeJumpDef, 1, len(j.trace.ANodeIndices)+1)
	aNodeJds[0] = nil

	targetNodes := j.trace.RLNodeIndices[1:]
	targetNodes = append(targetNodes, j.trace.FirstStoreNodeIndex)

	for _, nodeIndex := range targetNodes {
		desNode := snodes[nodeIndex]
		j.ExecuteStats(fSrc.LRNode.Seq, desNode.Seq)
		fVId := j.nodeIndexToFVId[fSrc.LRNode.Seq]
		jd := fSrc.GetOrCreateFJumpDef(fVId)
		var fDes *FieldSearchTrieNode
		if jd.Dest == nil {
			if tt.DebugOut != nil {
				tt.DebugOut("NewFieldLaneFJump from %v to %v on FVId %v\n",
					fSrc.LRNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping),
					desNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping),
					fVId)
			}
			fDes = NewFieldSearchTrieNode(desNode)
			if !fDes.LRNode.Op.isStoreOp {
				fDes.SetAddrLoc(j.registers)
			}
			fDes.Exit = snodes[desNode.Seq+1]
			jd.Dest = fDes
			cmptypes.MyAssert(jd.RegisterSnapshot == nil && jd.FieldHistorySegment == nil)
			jd.RegisterSnapshot = j.registers.Slice(0, desNode.BeforeRegisterSize).ToRegisterFileSnapshot()
		} else {
			if tt.DebugOut != nil {
				tt.DebugOut("OldFieldLaneFJump from %v to %v on FVId %v\n",
					fSrc.LRNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping),
					desNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping),
					fVId)
			}
			fDes = jd.Dest
			if !fDes.LRNode.Op.isStoreOp {
				fDes.CheckAddrLoc(j.registers)
			}
			if tt.DebugOut != nil {
				cmptypes.MyAssert(fDes.LRNode == desNode)
				cmptypes.MyAssert(fDes.Exit == snodes[desNode.Seq+1])
				cmptypes.MyAssert(jd.FieldHistorySegment == nil)
				jd.RegisterSnapshot.AssertSnapshotEqual(j.registers.Slice(0, desNode.BeforeRegisterSize).ToRegisterFileSnapshot())
			}
		}
		fNodes = append(fNodes, fDes)
		fNodeJds = append(fNodeJds, jd)
		if fDes.LRNode.IsANode || fDes.LRNode.Op.isStoreOp {
			aNodeJds = append(aNodeJds, jd)
		}
		fSrc = fDes
		j.AddRefNode(jd)
		j.AddRefNode(fSrc)
	}

	// setup additional FieldAJumps
	if !j.noOverMatching {
		j.aNodeFJds = aNodeJds

		for si, fSrc := range fNodes {
			if fSrc.LRNode.Op.isLoadOp && (fSrc.LRNode.IsANode || fSrc.LRNode.IsTLNode) {
				di := si
				var fDes *FieldSearchTrieNode
				for di < len(fNodes)-1 {
					di++
					fDes = fNodes[di]
					if fDes.LRNode.Op.isReadOp || fDes.LRNode.Op.isStoreOp {
						break
					}
					cmptypes.MyAssert(fDes.LRNode.Op.isLoadOp)
					srcAddr := fSrc.LRNode.GetInputVal(0, j.registers).(*MultiTypedValue).GetAddress()
					desAddr := fDes.LRNode.GetInputVal(0, j.registers).(*MultiTypedValue).GetAddress()
					if srcAddr != desAddr {
						break
					}
				}
				dJd := fNodeJds[di]
				cmptypes.MyAssert(dJd.Dest == fDes)
				rsnap := dJd.RegisterSnapshot
				cmptypes.MyAssert(rsnap != nil)
				fieldSeg := j.fieldHistroy.Slice(fSrc.LRNode.FieldIndex, uint(di)+1)

				aVId := j.accountHistory.Get(fSrc.LRNode.AccountIndex).(uint)
				jd := fSrc.GetOrCreateAJumpDef(aVId)
				j.AddRefNode(jd)
				if jd.Dest == nil {
					if tt.DebugOut != nil {
						tt.DebugOut("NewFieldLaneAJump from %v to %v on AVId %v adding fields %v\n",
							fSrc.LRNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping),
							fDes.LRNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping),
							aVId, fieldSeg)
					}
					jd.Dest = fDes
					cmptypes.MyAssert(jd.RegisterSnapshot == nil && jd.FieldHistorySegment == nil)
					jd.RegisterSnapshot = rsnap
					jd.FieldHistorySegment = fieldSeg
				} else {
					if tt.DebugOut != nil {
						tt.DebugOut("OldFieldLaneAJump from %v to %v on AVId %v adding fields %v\n",
							fSrc.LRNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping),
							fDes.LRNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping),
							aVId, fieldSeg)
						cmptypes.MyAssert(jd.Dest == fDes)
						jd.RegisterSnapshot.AssertSnapshotEqual(rsnap)
						jd.FieldHistorySegment.AssertEqual(fieldSeg)
					}

				}
			}
		}
	}
	j.fnodes = fNodes

	//if j.newNodeCount > beforeNewNodeCount {
	storeFNode := fNodes[len(fNodes)-1]
	cmptypes.MyAssert(storeFNode.LRNode.Op.isStoreOp)
	lastFDes = storeFNode
	//storeFNode.Rounds = append(storeFNode.Rounds, j.round)
	//}
	return
}

func (j *JumpInserter) SetupAccountJumpLane() (lastADes *AccountSearchTrieNode) {
	if j.noOverMatching {
		cmptypes.MyAssert(false, "Should not setup Account Lane without overmatching!")
	}

	tt := j.tt
	snodes := j.snodes
	fnodes := j.fnodes

	//beforeNewNodeCount := j.newNodeCount

	registerMapping := &(j.trace.RAlloc)
	//accountHistory := NewRegisterFile(uint(len(j.trace.ANodeSet)))
	//fieldHistory := NewRegisterFile(uint(len(j.trace.RLNodeIndices)))

	// set up Account Search Track
	aSrc := tt.AccountHead
	if aSrc == nil {
		aSrc = NewAccountSearchTrieNode(snodes[0])
		aSrc.SetAddrLoc(j.registers)
		aSrc.Exit = fnodes[0]
		tt.AccountHead = aSrc
	} else {
		cmptypes.MyAssert(aSrc.LRNode == snodes[0])
		cmptypes.MyAssert(aSrc.Exit == fnodes[0])
		aSrc.CheckAddrLoc(j.registers)
	}
	j.AddRefNode(aSrc)

	targetNodes := j.trace.ANodeIndices[1:]
	targetNodes = append(targetNodes, j.trace.FirstStoreNodeIndex)

	for i, nodeIndex := range targetNodes {
		desNode := snodes[nodeIndex]
		aVId := j.nodeIndexToAVId[aSrc.LRNode.Seq]
		jd := aSrc.GetOrCreateAJumpDef(aVId)
		var aDes *AccountSearchTrieNode
		if jd.Dest == nil {
			if tt.DebugOut != nil {
				tt.DebugOut("NewAccountLaneAJump from %v to %v on aVId %v\n",
					aSrc.LRNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping),
					desNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping),
					aVId)
			}
			aDes = NewAccountSearchTrieNode(desNode)
			if !aDes.LRNode.Op.isStoreOp {
				aDes.SetAddrLoc(j.registers)
			}
			exitIndex := desNode.FieldIndex - 1
			if desNode.Op.isStoreOp {
				exitIndex = uint(len(fnodes) - 1)
			}
			aDes.Exit = fnodes[exitIndex]
			cmptypes.MyAssert(aDes.Exit.LRNode == desNode)
			jd.Dest = aDes
			cmptypes.MyAssert(jd.RegisterSnapshot == nil && jd.FieldHistorySnapshot == nil)
			cmptypes.MyAssert(j.aNodeFJds[i+1].Dest == aDes.Exit)
			jd.RegisterSnapshot = j.aNodeFJds[i+1].RegisterSnapshot
			cmptypes.MyAssert(jd.RegisterSnapshot != nil)
			endIndex := aDes.LRNode.FieldIndex
			if aDes.LRNode.Op.isStoreOp {
				endIndex = j.fieldHistroy.size
			}
			jd.FieldHistorySnapshot = j.fieldHistroy.Slice(0, endIndex).ToRegisterFileSnapshot()
		} else {
			if tt.DebugOut != nil {
				tt.DebugOut("OldAccountLaneAJump from %v to %v on AVId %v\n",
					aSrc.LRNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping),
					desNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping),
					aVId)
			}
			aDes = jd.Dest
			cmptypes.MyAssert(aDes.LRNode == desNode)
			if !aDes.LRNode.Op.isStoreOp {
				aDes.CheckAddrLoc(j.registers)
			}
			exitIndex := desNode.FieldIndex - 1
			if desNode.Op.isStoreOp {
				exitIndex = uint(len(fnodes) - 1)
			}
			cmptypes.MyAssert(aDes.Exit == fnodes[exitIndex])
			if tt.DebugOut != nil {
				jd.RegisterSnapshot.AssertSnapshotEqual(j.registers.Slice(0, jd.RegisterSnapshot.size).ToRegisterFileSnapshot())
			}
			endIndex := aDes.LRNode.FieldIndex
			if aDes.LRNode.Op.isStoreOp {
				endIndex = j.fieldHistroy.size
			}
			if tt.DebugOut != nil {
				jd.FieldHistorySnapshot.AssertSnapshotEqual(j.fieldHistroy.Slice(0, endIndex).ToRegisterFileSnapshot())
			}
		}
		aSrc = aDes
		j.AddRefNode(aSrc)
		j.AddRefNode(jd)
		if i == len(targetNodes)-1 {
			cmptypes.MyAssert(aDes.LRNode.Op.isStoreOp)
			cmptypes.MyAssert(aDes.LRNode.Seq == j.trace.FirstStoreNodeIndex)
			lastADes = aDes
			//if j.newNodeCount > beforeNewNodeCount {
			//	aDes.Rounds = append(aDes.Rounds, j.round)
			//}
		}
	}
	return
}

func CopyUArray(a []uint) []uint {
	b := make([]uint, len(a))
	copy(b, a)
	return b
}

func (j *JumpInserter) SetupOpJumpLane() {
	tt := j.tt
	snodes := j.snodes

	rMap := &(j.trace.RAlloc)

	// set up OpLJump for Non-A-Load
	if !j.noOverMatching {
		for _, nodeIndex := range j.trace.RLNodeIndices {
			node := snodes[nodeIndex]
			cmptypes.MyAssert(node.IsRLNode)
			if !node.IsANode {
				cmptypes.MyAssert(node.Op.isLoadOp)
				jn := node.JumpHead
				if jn == nil {
					jn = NewOpSearchTrieNode(node)
					node.JumpHead = jn
					jn.IsAccountJump = true
					jn.VIndices = []uint{node.AccountIndex}
					if tt.DebugOut != nil {
						tt.DebugOut("NewOpLaneLAJumpNode of %v on AIndex %v\n",
							node.Statement.SimpleNameStringWithRegisterAnnotation(rMap), jn.VIndices)
					}
				} else {
					if tt.DebugOut != nil {
						tt.DebugOut("OldOpLaneLAJumpNode of %v on AIndex %v\n",
							node.Statement.SimpleNameStringWithRegisterAnnotation(rMap), jn.VIndices)
						cmptypes.MyAssert(jn.OpNode == node)
						cmptypes.MyAssert(jn.IsAccountJump == true)
						cmptypes.MyAssert(len(jn.VIndices) == 1)
						cmptypes.MyAssert(jn.VIndices[0] == node.AccountIndex)
					}
				}

				aKey, aOk := GetOpJumKey(j.accountHistory, jn.VIndices)
				if DEBUG_TRACER {
					cmptypes.MyAssert(aOk)
					cmptypes.MyAssert(aKey != EMPTY_JUMP_KEY)
				}
				jd, newCreated := jn.GetOrCreateMapJumpDef(aKey)
				j.AddRefNode(jn)
				j.AddRefNode(jd)

				if newCreated {
					if tt.DebugOut != nil {
						tt.DebugOut("NewOpLaneLAJumpDef of %v on AIndex %v AVId %v\n",
							node.Statement.SimpleNameStringWithRegisterAnnotation(rMap), jn.VIndices, aKey[:1])
					}
				} else {
					if tt.DebugOut != nil {
						tt.DebugOut("OldOpLaneLAJumpDef of %v on AIndex %v AVId %v\n",
							node.Statement.SimpleNameStringWithRegisterAnnotation(rMap), jn.VIndices, aKey[:1])
					}
				}

				aDeps := node.AccountDepedenciesExcludingSelfAccount
				allADeps := MergeInputDependencies(aDeps, jn.VIndices)
				var aNodes []*OpSearchTrieNode

				for i := 0; i < len(aDeps); i += OP_JUMP_KEY_SIZE {
					start := i
					end := i + OP_JUMP_KEY_SIZE
					if end > len(aDeps) {
						end = len(aDeps)
					}
					aIndices := aDeps[start:end]
					aKey, aOk := GetOpJumKey(j.accountHistory, aIndices)
					if DEBUG_TRACER {
						cmptypes.MyAssert(aOk)
					}
					var aNode *OpSearchTrieNode
					if newCreated {
						if tt.DebugOut != nil {
							tt.DebugOut("NewOpLaneLAJumpNode #%v of %v on AIndex %v AVId %v\n",
								i, node.Statement.SimpleNameStringWithRegisterAnnotation(rMap), aIndices, aKey[:end-start])
						}
						aNode = NewOpSearchTrieNode(node)
						aNode.IsAccountJump = true
						aNode.VIndices = CopyUArray(aIndices)
						jd.Next = aNode
					} else {
						if tt.DebugOut != nil {
							tt.DebugOut("OldOpLaneLAJumpNode #%v of %v on AIndex %v AVId %v\n",
								i, node.Statement.SimpleNameStringWithRegisterAnnotation(rMap), aIndices, aKey[:end-start])
						}
						aNode = jd.Next
						if DEBUG_TRACER {
							cmptypes.MyAssert(aNode != nil)
							cmptypes.MyAssert(aNode.IsAccountJump == true)
							AssertUArrayEqual(aNode.VIndices, aIndices)
							cmptypes.MyAssert(jd.FieldHistorySegment == nil)
							cmptypes.MyAssert(jd.RegisterSegment == nil)
						}
					}
					jd, newCreated = aNode.GetOrCreateMapJumpDef(aKey)
					if newCreated {
						if tt.DebugOut != nil {
							tt.DebugOut("NewOpLaneLAJumpDef #%v of %v on AIndex %v AVId %v\n",
								i, node.Statement.SimpleNameStringWithRegisterAnnotation(rMap), aIndices, aKey[:end-start])
						}
					} else {
						if tt.DebugOut != nil {
							tt.DebugOut("OldOpLaneLAJumpDef #%v of %v on AIndex %v AVId %v\n",
								i, node.Statement.SimpleNameStringWithRegisterAnnotation(rMap), aIndices, aKey[:end-start])
						}
					}
					j.AddRefNode(aNode)
					j.AddRefNode(jd)
					aNodes = append(aNodes, aNode)
				}

				desNode := snodes[nodeIndex+1]
				for {
					cmptypes.MyAssert(desNode == snodes[desNode.Seq])
					if desNode.Op.isStoreOp {
						break
					}
					diff := desNode.AccountDependencies.SetDiff(allADeps)
					if len(diff) == 0 {
						desNode = snodes[desNode.Seq+1]
					} else {
						break
					}
				}

				if newCreated {
					if tt.DebugOut != nil {
						tt.DebugOut("NewOpLaneLAJump from %v to %v on aKey %v\n",
							node.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
							desNode.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
							aKey[:1])
					}
					jd.Next = nil
					jd.Dest = desNode
					cmptypes.MyAssert(node.OutputRegisterIndex != 0)
					jd.RegisterSegment = j.registers.Slice(node.OutputRegisterIndex, desNode.BeforeRegisterSize)
					cmptypes.MyAssert(node.FieldIndex != 0)
					jd.FieldHistorySegment = j.fieldHistroy.Slice(node.FieldIndex, desNode.BeforeFieldHistorySize)
				} else {
					if tt.DebugOut != nil {
						tt.DebugOut("OldOpLaneLAJump from %v to %v on aKey %v\n",
							node.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
							desNode.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
							aKey[:1])
						cmptypes.MyAssert(jd.Next == nil)
						cmptypes.MyAssert(jd.Dest == desNode)
						jd.RegisterSegment.AssertEqual(j.registers.Slice(node.OutputRegisterIndex, desNode.BeforeRegisterSize))
						jd.FieldHistorySegment.AssertEqual(j.fieldHistroy.Slice(node.FieldIndex, desNode.BeforeFieldHistorySize))
					}
				}

				if len(aNodes) > 0 {
					//cmptypes.MyAssert(len(node.FieldDependenciesExcludingSelfAccount) > 0)
					cmptypes.MyAssert(len(node.FieldDependencies) > 0)
					if aNodes[0].Exit == nil {
						jd = &OpNodeJumpDef{}
						for _, n := range aNodes {
							cmptypes.MyAssert(n.Exit == nil)
							n.Exit = jd
						}
					} else {
						firstExist := aNodes[0].Exit
						cmptypes.MyAssert(firstExist != nil)
						for _, n := range aNodes[1:] {
							if n.Exit != nil {
								cmptypes.MyAssert(n.Exit == firstExist)
							} else {
								n.Exit = firstExist
							}
						}
						jd = firstExist
					}

					//				fDeps := node.FieldDependenciesExcludingSelfAccount
					fDeps := node.FieldDependencies

					for i := 0; i < len(fDeps); i += OP_JUMP_KEY_SIZE {
						start := i
						end := i + OP_JUMP_KEY_SIZE
						if end > len(fDeps) {
							end = len(fDeps)
						}
						fIndices := fDeps[start:end]
						fKey, fOk := GetOpJumKey(j.fieldHistroy, fIndices)
						if DEBUG_TRACER {
							cmptypes.MyAssert(fOk)
						}
						var fNode *OpSearchTrieNode
						if newCreated {
							if tt.DebugOut != nil {
								tt.DebugOut("NewOpLaneLFJumpNode #%v of %v on FIndex %v\n",
									i, node.Statement.SimpleNameStringWithRegisterAnnotation(rMap), fIndices)
							}
							fNode = NewOpSearchTrieNode(node)
							fNode.IsAccountJump = false
							fNode.VIndices = CopyUArray(fIndices)
							jd.Next = fNode
						} else {
							if tt.DebugOut != nil {
								tt.DebugOut("OldOpLaneLFJumpNode #%v of %v on FIndex %v\n",
									i, node.Statement.SimpleNameStringWithRegisterAnnotation(rMap), fIndices)
							}
							fNode = jd.Next
							cmptypes.MyAssert(fNode != nil)
							cmptypes.MyAssert(fNode.IsAccountJump == false)
							AssertUArrayEqual(fNode.VIndices, fIndices)
							cmptypes.MyAssert(jd.FieldHistorySegment == nil)
							cmptypes.MyAssert(jd.RegisterSegment == nil)
						}
						jd, newCreated = fNode.GetOrCreateMapJumpDef(fKey)
						if newCreated {
							if tt.DebugOut != nil {
								tt.DebugOut("NewOpLaneLFJumpDef #%v of %v on FIndex %v FVId %v\n",
									i, node.Statement.SimpleNameStringWithRegisterAnnotation(rMap), fIndices, fKey[:end-start])
							}
						} else {
							if tt.DebugOut != nil {
								tt.DebugOut("OldOpLaneLFJumpDef #%v of %v on FIndex %v FVId %v\n",
									i, node.Statement.SimpleNameStringWithRegisterAnnotation(rMap), fIndices, fKey[:end-start])
							}
						}
						j.AddRefNode(fNode)
						j.AddRefNode(jd)
					}

					desNode := snodes[nodeIndex+1]

					if newCreated {
						if tt.DebugOut != nil {
							tt.DebugOut("NewOpLaneLHJump from %v to %v on aKey %v\n",
								node.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
								desNode.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
								aKey[:1])
						}
						jd.Next = nil
						jd.Dest = desNode
						cmptypes.MyAssert(node.OutputRegisterIndex != 0)
						jd.RegisterSegment = j.registers.Slice(node.OutputRegisterIndex, node.OutputRegisterIndex+1)
						cmptypes.MyAssert(node.FieldIndex != 0)
						jd.FieldHistorySegment = j.fieldHistroy.Slice(node.FieldIndex, node.FieldIndex+1)
					} else {
						if tt.DebugOut != nil {
							tt.DebugOut("OldOpLaneLHJump from %v to %v on aKey %v\n",
								node.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
								desNode.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
								aKey[:1])
							cmptypes.MyAssert(jd.Next == nil)
							cmptypes.MyAssert(jd.Dest == desNode)
							jd.RegisterSegment.AssertEqual(j.registers.Slice(node.OutputRegisterIndex, node.OutputRegisterIndex+1))
							jd.FieldHistorySegment.AssertEqual(j.fieldHistroy.Slice(node.FieldIndex, node.FieldIndex+1))
						}
					}
				} else {
					cmptypes.MyAssert(len(node.FieldDependenciesExcludingSelfAccount) == 0)
				}
			}
		}
	}

	// setup for other Compute/Guard JSPs
	nodeBudget := len(j.trace.Stats) * 5 // shared budget

	jumpStations := j.trace.RLNodeIndices[:]
	jumpStations = append(jumpStations, j.trace.FirstStoreNodeIndex)

	fDeps := make(InputDependence, 0, 100)
	fDepBuffer := make(InputDependence, 0, 100)
	newFDeps := make(InputDependence, 0, 100)
	aDeps := make(InputDependence, 0, 100)
	aDepBuffer := make(InputDependence, 0, 100)
	newADeps := make(InputDependence, 0, 100)

	for i, nodeIndex := range jumpStations[:len(jumpStations)-1] {
		targetNodeIndex := jumpStations[i+1]
		targetNode := snodes[targetNodeIndex]
		cmptypes.MyAssert(targetNodeIndex > nodeIndex)
		cmptypes.MyAssert(targetNode.IsRLNode || targetNode.Op.isStoreOp)
		cmptypes.MyAssert(targetNode == snodes[targetNode.Seq])
		if targetNodeIndex-nodeIndex == 1 {
			continue
		}
		jumpSrc := snodes[nodeIndex]
		cmptypes.MyAssert(jumpSrc.IsRLNode)
		for {
			// find out JumpSrc
			for {
				jumpSrc = snodes[jumpSrc.Seq+1]
				if jumpSrc == targetNode {
					break
				}
				cmptypes.MyAssert(!jumpSrc.IsRLNode)
				if jumpSrc.IsJSPNode {
					break
				}
			}

			if jumpSrc == targetNode {
				break
			}
			cmptypes.MyAssert(!jumpSrc.IsRLNode && jumpSrc.IsJSPNode)

			// create OpFJumps
			nodeBudget += 5 // allocate 5 nodes budget for the FJumps of each JSP
			nodeIndexToFJumpNodes := make(map[uint]*OpSearchTrieNode)
			jumpDes := jumpSrc
			jd := &OpNodeJumpDef{} // head.Exit
			fjdHead := jd
			if jumpSrc.JumpHead != nil {
				if j.noOverMatching {
					jd.Next = jumpSrc.JumpHead
					if jumpSrc.JumpHead.IsAccountJump {
						cmptypes.MyAssert(false, "Unexpected Account Jump when overmatching is disabled.")
					}
				} else {
					jd = jumpSrc.JumpHead.Exit
				}
				cmptypes.MyAssert(jd != nil)
			}
			j.AddRefNode(jd)
			newCreatedJd := false
			jdFromNode := jumpSrc
			var jdFromFIndices []uint
			var jdFromFKey OpJumpKey
			fNodeSeq := 0

			fDeps = fDeps[:0]
			fDepBuffer = fDepBuffer[:0]
			newFDeps = newFDeps[:0]
			//fDeps := make(InputDependence, 0, 100)
			//fDepBuffer := make(InputDependence, 0, 100)
			//newFDeps := make(InputDependence, 0, 100)
			for {
				if jumpDes != targetNode {
					newFDeps = jumpDes.FieldDependencies.SetDiffWithPreallocation(fDeps, newFDeps[:0])
				}

				if (jdFromNode != jumpDes) && (len(newFDeps) > 0 || jumpDes == targetNode) {
					if newCreatedJd {
						if tt.DebugOut != nil {
							tt.DebugOut("NewOpLaneFJump from %v to %v on FIndex %v FVId %v\n",
								jdFromNode.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
								jumpDes.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
								jdFromFIndices, jdFromFKey[:len(jdFromFIndices)])
						}
						jd.RegisterSegment = j.registers.Slice(jdFromNode.BeforeRegisterSize, jumpDes.BeforeRegisterSize)
						if jumpDes == targetNode {
							jd.Dest = targetNode
						}
					} else {
						if tt.DebugOut != nil {
							tt.DebugOut("OldOpLaneFJump from %v to %v on FIndex %v FVId %v\n",
								jdFromNode.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
								jumpDes.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
								jdFromFIndices, jdFromFKey[:len(jdFromFIndices)])
							cmptypes.MyAssert(jd.FieldHistorySegment == nil)
							jd.RegisterSegment.AssertEqual(j.registers.Slice(jdFromNode.BeforeRegisterSize, jumpDes.BeforeRegisterSize))
							if jumpDes == targetNode {
								cmptypes.MyAssert(jd.Next == nil)
								cmptypes.MyAssert(jd.Dest != nil)
								cmptypes.MyAssert(jd.Dest == targetNode, "%v", jd.Dest.Statement.SimpleNameStringWithRegisterAnnotation(rMap))
							}
						}
					}
				}

				if jumpDes == targetNode {
					break
				}

				for i := 0; i < len(newFDeps); i += OP_JUMP_KEY_SIZE {
					start := i
					end := start + OP_JUMP_KEY_SIZE
					if end > len(newFDeps) {
						end = len(newFDeps)
					}
					fIndices := newFDeps[start:end]
					fKey, fOk := GetOpJumKey(j.fieldHistroy, fIndices)
					if DEBUG_TRACER {
						cmptypes.MyAssert(fOk)
					}
					fjn := jd.Next
					if fjn == nil {
						if tt.DebugOut != nil {
							tt.DebugOut("NewOpLaneFJump #%v of %v on FIndex %v with FVId %v\n",
								fNodeSeq, jumpDes.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
								fIndices, fKey[:len(fIndices)])
						}
						fNodeSeq++
						fjn = NewOpSearchTrieNode(jumpDes)
						fjn.VIndices = CopyUArray(fIndices)
						jd.Next = fjn
						nodeBudget--
						if nodeBudget <= 0 {
							if tt.DebugOut != nil {
								tt.DebugOut("Out of node Budget stop creating more OpLaneFJump\n")
							}
							break
						}
					} else {
						if tt.DebugOut != nil {
							tt.DebugOut("OldOpLaneFJump #%v of %v on FIndex %v with FVId %v\n",
								fNodeSeq, jumpDes.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
								fIndices, fKey[:len(fIndices)])
						}
						fNodeSeq++
						AssertUArrayEqual(fjn.VIndices, fIndices)
						cmptypes.MyAssert(fjn.IsAccountJump == false)
						cmptypes.MyAssert(fjn.OpNode == jumpDes)
					}

					if _, ok := nodeIndexToFJumpNodes[jumpDes.Seq]; !ok {
						cmptypes.MyAssert(i == 0)
						nodeIndexToFJumpNodes[jumpDes.Seq] = fjn // record the first fjump node for each snode
					}

					jd, newCreatedJd = fjn.GetOrCreateMapJumpDef(fKey)
					jdFromNode = jumpDes
					jdFromFIndices = fIndices
					jdFromFKey = fKey
					j.AddRefNode(fjn)
					j.AddRefNode(jd)
				}

				if nodeBudget <= 0 {
					break
				}

				if len(newFDeps) > 0 {
					fRet := MergeInputDependenciesWithPreallocation(fDepBuffer[:0], fDeps, newFDeps)
					fDepBuffer = fDeps
					fDeps = fRet
				}

				cmptypes.MyAssert(snodes[jumpDes.Seq] == jumpDes)
				jumpDes = snodes[jumpDes.Seq+1]
			}

			if j.noOverMatching {
				if jumpSrc.JumpHead == nil {
					jumpSrc.JumpHead = fjdHead.Next
				}
			} else {
				// create OpAJumps
				nodeBudget += 5
				jumpDes = jumpSrc
				jdFromNode = jumpSrc
				var jdFromAIndices []uint
				var jdFromAKey OpJumpKey
				jd = &OpNodeJumpDef{}
				if jumpSrc.JumpHead != nil {
					jd.Next = jumpSrc.JumpHead
				}
				j.AddRefNode(jd)
				jdHead := jd
				newCreatedJd = false
				//aDeps := make(InputDependence, 0, 100)
				//aDepBuffer := make(InputDependence, 0, 100)
				//newADeps := make(InputDependence, 0, 100)
				aDeps = aDeps[:0]           //make(InputDependence, 0, 100)
				aDepBuffer = aDepBuffer[:0] //make(InputDependence, 0, 100)
				newADeps = newADeps[:0]     //make(InputDependence, 0, 100)
				aNodeSeq := 0
				for {

					if jumpDes != targetNode {
						newADeps = jumpDes.AccountDependencies.SetDiffWithPreallocation(aDeps, newADeps[:0])
					}

					if (jdFromNode != jumpDes) && (len(newADeps) > 0 || jumpDes == targetNode) {
						if newCreatedJd {
							if tt.DebugOut != nil {
								tt.DebugOut("NewOpLaneAJump from %v to %v on AIndex %v AVId %v\n",
									jdFromNode.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
									jumpDes.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
									jdFromAIndices, jdFromAKey[:len(jdFromAIndices)])
							}
							jd.RegisterSegment = j.registers.Slice(jdFromNode.BeforeRegisterSize, jumpDes.BeforeRegisterSize)
							if jumpDes == targetNode {
								jd.Dest = targetNode
							}
						} else {
							if tt.DebugOut != nil {
								tt.DebugOut("OldOpLaneAJump from %v to %v on AIndex %v AVId %v\n",
									jdFromNode.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
									jumpDes.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
									jdFromAIndices, jdFromAKey[:len(jdFromAIndices)])
								cmptypes.MyAssert(jd.FieldHistorySegment == nil)
								jd.RegisterSegment.AssertEqual(j.registers.Slice(jdFromNode.BeforeRegisterSize, jumpDes.BeforeRegisterSize))
								if jumpDes == targetNode {
									cmptypes.MyAssert(jd.Next == nil)
									cmptypes.MyAssert(jd.Dest == targetNode)
								}
							}
						}
					}

					if jumpDes == targetNode {
						break
					}

					for i := 0; i < len(newADeps); i += OP_JUMP_KEY_SIZE {
						start := i
						end := start + OP_JUMP_KEY_SIZE
						if end > len(newADeps) {
							end = len(newADeps)
						}
						aIndices := newADeps[start:end]
						aKey, aOk := GetOpJumKey(j.accountHistory, aIndices)
						if DEBUG_TRACER {
							cmptypes.MyAssert(aOk)
						}
						ajn := jd.Next
						if ajn == nil {
							if tt.DebugOut != nil {
								tt.DebugOut("NewOpLaneAJump #%v of %v on AIndex %v with AVId %v\n",
									aNodeSeq, jumpDes.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
									aIndices, aKey[:len(aIndices)])
							}
							aNodeSeq++
							ajn = NewOpSearchTrieNode(jumpDes)
							ajn.IsAccountJump = true
							ajn.VIndices = CopyUArray(aIndices)

							fjn, ok := nodeIndexToFJumpNodes[jumpDes.Seq]
							if ok {
								cmptypes.MyAssert(fjn.OpNode == jumpDes)
								cmptypes.MyAssert(fjn.IsAccountJump == false)
								ajn.Exit = &OpNodeJumpDef{Next: fjn}
							}
							jd.Next = ajn
							nodeBudget--
							if nodeBudget <= 0 {
								if tt.DebugOut != nil {
									tt.DebugOut("Out of node Budget, stop creating more OpLaneAJump\n")
								}
								break
							}
						} else {
							if tt.DebugOut != nil {
								tt.DebugOut("OldOpLaneAJump #%v of %v on AIndex %v with AVId %v\n",
									aNodeSeq, jumpDes.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
									aIndices, aKey[:len(aIndices)])
							}
							aNodeSeq++
							AssertUArrayEqual(ajn.VIndices, aIndices)
							cmptypes.MyAssert(ajn.IsAccountJump == true)
							cmptypes.MyAssert(ajn.OpNode == jumpDes)
							if ajn.Exit != nil {
								fjn, ok := nodeIndexToFJumpNodes[jumpDes.Seq]
								cmptypes.MyAssert(ok)
								cmptypes.MyAssert(ajn.Exit.Next == fjn)
								cmptypes.MyAssert(fjn.OpNode == jumpDes)
							}
						}

						jd, newCreatedJd = ajn.GetOrCreateMapJumpDef(aKey)
						jdFromNode = jumpDes
						jdFromAIndices = aIndices
						jdFromAKey = aKey
						j.AddRefNode(ajn)
						j.AddRefNode(jd)
					}

					if nodeBudget <= 0 {
						break
					}

					if len(newADeps) > 0 {
						aRet := MergeInputDependenciesWithPreallocation(aDepBuffer[:0], aDeps, newADeps)
						aDepBuffer = aDeps
						aDeps = aRet
					}

					cmptypes.MyAssert(snodes[jumpDes.Seq] == jumpDes)
					jumpDes = snodes[jumpDes.Seq+1]
				}

				if jumpSrc.JumpHead == nil {
					jumpSrc.JumpHead = jdHead.Next
				}
			}
		}
	}

}

func (j *JumpInserter) SetupRoundMapping() {
	if j.noOverMatching {
		return
	}
	node := j.snodes[j.trace.FirstStoreNodeIndex]
	cmptypes.MyAssert(node.Op.isStoreOp)
	node.roundResults.SetupRoundMapping(j.registers, j.accountHistory, j.fieldHistroy, j.round, j.tt.DebugOut)
}

func (tt *TraceTrie) setupJumps(trace *STrace, currentNodes []*SNode, round *cache.PreplayResult, result *TraceTrieSearchResult, noOverMatching bool) *JumpInserter {
	j := NewJumpInserter(tt, trace, currentNodes, round, result, noOverMatching)
	fDes := j.SetupFieldJumpLane()
	var aDes *AccountSearchTrieNode
	if !j.noOverMatching {
		aDes = j.SetupAccountJumpLane()
	}
	j.SetupOpJumpLane()
	if j.newNodeCount > 0 {
		// do not store repeated round
		fDes.Rounds = append(fDes.Rounds, round)
		if aDes != nil {
			aDes.Rounds = append(aDes.Rounds, round)
		}
		j.SetupRoundMapping()
	}
	return j
}
