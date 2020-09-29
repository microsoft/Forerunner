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
}

func NewSTrace(stats []*Statement, debugOut DebugOutFunc, buffer *DebugBuffer) *STrace {
	st := &STrace{}
	PrintStatementsWithWriteOut(stats, debugOut)
	stats = ExecutePassAndPrintWithOut(EliminateRevertedStore, stats, debugOut, false)
	stats = ExecutePassAndPrintWithOut(EliminateRedundantStore, stats, debugOut, false)
	stats = ExecutePassAndPrintWithOut(EliminateSuicidedStore, stats, debugOut, false)

	st.CrosscheckStats = stats

	stats = ExecutePassAndPrintWithOut(EliminateUnusedStatement, stats, debugOut, false)
	stats = ExecutePassAndPrintWithOut(PushAllStoresToBottom, stats, debugOut, false)
	stats = ExecutePassAndPrintWithOut(PushLoadReadComputationDownToFirstUse, stats, debugOut, false)
	st.Stats = stats
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
	RefNodes []ISRefCountNode
	RoundID  uint64
	Round    *cache.PreplayResult
	Next     *TrieRoundRefNodes
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
	SRefCount
	BigSize
}

func NewAccountSearchTrieNode(node *SNode) *AccountSearchTrieNode {
	return &AccountSearchTrieNode{
		LRNode: node,
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
	SRefCount
	BigSize
}

func NewFieldSearchTrieNode(node *SNode) *FieldSearchTrieNode {
	fn := &FieldSearchTrieNode{
		LRNode: node,
	}
	return fn
}

func (n *FieldSearchTrieNode) RemoveRef(roundID uint64) {
	n.SRemoveRef()
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

func GetDepsKey(depIndices []uint, history *RegisterFile) (string, bool, []uint) {
	buf := strings.Builder{}
	fvids := make([]uint, len(depIndices))
	cmptypes.MyAssert(reflect.TypeOf(uint(0)).Size() == 8)
	for i, index := range depIndices {
		vId := history.Get(index).(uint)
		if vId == 0 {
			return "", false, nil
		} else {
			buf.Write(((*[8]byte)(unsafe.Pointer(&(vId))))[:])
			fvids[i] = vId
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
			adk, aok, avids := GetDepsKey(accountDeps, accounts)
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
			afk, fok, fvids := GetDepsKey(fieldDeps, fields)
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
	FieldValueToID                         map[interface{}]uint
	AccountValueToID                       map[interface{}]uint
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
		if st.ANodeSet[nodeIndex] {
			n.AccountIndex = st.ANodeIndexToAccountIndex[nodeIndex]
			n.IsANode = true
			n.AccountValueToID = make(map[interface{}]uint)
		} else {
			n.AccountIndex = st.RLNodeIndexToAccountIndex[nodeIndex]
			n.IsTLNode = st.TLNodeSet[nodeIndex]
		}
		n.IsRLNode = true
		n.FieldIndex = st.RLNodeIndexToFieldIndex[nodeIndex]
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

func (n *SNode) Execute(env *ExecEnv, registers *RegisterFile) interface{} {
	env.inputs = n.InputVals
	for i, rid := range n.InputRegisterIndices {
		if rid != 0 {
			env.inputs[i] = registers.Get(rid)
		}
	}
	env.config = &n.Op.config
	return CreateMultiTypedValueIfNeed(n.Op.impFuc(env), env)
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

func (n *SNode) GetOrLoadAccountValueID(env *ExecEnv, registers *RegisterFile, accountHistory *RegisterFile) (uint, bool) {
	cmptypes.MyAssert(n.Op.isLoadOp || n.Op.isReadOp)
	//cmptypes.MyAssert(n.IsANode)
	if n.AccountIndex < accountHistory.size {
		return accountHistory.Get(n.AccountIndex).(uint), false
	}

	var vid uint
	if n.Op.isReadOp {
		val := n.Execute(env, registers)
		k := GetGuardKey(val)
		vid = n.AccountValueToID[k]
	} else {
		var addr common.Address
		if n.InputRegisterIndices[0] == 0 {
			addr = n.InputVals[0].(*MultiTypedValue).GetAddress()
		} else {
			addr = registers.Get(n.InputRegisterIndices[0]).(*MultiTypedValue).GetAddress()
		}
		changedBy := env.state.GetAccountSnapOrChangedBy(addr)
		k := GetGuardKey(changedBy)
		vid = n.AccountValueToID[k]
	}

	if DEBUG_TRACER {
		cmptypes.MyAssert(accountHistory.firstSectionSealed == false)
	}
	accountHistory.AppendRegister(n.AccountIndex, vid)
	return vid, true
}

func (n *SNode) LoadFieldValueID(env *ExecEnv, registers *RegisterFile, fieldHistory *RegisterFile) (uint, interface{}) {
	cmptypes.MyAssert(n.Op.isLoadOp || n.Op.isReadOp)

	val := n.Execute(env, registers)
	k := GetGuardKey(val)
	vid := n.FieldValueToID[k]
	fieldHistory.AppendRegister(n.FieldIndex, vid)
	return vid, val
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
		panic(fmt.Sprintf("Unknown type: %v", reflect.TypeOf(a).Name()))
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
}

func NewTraceTrie(tx *types.Transaction) *TraceTrie {
	tt := &TraceTrie{
		Head:         nil,
		Tx:           tx,
		PathCount:    0,
		RegisterSize: 0,
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
	getHashFunc vm.GetHashFunc, precompiles map[common.Address]vm.PrecompiledContract, globalCache *cache.GlobalCache) (env *ExecEnv) {
	if len(tt.PreAllocatedExecEnvs) > 0 {
		env = tt.PreAllocatedExecEnvs[0]
		tt.PreAllocatedExecEnvs = tt.PreAllocatedExecEnvs[1:]
	} else {
		env = NewExecEnv()
	}
	env.state = db
	env.header = header
	env.precompiles = precompiles
	env.isProcess = true
	env.getHash = getHashFunc
	return
}

func (tt *TraceTrie) GetNewHistory() (rf *RegisterFile) {
	if len(tt.PreAllocatedHistory) > 0 {
		rf = tt.PreAllocatedHistory[0]
		tt.PreAllocatedHistory = tt.PreAllocatedHistory[1:]
	} else {
		rf = NewRegisterFile(1000)
	}
	return
}

func (tt *TraceTrie) GetNewRegisters() (rf *RegisterFile) {
	if len(tt.PreAllocatedRegisters) > 0 {
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

func GetNodeIndexToAccountSnapMapping(trace *STrace, readDep []*cmptypes.AddrLocValue) map[uint]interface{} {
	ret := make(map[uint]interface{})
	temp := make(map[string]interface{})

	for _, alv := range readDep {
		var key string
		val := alv.Value
		switch alv.AddLoc.Field {
		case cmptypes.Dependence:
			key = alv.AddLoc.Address.Hex()
			val = val.(*cmptypes.ChangedBy).Hash()
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
			}
			ret[nodeIndex] = temp[key]
		}
	}

	return ret
}

func (tt *TraceTrie) InsertTrace(trace *STrace, round *cache.PreplayResult) {
	gcMutex.Lock()
	defer gcMutex.Unlock()
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
		tt.DebugOut("InsertTrace\n")
	}
	currentTraceNodes := make([]*SNode, len(stats))
	refNodes := make([]ISRefCountNode, len(stats))
	newNodeCount := uint(0)
	for i, s := range stats {
		n, gk = tt.insertStatement(n, gk, s, uint(i), trace)
		cmptypes.MyAssert(n.Seq == uint(i))
		//n.Seq = uint(i)
		currentTraceNodes[i] = n
		refNodes[i] = n
		if n.GetRefCount() == 0 {
			newNodeCount++
		}
	}

	cmptypes.MyAssert(tt.Head == nil || tt.Head == preHead.Next)
	tt.Head = preHead.Next

	firstStoreNode := currentTraceNodes[trace.FirstStoreNodeIndex]
	firstStoreNode.AddStoreInfo(round, trace)

	ji := tt.setupJumps(trace, currentTraceNodes, round)

	tt.UpdateTraceSize(trace.RegisterFileSize)

	refNodes = append(refNodes, ji.refNodes...)
	tt.TrackRoundRefNodes(refNodes, newNodeCount+ji.newNodeCount, round)
}

func (tt *TraceTrie) TrackRoundRefNodes(refNodes []ISRefCountNode, newNodeCount uint, round *cache.PreplayResult) {
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
			RefNodes: refNodes,
			RoundID:  round.RoundID,
			Round:    round,
		}

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
	if tt.RefedRoundCount > uint(config.TXN_PREPLAY_ROUND_LIMIT+1) {
		for i := 0; i < 1; i++ {
			head := tt.RoundRefNodesHead
			removed := tt.RemoveRoundRef(head)
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
	env                          *ExecEnv
	debugOut                     DebugOutFunc
	fOut                         *os.File
	accountLaneRounds            []*cache.PreplayResult
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

func (r *TraceTrieSearchResult) ApplyStores(txPreplay *cache.TxPreplay, traceStatus *cmptypes.TraceStatus, abort func() bool) (bool, cmptypes.TxResIDMap) {
	cmptypes.MyAssert(r.Node.Op.isStoreOp)
	resMap := r.applyWObjects(txPreplay)
	traceStatus.AccountWrittenCount = uint64(len(r.accountIndexToAppliedWObject))
	for n := r.Node; !n.Op.IsVirtual(); n = n.Next {
		if abort != nil && abort() {
			return true, nil
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
			traceStatus.ExecutedNodes++
		}
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

func (r *TraceTrieSearchResult) applyWObjects(txPreplay *cache.TxPreplay) cmptypes.TxResIDMap {
	cmptypes.MyAssert(r.accountIndexToAppliedWObject == nil)
	r.accountIndexToAppliedWObject = r.env.GetNewAIdToApplied()

	accountLaneHit := r.accountLaneRounds != nil
	rounds := r.accountLaneRounds
	resMap := r.env.GetNewResMap()
	for _, si := range r.storeInfo.StoreAccountIndices {
		aVId, addr := r.getAVIdAndAddress(si)
		resMap[addr] = cmptypes.DEFAULT_TXRESID
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
			r.applyObjectInRounds(txPreplay, rounds, addr, resMap, si, aVId)
		}
	}
	return resMap
}

func (r *TraceTrieSearchResult) getAVIdAndAddress(si uint) (uint, common.Address) {
	aVId := r.accountHistory.Get(si).(uint)
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
	return aVId, addr
}

func (r *TraceTrieSearchResult) applyObjectInRounds(txPreplay *cache.TxPreplay, rounds []*cache.PreplayResult, addr common.Address, resMap cmptypes.TxResIDMap, si uint, aVId uint) {
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
	dk, dok, dvids := GetDepsKey(accountDeps, r.accountHistory)
	rounds = nil
	if dok {
		rounds = rm.AccountDepsToRounds[dk]
		if rounds != nil && r.debugOut != nil {
			r.debugOut("Account %v AVId %v full match by AccountDeps %v of %v with keyLen %v\n", si, aVId, accountDeps, dvids, len(dk))
		}
	}
	if rounds == nil {
		fieldDeps := r.storeInfo.StoreAccountIndexToStoreFieldDependencies[si]
		fk, fok, fvids := GetDepsKey(fieldDeps, r.fieldHistory)
		if fok {
			rounds = rm.FieldDepsToRounds[fk]
			if rounds != nil && r.debugOut != nil {
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
	precompiles map[common.Address]vm.PrecompiledContract, abort func() bool, debug bool, globalCache *cache.GlobalCache) (result *TraceTrieSearchResult) {

	var node *SNode
	debugOut, fOut, env, registers, fieldHistory, accountHistory := tt.initSearch(db, header, getHashFunc, precompiles, globalCache, debug)

	var accountLaneRounds []*cache.PreplayResult
	var (
		traceStatus             = &cmptypes.TraceStatus{}
		totalJumps              = &traceStatus.TotalJumps
		failedJumps             = &traceStatus.FailedJumps
		aJumps                  = &traceStatus.AJumps
		fJumps                  = &traceStatus.FJumps
		oJumps                  = &traceStatus.OJumps
		totalJumpKeys           = &traceStatus.TotalJumpKeys
		executedNodes           = &traceStatus.ExecutedNodes
		executedInputNodes      = &traceStatus.ExecutedInputNodes
		executedChainInputNodes = &traceStatus.ExecutedChainInputNodes
		accountReadCount        = &traceStatus.AccountReadCount
		accountReadFailedCount  = &traceStatus.AccountReadFailedCount
		fieldActualReadCount    = &traceStatus.FieldActualReadCount
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
		result = &TraceTrieSearchResult{
			Node:              node,
			hit:               hit,
			registers:         registers,
			fieldHistory:      fieldHistory,
			accountHistory:    accountHistory,
			env:               env,
			debugOut:          debugOut,
			fOut:              fOut,
			storeInfo:         node.roundResults,
			accountLaneRounds: accountLaneRounds,
			aborted:           aborted,
			TraceStatus:       traceStatus,
		}
	}()

	if debugOut != nil {
		debugOut("Start Search Trie for Tx %v\n", tt.Tx.Hash().Hex())
	}

	// start with the AccountLane
	var fNode *FieldSearchTrieNode

	node, fNode, accountLaneRounds, aborted, hit = tt.searchAccountLane(tt.AccountHead,
		env, registers, accountHistory, fieldHistory,
		totalJumps, totalJumpKeys, failedJumps, aJumps,
		executedNodes, executedInputNodes, executedChainInputNodes,
		accountReadCount, accountReadFailedCount, fieldActualReadCount,
		debug, debugOut,
		abort)

	if aborted || hit {
		return
	}

	fNode, node, aborted, hit = tt.searchFieldLane(fNode, node,
		env, registers, accountHistory, fieldHistory,
		totalJumps, totalJumpKeys, failedJumps, fJumps,
		executedNodes, executedInputNodes, executedChainInputNodes,
		accountReadCount, accountReadFailedCount, fieldActualReadCount,
		debug, debugOut, abort)

	if aborted || hit {
		return
	}

	// start the OpLane
	if debug {
		cmptypes.MyAssert(fNode.Exit == node)
		cmptypes.MyAssert(!node.Op.isStoreOp)
	}

	node, aborted, hit = tt.searchOpLane(node,
		env, accountHistory, fieldHistory, registers,
		totalJumps, totalJumpKeys, failedJumps, oJumps,
		executedNodes, executedInputNodes, executedChainInputNodes,
		accountReadCount, accountReadFailedCount, fieldActualReadCount,
		debug, debugOut, abort)
	return
}

func (tt *TraceTrie) searchOpLane(nStart *SNode,
	env *ExecEnv, accountHistory *RegisterFile, fieldHistory *RegisterFile, registers *RegisterFile,
	totalJumps *uint64, totalJumpKeys *uint64, failedJumps *uint64, oJumps *uint64,
	executedNodes *uint64, executedInputNodes *uint64, executedChainInputNodes *uint64,
	accountReadCount *uint64, accountReadFailedCount *uint64, fieldActualReadCount *uint64,
	debug bool, debugOut func(fmtStr string, params ...interface{}), abort func() bool) (node *SNode, aborted, hit bool) {

	node = nStart
	for {
		if abort != nil && abort() {
			aborted = true
			break
		}
		jHead := node.JumpHead
		beforeJumpNode := node

		if jHead != nil {
			aCheckCount := 0
			fCheckCount := 0
			jNode := jHead
			// try to jump
			node = tt.OpLaneJump(debug, jNode, node, aCheckCount, accountHistory, debugOut,
				fieldHistory, registers, fCheckCount, beforeJumpNode, totalJumps, totalJumpKeys, failedJumps)
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
			if node.IsANode {
				aVId, newAccount := node.GetOrLoadAccountValueID(env, registers, accountHistory)
				if debug {
					cmptypes.MyAssert(newAccount)
				}
				if newAccount {
					if node.Op.isLoadOp {
						*accountReadCount++
						if aVId == 0 {
							*accountReadFailedCount++
						}
					}
				}
			}

			*executedNodes++
			if node.IsRLNode {
				_, fVal = node.LoadFieldValueID(env, registers, fieldHistory)
				*executedInputNodes++
				*fieldActualReadCount++
				if node.Op.isReadOp {
					*executedChainInputNodes++
				}
			} else {
				fVal = node.Execute(env, registers)
			}

			if node.OutputRegisterIndex != 0 {
				registers.AppendRegister(node.OutputRegisterIndex, fVal)
			}

			node = node.GetNextNode(registers)
		} else {
			*oJumps++
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
	debug bool) (func(fmtStr string, params ...interface{}), *os.File, *ExecEnv, *RegisterFile, *RegisterFile, *RegisterFile) {

	var debugOut func(fmtStr string, params ...interface{})
	var fOut *os.File

	env := tt.GetNewExecEnv(db, header, getHashFunc, precompiles, globalCache)
	registers := tt.GetNewRegisters() //NewRegisterFile(tt.RegisterSize) // RegisterFile // := make(RegisterFile, tt.RegisterSize)
	fieldHistory := tt.GetNewHistory()
	accountHistory := tt.GetNewHistory()

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
	env *ExecEnv, registers *RegisterFile, accountHistory *RegisterFile, fieldHistory *RegisterFile,
	totalJumps *uint64, totalJumpKeys *uint64, failedJumps *uint64, fJumps *uint64,
	executedNodes *uint64, executedInputNodes *uint64, executedChainInputNodes *uint64,
	accountReadCount *uint64, accountReadFailedCount *uint64, fieldActualReadCount *uint64,
	debug bool, debugOut func(fmtStr string, params ...interface{}),
	abort func() bool) (fNode *FieldSearchTrieNode, node *SNode, aborted, ok bool) {

	if debug {
		cmptypes.MyAssert(fStart != nil)
	}

	fNode = fStart
	node = nStart

	// Now in the FieldLane
	for {
		if abort != nil && abort() {
			aborted = true
			break
		}
		if debug {
			cmptypes.MyAssert(fNode.LRNode == node)
		}

		beforeNode := node

		//if node != nStart {
		//	*executedNodes++
		//	*executedInputNodes++
		//}

		if fNode.LRNode.Op.isLoadOp {
			aVId, newAccount := fNode.LRNode.GetOrLoadAccountValueID(env, registers, accountHistory)
			if !node.IsANode {
				cmptypes.MyAssert(!newAccount)
			}
			if newAccount {
				*accountReadCount++
			}
			jd := fNode.GetAJumpDef(aVId)
			*totalJumps++
			*totalJumpKeys++
			if jd == nil {
				if newAccount {
					*accountReadFailedCount++
				}
				*failedJumps++
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
				registers.ApplySnapshot(jd.RegisterSnapshot)
				if debug {
					cmptypes.MyAssert(jd.FieldHistorySegment != nil)
					cmptypes.MyAssert(fieldHistory.size == node.FieldIndex, "%v, %v", fieldHistory.size, node.FieldIndex)
				}
				fieldHistory.AppendSegment(fNode.LRNode.FieldIndex, jd.FieldHistorySegment)
				fNode = jd.Dest
				node = fNode.LRNode
				*fJumps++
				if debug {
					cmptypes.MyAssert(registers.size == node.BeforeRegisterSize)
					cmptypes.MyAssert(fieldHistory.size == node.BeforeFieldHistorySize)
				}
			}
		}

		if beforeNode == node {
			if node.Op.isReadOp {
				_, newAccount := node.GetOrLoadAccountValueID(env, registers, accountHistory)
				if fNode != fStart {
					cmptypes.MyAssert(newAccount)
				}
			}
			var fVId uint
			var fVal interface{}
			fVId, fVal = fNode.LRNode.LoadFieldValueID(env, registers, fieldHistory)
			jd := fNode.GetFJumpDef(fVId)
			if !(node == nStart && node.Op.isReadOp) { // if nStart is ReadOp, it is already counted in AccountLane
				*executedNodes++
				*executedInputNodes++
				if node.Op.isReadOp {
					*executedChainInputNodes++
				}
				*fieldActualReadCount++
			}

			*totalJumps++
			*totalJumpKeys++
			if jd == nil {
				*failedJumps++
				if debugOut != nil {
					debugOut("  FieldLane FJump Failed #%v %v on FIndex %v FVId %v\n", node.Seq, node.Statement.SimpleNameString(), node.FieldIndex, fVId)
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
				if debug {
					cmptypes.MyAssert(jd.RegisterSnapshot != nil)
				}
				registers.ApplySnapshot(jd.RegisterSnapshot)
				fNode = jd.Dest
				node = fNode.LRNode
				*fJumps++
				if debug {
					cmptypes.MyAssert(registers.size == node.BeforeRegisterSize)
				}
			}
		}

		if fNode.LRNode.Op.isStoreOp {
			if debugOut != nil {
				debugOut("FieldLane Hit by reaching: #%v %v\n", node.Seq, node.Statement.SimpleNameString())
			}
			ok = true
			break
		}
	}
	return
}

func (tt *TraceTrie) searchAccountLane(aNode *AccountSearchTrieNode,
	env *ExecEnv, registers *RegisterFile, accountHistory *RegisterFile, fieldHistory *RegisterFile,
	totalJumps *uint64, totalJumpKeys *uint64, failedJumps *uint64, aJumps *uint64,
	executedNodes *uint64, executedInputNodes *uint64, executedChainInputNodes *uint64,
	accountReadCount *uint64, accountReadFailedCount *uint64, fieldActualReadCount *uint64,
	debug bool, debugOut func(fmtStr string, params ...interface{}),
	abort func() bool) (node *SNode, fNode *FieldSearchTrieNode, accountLaneRounds []*cache.PreplayResult, aborted bool, ok bool) {
	node = aNode.LRNode
	for {
		if abort != nil && abort() {
			aborted = true
			break
		}
		aVId, newAccount := aNode.LRNode.GetOrLoadAccountValueID(env, registers, accountHistory)
		if newAccount {
			if aNode.LRNode.Op.isReadOp {
				*executedNodes++
				*executedInputNodes++
				*executedChainInputNodes++
				*fieldActualReadCount++ // for reading chain head fields
			} else {
				*accountReadCount++
			}
		}
		if debug {
			cmptypes.MyAssert(newAccount)
		}
		jd := aNode.GetAJumpDef(aVId)
		*totalJumps++
		*totalJumpKeys++
		if jd == nil {
			*failedJumps++
			if newAccount && aNode.LRNode.Op.isLoadOp {
				*accountReadFailedCount++
			}
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
			registers.ApplySnapshot(jd.RegisterSnapshot)
			fieldHistory.ApplySnapshot(jd.FieldHistorySnapshot)
			aNode = jd.Dest
			node = aNode.LRNode
			*aJumps++
			if debug {
				cmptypes.MyAssert(registers.size == aNode.LRNode.BeforeRegisterSize)
				if !aNode.LRNode.Op.isStoreOp {
					cmptypes.MyAssert(fieldHistory.size == aNode.LRNode.FieldIndex)
				}
			}
		}
		if aNode.LRNode.Op.isStoreOp {
			if debugOut != nil {
				debugOut("AccountLane Hit by reaching: #%v %v\n", node.Seq, node.Statement.SimpleNameString())
			}
			accountLaneRounds = aNode.Rounds
			if debug {
				cmptypes.MyAssert(len(accountLaneRounds) > 0)
			}
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

func (tt *TraceTrie) OpLaneJump(debug bool, jNode *OpSearchTrieNode, node *SNode, aCheckCount int,
	accountHistory *RegisterFile, debugOut func(fmtStr string, params ...interface{}),
	fieldHistory *RegisterFile, registers *RegisterFile, fCheckCount int, beforeJumpNode *SNode,
	pTotalJumps, pTotalJumpKeys, pFailedJumps *uint64) *SNode {
	for {
		if debug {
			cmptypes.MyAssert(jNode.OpNode == node)
		}
		if jNode.IsAccountJump {
			aCheckCount += len(jNode.VIndices)

			aKey, aOk := GetOpJumKey(accountHistory, jNode.VIndices)
			*pTotalJumps++
			*pTotalJumpKeys += uint64(len(jNode.VIndices))
			var jd *OpNodeJumpDef
			if aOk {
				jd = jNode.GetMapJumpDef(&aKey)
			}
			if jd == nil {
				*pFailedJumps++
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
			fCheckCount += len(jNode.VIndices)
			fKey, fOk := GetOpJumKey(fieldHistory, jNode.VIndices)
			*pTotalJumps++
			*pTotalJumpKeys += uint64(len(jNode.VIndices))
			var jd *OpNodeJumpDef
			if fOk {
				jd = jNode.GetMapJumpDef(&fKey)
			}
			if jd == nil {
				*pFailedJumps++
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

func (tt *TraceTrie) insertStatement(pNode *SNode, guardKey interface{}, s *Statement, nodeIndex uint, trace *STrace) (*SNode, interface{}) {
	registerMapping := &(trace.RAlloc)
	if pNode == nil {
		if tt.DebugOut != nil {
			tt.DebugOut("NewHead %v\n", s.SimpleNameStringWithRegisterAnnotation(registerMapping))
		}
		return NewSNode(s, nodeIndex, trace, nil, nil), nil
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
			if tt.DebugOut != nil {
				tt.DebugOut("InsertAfter Guard %v on key %v\n", pNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping), guardKey)
			}
			if pNode.GuardedNext == nil {
				pNode.GuardedNext = make(map[interface{}]*SNode)
			}
			pNode.GuardedNext[guardKey] = node
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
		return node, GetGuardKey(s.output.val)
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
	registers               *RegisterFile
	accountHistory          *RegisterFile
	fieldHistroy            *RegisterFile
	nodeIndexToAccountValue map[uint]interface{}
	snodes                  []*SNode
	fnodes                  []*FieldSearchTrieNode
	trace                   *STrace
	nodeIndexToAVId         map[uint]uint
	nodeIndexToFVId         map[uint]uint
	tt                      *TraceTrie
	aNodeFJds               []*FieldNodeJumpDef
	round                   *cache.PreplayResult
	refNodes                []ISRefCountNode
	newNodeCount            uint
}

func NewJumpInserter(tt *TraceTrie, trace *STrace, currentNodes []*SNode, round *cache.PreplayResult) *JumpInserter {

	nodeIndexToAccountValue := GetNodeIndexToAccountSnapMapping(trace, round.ReadDepSeq)
	j := &JumpInserter{
		//registers:               NewRegisterFile(trace.RegisterFileSize),
		//accountHistory:          NewRegisterFile(uint(len(trace.ANodeSet))),
		//fieldHistroy:            NewRegisterFile(uint(len(trace.RLNodeSet))),
		nodeIndexToAccountValue: nodeIndexToAccountValue,
		snodes:                  currentNodes,
		trace:                   trace,
		nodeIndexToAVId:         make(map[uint]uint),
		nodeIndexToFVId:         make(map[uint]uint),
		tt:                      tt,
		round:                   round,
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
			registers.AppendRegister(rid, s.output.val)
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
		if node.IsANode {
			var aval interface{}
			var ok bool
			if node.Op.isLoadOp {
				aval, ok = j.nodeIndexToAccountValue[node.Seq]
				cmptypes.MyAssert(ok)
			} else {
				aval = s.output.val
			}
			aVId = node.GetOrCreateAccountValueID(aval)
			accountHistory.AppendRegister(node.AccountIndex, aVId)
			if tt.DebugOut != nil {
				tt.DebugOut("AccountVID for %v of %v is %v\n", s.SimpleNameStringWithRegisterAnnotation(rmap), aval, aVId)
			}
		} else {
			cmptypes.MyAssert(node.Op.isLoadOp)
			aVId = accountHistory.Get(node.AccountIndex).(uint)
			if tt.DebugOut != nil {
				tt.DebugOut("AccountVID for %v is %v\n", s.SimpleNameStringWithRegisterAnnotation(rmap), aVId)
			}
		}

		fVal := s.output.val
		fVId = node.GetOrCreateFieldValueID(fVal)
		fieldHistory.AppendRegister(node.FieldIndex, fVId)

		if tt.DebugOut != nil {
			tt.DebugOut("FieldVID for %v of %v is %v\n", s.SimpleNameStringWithRegisterAnnotation(rmap), fVal, fVId)
		}

		j.nodeIndexToAVId[nodeIndex] = aVId
		j.nodeIndexToFVId[nodeIndex] = fVId
	}
	j.accountHistory = accountHistory
	j.fieldHistroy = fieldHistory
}

func (j *JumpInserter) SetupFieldJumpLane() {
	tt := j.tt
	snodes := j.snodes
	if tt.DebugOut != nil {
		tt.DebugOut("SetupJumps\n")
	}

	j.registers = NewRegisterFile(j.trace.RegisterFileSize)
	registerMapping := &(j.trace.RAlloc)
	//accountHistory := NewRegisterFile(uint(len(j.trace.ANodeSet)))
	//fieldHistory := NewRegisterFile(uint(len(j.trace.RLNodeIndices)))

	// set up Field Search Track
	fSrc := tt.FieldHead
	if fSrc == nil {
		fSrc = NewFieldSearchTrieNode(snodes[0])
		fSrc.Exit = snodes[1]
		tt.FieldHead = fSrc
	} else {
		cmptypes.MyAssert(fSrc.LRNode == snodes[0])
		cmptypes.MyAssert(fSrc.Exit == snodes[1])
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
	j.fnodes = fNodes
}

func (j *JumpInserter) SetupAccountJumpLane() {
	tt := j.tt
	snodes := j.snodes
	fnodes := j.fnodes

	beforeNewNodeCount := j.newNodeCount

	registerMapping := &(j.trace.RAlloc)
	//accountHistory := NewRegisterFile(uint(len(j.trace.ANodeSet)))
	//fieldHistory := NewRegisterFile(uint(len(j.trace.RLNodeIndices)))

	// set up Account Search Track
	aSrc := tt.AccountHead
	if aSrc == nil {
		aSrc = NewAccountSearchTrieNode(snodes[0])
		aSrc.Exit = fnodes[0]
		tt.AccountHead = aSrc
	} else {
		cmptypes.MyAssert(aSrc.LRNode == snodes[0])
		cmptypes.MyAssert(aSrc.Exit == fnodes[0])
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
			if j.newNodeCount > beforeNewNodeCount {
				aDes.Rounds = append(aDes.Rounds, j.round)
			}
		}
	}
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
			if jumpSrc.JumpHead != nil {
				jd = jumpSrc.JumpHead.Exit
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

func (j *JumpInserter) SetupRoundMapping() {
	node := j.snodes[j.trace.FirstStoreNodeIndex]
	cmptypes.MyAssert(node.Op.isStoreOp)
	node.roundResults.SetupRoundMapping(j.registers, j.accountHistory, j.fieldHistroy, j.round, j.tt.DebugOut)
}

func (tt *TraceTrie) setupJumps(trace *STrace, currentNodes []*SNode, round *cache.PreplayResult) *JumpInserter {
	j := NewJumpInserter(tt, trace, currentNodes, round)
	j.SetupFieldJumpLane()
	j.SetupAccountJumpLane()
	j.SetupOpJumpLane()
	if j.newNodeCount > 0 {
		// do not store repeated round
		j.SetupRoundMapping()
	}
	return j
}
