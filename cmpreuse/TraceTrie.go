package cmpreuse

import (
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/optipreplayer"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
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
}

func NewSTrace(stats []*Statement, debugOut DebugOutFunc) *STrace {
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
		MyAssert(len(ret) == 0)
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
			//MyAssert(dep[i-1] < dep[i], "wrong dep: %v", dep)
			MyAssert(dep[i-1] < dep[i])
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
		MyAssert(len(ret) == 0)
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

		MyAssert(minDep != -1 && minVId != MAX_UINT)

		// append it to ret if not already appended
		lenRet := len(ret)
		if lenRet == 0 {
			ret = append(ret, minVId)
		} else {
			MyAssert(ret[lenRet-1] <= minVId)
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
				MyAssert(i > 0)
				st.FirstStoreNodeIndex = uint(i)
			}
			if !(s.op.IsLog()) && !(s.op.IsVirtual()) {
				addr := s.inputs[0].BAddress()
				addrAccountIndex := addrToAccountId[addr]
				MyAssert(addrAccountIndex > 0)

				storeNodeIndexToStoreAccountIndex[uint(i)] = addrAccountIndex

				if !storeAccountProcessed[addrAccountIndex] {
					storeAccountProcessed[addrAccountIndex] = true
					storeAccountIndices = append(storeAccountIndices, addrAccountIndex)
				}

				//if s.op.config.variant == ACCOUNT_SUICIDE {
				//	continue
				//}

				//MyAssert(len(s.inputs) > 1)

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
					MyAssert(fok)
					fDeps := storeAccountIndexToStoreFieldDependencies[addrAccountIndex]
					fDeps = MergeInputDependencies(fDeps, valFieldDependencies)
					storeAccountIndexToStoreFieldDependencies[addrAccountIndex] = fDeps

					valAccountDependencies, aok := variableInputAccountDependencies[valueVarID]
					MyAssert(aok)
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
		MyAssert(!ok)
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
						MyAssert(rid != 0)
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
				MyAssert(len(rLNodeIndices) >= 2)
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
			MyAssert(ok)

			inputFieldDependency := make(InputDependence, 0)
			inputAccountDependency := make(InputDependence, 0)
			for _, v := range s.inputs {
				if v.IsConst() {
					continue
				}
				inputFieldDependency = MergeInputDependencies(variableInputFieldDependencies[v.id], inputFieldDependency)
				MyAssert(len(inputFieldDependency) > 0)
				inputAccountDependency = MergeInputDependencies(variableInputAccountDependencies[v.id], inputAccountDependency)
				MyAssert(len(inputAccountDependency) > 0)
			}
			//if s.op == OP_LoadState && !s.inputs[1].IsConst() {
			//	inputFieldDependency = variableInputFieldDependencies[s.inputs[1].id]
			//	MyAssert(len(inputFieldDependency) > 0)
			//	inputAccountDependency = variableInputAccountDependencies[s.inputs[1].id]
			//	MyAssert(len(inputAccountDependency) > 0)
			//}
			fDeps := MergeInputDependencies(inputFieldDependency, InputDependence{fieldIndex})
			variableInputFieldDependencies[vid] = fDeps // load/read output depend on itself
			sNodeInputFieldDependencies[nodeIndex] = fDeps

			aDeps := MergeInputDependencies(inputAccountDependency, InputDependence{aIndex})
			variableInputAccountDependencies[vid] = aDeps    //MergeInputDependencies(inputAccountDependency, InputDependence{aIndex})
			sNodeInputAccountDependencies[nodeIndex] = aDeps //inputAccountDependency

		} else {
			inputFieldDeps := make([]InputDependence, 0, 2)
			inputAccountDeps := make([]InputDependence, 0, 2)
			for _, v := range s.inputs {
				if !v.IsConst() {
					dep, ok := variableInputFieldDependencies[v.id]
					MyAssert(ok)
					inputFieldDeps = append(inputFieldDeps, dep)
					dep, ok = variableInputAccountDependencies[v.id]
					MyAssert(ok)
					inputAccountDeps = append(inputAccountDeps, dep)
				}
			}
			MyAssert(len(inputFieldDeps) > 0)
			outputFieldDep := MergeInputDependencies(inputFieldDeps...)
			outputAccountDep := MergeInputDependencies(inputAccountDeps...)
			if !s.op.IsGuard() {
				MyAssert(!s.output.IsConst())
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
			MyAssert(nodeIndex > 0)
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

type AccountNodeJumpDef struct {
	Dest                 *AccountSearchTrieNode
	RegisterSnapshot     *RegisterFile
	FieldHistorySnapshot *RegisterFile
	InJumpNode           *AccountSearchTrieNode
	InJumpAVId           uint
	SRefCount
}

func (jd *AccountNodeJumpDef) RemoveRef(roundID uint64) {
	newRefCount := jd.removeRef()
	if newRefCount == 0 {
		if jd.InJumpNode != nil && jd.InJumpNode.GetRefCount() != 0 {
			MyAssert(jd.InJumpAVId != 0)
			MyAssert(jd.InJumpNode.JumpTable[jd.InJumpAVId] == jd)
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
	n.removeRef()
	if n.Rounds != nil {
		_removeRound(n.Rounds, roundID)
	}
}

func (n *AccountSearchTrieNode) getOrCreateJumpDef(jt []*AccountNodeJumpDef, vid uint) ([]*AccountNodeJumpDef, *AccountNodeJumpDef) {
	MyAssert(vid > 0)
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
	newRefCount := jd.removeRef()
	if newRefCount == 0 {
		if jd.InJumpNode != nil && jd.InJumpNode.GetRefCount() != 0 {
			MyAssert(jd.InJumpVId != 0)
			jt := jd.InJumpNode.AJumpTable
			if !jd.IsAJump {
				jt = jd.InJumpNode.FJumpTable
			}
			MyAssert(jt[jd.InJumpVId] != nil)
			MyAssert(jt[jd.InJumpVId] == jd)
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
	n.removeRef()
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
	MyAssert(vid > 0)
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
	newRefCount := jd.removeRef()
	if newRefCount == 0 {
		if jd.InJumpNode != nil && jd.InJumpNode.GetRefCount() != 0 {
			if len(jd.InJumpNode.VIndices) == 1 {
				if DEBUG_TRACER {
					MyAssert(jd.InJumpKey == EMPTY_JUMP_KEY)
					MyAssert(len(jd.InJumpNode.JumpMap) == 0)
					MyAssert(jd.InJumpVId != 0)
					MyAssert(jd.InJumpNode.JumpTable[jd.InJumpVId] == jd)
				}
				jd.InJumpNode.JumpTable[jd.InJumpVId] = nil
			} else {
				if DEBUG_TRACER {
					MyAssert(jd.InJumpKey != EMPTY_JUMP_KEY)
					MyAssert(jd.InJumpNode.JumpMap[jd.InJumpKey] == jd)
					MyAssert(jd.InJumpVId == 0)
					MyAssert(jd.InJumpNode.JumpTable == nil)
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
	n.removeRef()
}

func (f *OpSearchTrieNode) getOrCreateMapJumpDef(key OpJumpKey) (*OpNodeJumpDef, bool) {
	if len(f.VIndices) == 1 {
		vid := key[0]
		if DEBUG_TRACER {
			MyAssert(vid > 0)
			for i:= 1; i < OP_JUMP_KEY_SIZE; i++ {
				MyAssert(key[i]==0)
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
	//MyAssert(len(rf) == len(other))
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

func (rf *RegisterFile) Get(index uint) interface{} {
	if index < rf.firstSectionSize {
		if DEBUG_TRACER {
			MyAssert(rf.firstSectionSize == uint(len(rf.firstSection)))
			MyAssert(rf.size == rf.firstSectionSize+uint(len(rf.secondSection)))
		}
		return rf.firstSection[index]
	} else {
		if DEBUG_TRACER {
			MyAssert(rf.isSnapshot == false)
			MyAssert(rf.firstSectionSize == uint(len(rf.firstSection)))
			MyAssert(rf.size == rf.firstSectionSize+uint(len(rf.secondSection)))
		}
		return rf.secondSection[index-rf.firstSectionSize]
	}
}

func (rf *RegisterFile) AppendRegister(index uint, val interface{}) {
	if !rf.firstSectionSealed {
		if DEBUG_TRACER {
			MyAssert(index == rf.size)
			MyAssert(rf.isSnapshot == false)
			MyAssert(rf.firstSectionSize == uint(len(rf.firstSection)))
			MyAssert(rf.size == rf.firstSectionSize+uint(len(rf.secondSection)))
		}
		rf.firstSection = append(rf.firstSection, val)
		rf.firstSectionSize++
		rf.size++
	} else {
		if DEBUG_TRACER {
			MyAssert(index == rf.size)
			MyAssert(rf.isSnapshot == false)
			MyAssert(rf.firstSectionSize == uint(len(rf.firstSection)))
			MyAssert(rf.size == rf.firstSectionSize+uint(len(rf.secondSection)))
		}
		rf.secondSection = append(rf.secondSection, val)
		rf.size++
	}
}

func (rf *RegisterFile) ApplySnapshot(snapshot *RegisterFile) {
	if DEBUG_TRACER {
		MyAssert(snapshot.firstSectionSealed && snapshot.size == snapshot.firstSectionSize)
		MyAssert(snapshot.isSnapshot == true)
		MyAssert(snapshot.secondSection == nil)
		MyAssert(rf.size == rf.firstSectionSize)
		MyAssert(rf.isSnapshot == false)
		MyAssert(rf.firstSectionSize == uint(len(rf.firstSection)))
		MyAssert(rf.size == rf.firstSectionSize+uint(len(rf.secondSection)))
	}
	rf.firstSection = snapshot.firstSection
	rf.firstSectionSize = snapshot.firstSectionSize
	rf.firstSectionSealed = snapshot.firstSectionSealed
	rf.size = snapshot.size
}

func (rf *RegisterFile) AppendSegment(index uint, seg RegisterSegment) {
	if DEBUG_TRACER {
		MyAssert(rf.size == index)
		MyAssert(rf.isSnapshot == false)
		MyAssert(rf.firstSectionSize == uint(len(rf.firstSection)))
		MyAssert(rf.size == rf.firstSectionSize+uint(len(rf.secondSection)))
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
		MyAssert(!rf.firstSectionSealed && rf.size == rf.firstSectionSize)
		MyAssert(si < rf.firstSectionSize)
		MyAssert(di <= rf.firstSectionSize)
		MyAssert(rf.isSnapshot == false)
		MyAssert(rf.firstSectionSize == uint(len(rf.firstSection)))
		MyAssert(rf.size == rf.firstSectionSize+uint(len(rf.secondSection)))
	}
	return rf.firstSection[si:di]
}

func (rf *RegisterFile) AssertSnapshotEqual(other *RegisterFile) {
	MyAssert(rf.size == other.size)
	MyAssert(rf.firstSectionSealed && other.firstSectionSealed)
	MyAssert(rf.isSnapshot)
	MyAssert(other.isSnapshot)

	for i := uint(1); i < rf.size; i++ {
		AssertEqualRegisterValue(rf.Get(i), other.Get(i))
	}
}

func AssertEqualRegisterValue(a, b interface{}) {
	switch ta := a.(type) {
	case *big.Int:
		tb := b.(*big.Int)
		MyAssert(ta.Cmp(tb) == 0, "%v : %v", a, b)
		return
	case []byte:
		tb := b.([]byte)
		MyAssert(string(ta) == string(tb), "%v : %v", a, b)
		return
	case *StateIDM:
		tb := b.(*StateIDM)
		MyAssert(len(ta.mapping) == len(tb.mapping), "%v : %v", a, b)
		for k, v := range ta.mapping {
			MyAssert(tb.mapping[k] == v, "%v : %v", a, b)
		}
		return
	case *AddrIDM:
		tb := b.(*AddrIDM)
		MyAssert(len(ta.mapping) == len(tb.mapping), "%v : %v", a, b)
		for k, v := range ta.mapping {
			MyAssert(tb.mapping[k] == v, "%v : %v", a, b)
		}
		return
	case *BlockHashNumIDM:
		tb := b.(*BlockHashNumIDM)
		MyAssert(len(ta.mapping) == len(tb.mapping), "%v : %v", a, b)
		for k, v := range ta.mapping {
			MyAssert(tb.mapping[k] == v, "%v : %v", a, b)
		}
		return
	}
	MyAssert(a == b, "%v : %v", a, b)
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
	MyAssert(reflect.TypeOf(uint(0)).Size() == 8)
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
			buf.Write(tv.Bytes())
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
		MyAssert(r.RoundID != round.RoundID)
		MyAssert(r.Receipt.GasUsed == round.Receipt.GasUsed)
		MyAssert(r.Receipt.Status == round.Receipt.Status)
		MyAssert(len(r.Receipt.Logs) == len(round.Receipt.Logs))
	}
	rr.rounds = append(rr.rounds, round)
}

func AssertAAMapEqual(a, b map[uint]interface{}) {
	MyAssert(len(a) == len(b))
	for k, va := range a {
		vb, ok := b[k]
		MyAssert(ok)
		MyAssert(vb == va)
	}
}

func (rr *StoreInfo) SetupRoundMapping(registers *RegisterFile, accounts *RegisterFile, fields *RegisterFile, round *cache.PreplayResult, debugOut DebugOutFunc) {
	for _, aIndex := range rr.StoreAccountIndices {
		aVId := accounts.Get(aIndex).(uint)
		MyAssert(aVId != 0)
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
			addr = common.BigToAddress(registers.Get(a_rid).(*big.Int))
		default:
			panic(fmt.Sprintf("Unknow type %v", reflect.TypeOf(addrOrRId).Name()))
		}

		if true {
			accountDeps := rr.StoreAccountIndexToStoreAccountDependencies[aIndex]
			adk, aok, avids := GetDepsKey(accountDeps, accounts)
			MyAssert(aok)
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
			MyAssert(fok)
			frounds := rm.FieldDepsToRounds[afk]
			frounds = append(frounds, round)
			rm.FieldDepsToRounds[afk] = frounds

			//if bfk, bfok := rm.AccountDepsToFieldDeps[adk]; bfok {
			//	MyAssert(bfk == afk)
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
			//	MyAssert(bvk == vk)
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
	MyAssert(len(a) == len(b))
	for k, va := range a {
		vb, ok := b[k]
		MyAssert(ok)
		MyAssert(va == vb)
	}
}

func AssertADMapEqual(a map[uint][]uint, b map[uint][]uint) {
	MyAssert(len(a) == len(b))
	for k, va := range a {
		vb, ok := b[k]
		MyAssert(ok)
		AssertUArrayEqual(va, vb)
	}
}

func AssertUArrayEqual(indices []uint, indices2 []uint) {
	MyAssert(len(indices) == len(indices2))
	for i, v := range indices {
		MyAssert(v == indices2[i])
	}
}

type ISRefCountNode interface {
	GetRefCount() uint
	AddRef() uint             // add a ref and return the new ref count
	RemoveRef(roundID uint64) // remove a ref
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

func (rf *SRefCount) removeRef() uint {
	MyAssert(rf.refCount > 0)
	rf.refCount--
	return rf.refCount
}

type SNode struct {
	Op                                     *OpDef
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
		MyAssert(n.AccountIndex != 0)
		n.BeforeAccounHistorySize = n.AccountIndex
	} else {
		if n.Prev.IsANode {
			n.BeforeAccounHistorySize = n.Prev.BeforeAccounHistorySize + 1
		} else {
			n.BeforeAccounHistorySize = n.Prev.BeforeAccounHistorySize
		}
	}

	if n.IsRLNode {
		MyAssert(n.FieldIndex != 0)
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
		MyAssert(n.AccountIndex != 0)
		for _, fIndex := range n.FieldDependencies {
			nIndex := st.FieldIndexToRLNodeIndex[fIndex]
			aIndex := st.RLNodeIndexToAccountIndex[nIndex]
			MyAssert(aIndex != 0)
			if aIndex == n.AccountIndex {
				selfFDs = append(selfFDs, fIndex)
			}
		}
		n.FieldDependenciesExcludingSelfAccount = n.FieldDependencies.SetDiff(selfFDs)
		n.AccountDepedenciesExcludingSelfAccount = n.AccountDependencies.SetDiff([]uint{n.AccountIndex})
		if len(n.FieldDependenciesExcludingSelfAccount) == 0 {
			MyAssert(len(n.AccountDepedenciesExcludingSelfAccount) == 0)
		}
		if len(n.AccountDepedenciesExcludingSelfAccount) == 0 {
			MyAssert(len(n.FieldDependenciesExcludingSelfAccount) == 0)
		}
	}

	n.IsJSPNode = st.JSPSet[nodeIndex]

	for i, v := range s.inputs {
		if v.IsConst() {
			n.InputVals[i] = v.val
			//n.InputRegisterIndices[i] = 0
		} else {
			rid, ok := registerMapping[v.id]
			MyAssert(ok)
			n.InputRegisterIndices[i] = rid
		}
	}

	if s.output != nil && !s.output.IsConst() {
		rid, ok := registerMapping[s.output.id]
		MyAssert(ok)
		n.OutputRegisterIndex = rid
		n.BeforeRegisterSize = rid
	} else {
		MyAssert(n.Prev != nil)
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

func (n *SNode) RemoveRef(roundID uint64) {
	newRefCount := n.removeRef()
	if newRefCount == 0 {
		if n.Prev != nil && n.Prev.GetRefCount() != 0 {
			MyAssert(n.PrevGuardKey != nil)
			_, ok := n.Prev.GuardedNext[n.PrevGuardKey]
			MyAssert(ok)
			delete(n.Prev.GuardedNext, n.PrevGuardKey)
		}
	}
	if n.roundResults != nil {
		n.roundResults.ClearRound(roundID)
	}
}

func (n *SNode) AddStoreInfo(round *cache.PreplayResult, trace *STrace) {
	MyAssert(n.Op.isStoreOp)
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
	return n.Op.impFuc(env)
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
		inputs[i] = n.Statement.TypeConvert(n.Statement.inputs[i], inputs[i])
	}
	var output interface{}
	if n.Statement.output != nil {
		if n.OutputRegisterIndex != 0 {
			output = registers.Get(n.OutputRegisterIndex)
		} else {
			output = n.Statement.output.val
		}
		output = n.Statement.TypeConvert(n.Statement.output, output)
	}

	return n.Statement.ValueString(inputs, output)
}

func (n *SNode) GetOrLoadAccountValueID(env *ExecEnv, registers *RegisterFile, accountHistory *RegisterFile) (uint, bool) {
	MyAssert(n.Op.isLoadOp || n.Op.isReadOp)
	//MyAssert(n.IsANode)
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
			//addr = common.BigToAddress(n.InputVals[0].(*big.Int))
			addr = env.BigToAddress(n.InputVals[0].(*big.Int))
		} else {
			//addr = common.BigToAddress(registers.Get(n.InputRegisterIndices[0]).(*big.Int))
			addr = env.BigToAddress(registers.Get(n.InputRegisterIndices[0]).(*big.Int))
		}
		changedBy := env.state.GetTxDepByAccountNoCopy(addr)
		value := changedBy.Hash()
		k := GetGuardKey(value)
		vid = n.AccountValueToID[k]
	}

	if DEBUG_TRACER {
		MyAssert(accountHistory.firstSectionSealed == false)
	}
	accountHistory.AppendRegister(n.AccountIndex, vid)
	return vid, true
}

func (n *SNode) LoadFieldValueID(env *ExecEnv, registers *RegisterFile, fieldHistory *RegisterFile) (uint, interface{}) {
	MyAssert(n.Op.isLoadOp || n.Op.isReadOp)

	val := n.Execute(env, registers)
	k := GetGuardKey(val)
	vid := n.FieldValueToID[k]
	fieldHistory.AppendRegister(n.FieldIndex, vid)
	return vid, val
}

func (n *SNode) GetOrCreateAccountValueID(val interface{}) uint {
	MyAssert(n.IsANode)
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
	MyAssert(n.Op.isLoadOp || n.Op.isReadOp)
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
	MyAssert(n.Op == s.op, params...)
	MyAssert(len(n.InputVals) == len(s.inputs), params...)
	MyAssert(n.Seq == nodeIndex, params...)
	if n.Op.IsGuard() {
		MyAssert(n.Statement.inputs[0].varType == s.inputs[0].varType, params...)
	} else {
		for i, v := range s.inputs {
			if v.IsConst() {
				MyAssert(n.InputRegisterIndices[i] == 0, params...)
				AssertEqualRegisterValue(n.InputVals[i], v.val)
			} else {
				registerMapping := trace.RAlloc
				rid, ok := registerMapping[v.id]
				MyAssert(ok, params...)
				MyAssert(n.InputRegisterIndices[i] == rid, params...)
			}
		}
	}
	MyAssert(n.IsANode == trace.ANodeSet[nodeIndex], params...)
	MyAssert(n.IsTLNode == trace.TLNodeSet[nodeIndex], params...)
	MyAssert(n.AccountIndex == trace.RLNodeIndexToAccountIndex[nodeIndex], params...)
	MyAssert(n.FieldIndex == trace.RLNodeIndexToFieldIndex[nodeIndex], params...)
	MyAssert(n.FieldDependencies.Equals(trace.SNodeInputFieldDependencies[nodeIndex]), params...)
	MyAssert(n.AccountDependencies.Equals(trace.SNodeInputAccountDependencies[nodeIndex]), params...)
	MyAssert(n.IsJSPNode == trace.JSPSet[nodeIndex], params...)
	if s.output == nil || s.output.IsConst() {
		MyAssert(n.OutputRegisterIndex == 0)
		MyAssert(n.Prev != nil)
		if n.Prev.OutputRegisterIndex == 0 {
			MyAssert(n.BeforeRegisterSize == n.Prev.BeforeRegisterSize)
		} else {
			MyAssert(n.BeforeRegisterSize == n.Prev.BeforeRegisterSize+1)
		}
	} else {
		MyAssert(n.OutputRegisterIndex != 0)
		MyAssert(n.OutputRegisterIndex == trace.RAlloc[s.output.id])
		MyAssert(n.OutputRegisterIndex == n.BeforeRegisterSize)
	}

	if n.IsRLNode && !n.IsANode {
		MyAssert(n.FieldDependenciesExcludingSelfAccount != nil)
		MyAssert(n.AccountDepedenciesExcludingSelfAccount != nil)
		selfFDs := make(InputDependence, 0, len(n.FieldDependencies))
		MyAssert(n.AccountIndex != 0)
		for _, fIndex := range n.FieldDependencies {
			nIndex := trace.FieldIndexToRLNodeIndex[fIndex]
			aIndex := trace.RLNodeIndexToAccountIndex[nIndex]
			MyAssert(aIndex != 0)
			if aIndex == n.AccountIndex {
				selfFDs = append(selfFDs, fIndex)
			}
		}
		AssertUArrayEqual(n.FieldDependenciesExcludingSelfAccount, n.FieldDependencies.SetDiff(selfFDs))
		AssertUArrayEqual(n.AccountDepedenciesExcludingSelfAccount, n.AccountDependencies.SetDiff([]uint{n.AccountIndex}))
	} else {
		MyAssert(n.FieldDependenciesExcludingSelfAccount == nil)
		MyAssert(n.AccountDepedenciesExcludingSelfAccount == nil)
	}
}

func ImmutableBytesToString(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
	//return string(buf)
}

func GetBigIntGuardKey(bi *big.Int) interface{} {
	words := bi.Bits()
	if len(words) <= 8 {
		var key [8]big.Word
		copy(key[:], words)
		return key
	}else {
		return ImmutableBytesToString(bi.Bytes())
	}
}

func GetGuardKey(a interface{}) interface{} {
	switch ta := a.(type) {
	case *big.Int:
		//return ImmutableBytesToString(ta.Bytes())
		return GetBigIntGuardKey(ta)
	case []byte:
		return ImmutableBytesToString(ta)
	case uint64:
		return ta
	case bool:
		return ta
	case cmptypes.AccountSnap:
		return ta
	case common.Hash:
		return ta
	case uint32:
		return ta
	case int:
		return ta
	default:
		panic(fmt.Sprintf("Unknown type: %v", reflect.TypeOf(a).Name()))
	}
}

func (n *SNode) GetNextNode(registers *RegisterFile) *SNode {
	if n.Op.IsGuard() {
		k := registers.Get(n.InputRegisterIndices[0])
		if DEBUG_TRACER {
			MyAssert(k != nil)
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
		MyAssert(node.BeforeNextGuardNewRegisterSize == -1)
		MyAssert(registerCount >= 0)
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

type TraceTrieRoundRefNodes struct {
	RefNodes []ISRefCountNode
	RoundID  uint64
	Next     *TraceTrieRoundRefNodes
}

type TraceTrie struct {
	Head                  *SNode
	Tx                    *types.Transaction
	PathCount             uint
	RegisterSize          uint
	DebugOut              DebugOutFunc
	AccountHead           *AccountSearchTrieNode
	FieldHead             *FieldSearchTrieNode
	RoundRefNodesHead     *TraceTrieRoundRefNodes
	RoundRefNodesTail     *TraceTrieRoundRefNodes
	RoundRefCount         uint
	TrieNodeCount         int64
	PreAllocatedExecEnvs  []*ExecEnv
	PreAllocatedHistory   []*RegisterFile
	PreAllocatedRegisters []*RegisterFile
}

func NewTraceTrie(tx *types.Transaction) *TraceTrie {
	tt := &TraceTrie{
		Head:         nil,
		Tx:           tx,
		PathCount:    0,
		RegisterSize: 0,
	}
	tt.PreAllocatedExecEnvs = []*ExecEnv{
		NewExecEnvWithCache(),
		NewExecEnvWithCache(),
	}
	for i := 0; i < 4; i++ {
		tt.PreAllocatedHistory = append(tt.PreAllocatedHistory, NewRegisterFile(1000))
	}
	return tt
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
		env = NewExecEnvWithCache()
	}
	env.state = db
	env.header = header
	env.precompiles = precompiles
	env.isProcess = true
	env.getHash = getHashFunc
	env.globalCache = globalCache
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
		tt.PreAllocatedRegisters = []*RegisterFile{
			NewRegisterFile(size),
			NewRegisterFile(size),
		}
	}
}

func MyAssert(b bool, params ...interface{}) {
	if !b {
		msg := ""
		if len(params) > 0 {
			msg = fmt.Sprintf(params[0].(string), params[1:]...)
		}
		panic(msg)
	}
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
			//case cmptypes.Blockhash:
			//	key = *BLOCK_HASH + alv.AddLoc.Field.String()
			//case cmptypes.Number:
			//	key = *BLOCK_NUMBER
			//case cmptypes.Timestamp:
			//	key = *BLOCK_TIMESTAMP
			//case cmptypes.GasLimit:
			//	key = *BLOCK_GASLIMIT
			//case cmptypes.Coinbase:
			//	key = *BLOCK_COINBASE
			//case cmptypes.Difficulty:
			//	key = *BLOCK_DIFFICULTY
			//default:
			//	panic(fmt.Sprintf("Unexpected %v", alv.AddLoc.Field))
		}
		//temp[key] = val
	}

	for _, nodeIndex := range trace.ANodeIndices {
		s := trace.Stats[nodeIndex]
		var key string
		if s.op.isLoadOp {
			key = s.inputs[0].BAddress().Hex()
			MyAssert(temp[key] != nil, "%v : %v", s.SimpleNameString(), key)
			ret[nodeIndex] = temp[key]
		}
		//} else {
		//	key = *(s.op.config.variant)
		//	if s.op.config.variant == BLOCK_HASH {
		//		key += s.inputs[0].BigInt().String()
		//	}
		//}
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
		MyAssert(n.Seq == uint(i))
		//n.Seq = uint(i)
		currentTraceNodes[i] = n
		refNodes[i] = n
		if n.GetRefCount() == 0 {
			newNodeCount++
		}
	}

	MyAssert(tt.Head == nil || tt.Head == preHead.Next)
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
				round.RoundID, newNodeCount, len(refNodes), tt.RoundRefCount, tt.TrieNodeCount)
		}
		for _, n := range refNodes {
			rc := n.AddRef()
			MyAssert(rc > 0)
		}

		r := &TraceTrieRoundRefNodes{
			RefNodes: refNodes,
			RoundID:  round.RoundID,
		}

		tt.RoundRefCount++
		atomic.AddInt64(&tt.TrieNodeCount, int64(newNodeCount))
		if tt.RoundRefNodesHead == nil {
			MyAssert(tt.RoundRefNodesTail == nil)
			MyAssert(uint(len(refNodes)) == newNodeCount)
			tt.RoundRefNodesHead = r
			tt.RoundRefNodesTail = r
		} else {
			MyAssert(tt.RoundRefNodesTail.Next == nil)
			tt.RoundRefNodesTail.Next = r
			tt.RoundRefNodesTail = r
		}
		tt.GCRoundRefNodes()
	} else {
		if tt.DebugOut != nil {
			tt.DebugOut("RedundantRound %v: No New TrieNodes Created, %v existing nodes\n", round.RoundID, len(refNodes))
		}
	}
}

var gcMutex sync.Mutex

func (tt *TraceTrie) GCRoundRefNodes() {
	MyAssert(tt.RoundRefNodesHead != nil && tt.RoundRefNodesTail != nil)
	if tt.RoundRefCount > uint(optipreplayer.TXN_PREPLAY_ROUND_LIMIT*2) {
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
		for i := 0; i < 1; i++ {
			head := tt.RoundRefNodesHead
			removed := tt.RemoveRoundRef(head)
			if tt.DebugOut != nil {
				tt.DebugOut("RemovedRound %v: %v trie nodes removed, %v trie nodes remain, %v rounds remain\n",
					head.RoundID, removed, tt.TrieNodeCount, tt.RoundRefCount)
				if removed > 100000 {
					log.Info(fmt.Sprintf("RemovedRound %v for tx %v: %v trie nodes removed, %v trie nodes remain, %v rounds remain",
						head.RoundID, tt.Tx.Hash().Hex(), removed, tt.TrieNodeCount, tt.RoundRefCount))
				}
			}
			tt.RoundRefNodesHead = head.Next
		}
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

func (tt *TraceTrie) RemoveRoundRef(rf *TraceTrieRoundRefNodes) uint {
	removedNodes := int64(0)
	for _, n := range rf.RefNodes {
		n.RemoveRef(rf.RoundID)
		if n.GetRefCount() == 0 {
			removedNodes++
		}
	}
	tt.RoundRefCount--
	atomic.AddInt64(&tt.TrieNodeCount, -removedNodes)
	MyAssert(tt.RoundRefCount >= 0)
	MyAssert(tt.TrieNodeCount >= 0)
	return uint(removedNodes)
}

type TraceTrieSearchResult struct {
	node                         *SNode
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
}

func (r *TraceTrieSearchResult) GetAnyRound() *cache.PreplayResult {
	MyAssert(r.hit)
	MyAssert(r.node.Op.isStoreOp)
	for _, round := range r.node.roundResults.rounds {
		if round != nil {
			return round
		}
	}
	MyAssert(false) // should never reach here
	return nil
}

func (r *TraceTrieSearchResult) ApplyStores(abort func() bool) bool {
	MyAssert(r.node.Op.isStoreOp)
	r.applyWObjects()
	for n := r.node; !n.Op.IsVirtual(); n = n.Next {
		if abort() {
			return true
		}
		if !n.Op.IsLog() {
			aIndex := r.storeInfo.StoreNodeIndexToAccountIndex[n.Seq]
			MyAssert(aIndex > 0)
			if r.accountIndexToAppliedWObject[aIndex] {
				if r.debugOut != nil {
					r.debugOut("SkipStore: %v", n.SimpleNameString())
					r.debugOut("    %v", n.RegisterValueString(r.registers))
				}
				continue
			}
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
	return false
}

func (r *TraceTrieSearchResult) applyWObjects() {
	MyAssert(r.accountIndexToAppliedWObject == nil)
	r.accountIndexToAppliedWObject = make(map[uint]bool)

	accountLaneHit := r.accountLaneRounds != nil
	rounds := r.accountLaneRounds

	for _, si := range r.storeInfo.StoreAccountIndices {
		aVId := r.accountHistory.Get(si).(uint)
		if aVId != 0 {
			if !accountLaneHit {
				ar := r.storeInfo.StoreAccountIndexToAVIdToRoundMapping[si]
				rm := ar[aVId]
				if rm == nil {
					continue
				}
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
			}

			if rounds == nil {
				continue
			}

			var addr common.Address
			addrOrRId := r.storeInfo.AccountIndexToAddressOrRId[si]
			switch a_rid := addrOrRId.(type) {
			case common.Address:
				addr = a_rid
			case uint:
				addr = common.BigToAddress(r.registers.Get(a_rid).(*big.Int))
			default:
				panic(fmt.Sprintf("Unknow type %v", reflect.TypeOf(addrOrRId).Name()))
			}

			for _, round := range rounds {
				if round != nil {
					obj := round.WObjects[addr]
					if obj != nil {
						if r.debugOut != nil {
							r.debugOut("Apply WObject from Round %v for Account %v AVID %v with Addr %v\n", round.RoundID, si, aVId, addr.Hex())
						}
						round.WObjects[addr] = nil
						r.env.state.ApplyStateObject(obj)
						r.accountIndexToAppliedWObject[si] = true
						break
					}
				}
			}

		}
	}
}

func (tt *TraceTrie) SearchTraceTrie(db *state.StateDB, header *types.Header, getHashFunc vm.GetHashFunc,
	precompiles map[common.Address]vm.PrecompiledContract, abort func() bool, debug bool, globalCache *cache.GlobalCache) (result *TraceTrieSearchResult, isAbort bool, ok bool) {
	var node *SNode

	globalCache.BigIntPoolMutex.Lock()
	defer globalCache.BigIntPoolMutex.Unlock()
	env := tt.GetNewExecEnv(db, header, getHashFunc, precompiles, globalCache)

	registers := tt.GetNewRegisters() //NewRegisterFile(tt.RegisterSize) // RegisterFile // := make(RegisterFile, tt.RegisterSize)
	fieldHistory := tt.GetNewHistory()
	accountHistory := tt.GetNewHistory()

	var debugOut func(fmtStr string, params ...interface{})
	var fOut *os.File

	if debug {
		fOut, _ := os.OpenFile("/tmp/SearchTrieLog.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)

		debugOut = func(fmtStr string, params ...interface{}) {
			if len(fmtStr) > 0 && fmtStr[len(fmtStr)-1] != '\n' {
				fmtStr += "\n"
			}
			s := fmt.Sprintf(fmtStr, params...)
			fOut.WriteString(s)
		}
	}

	var accountLaneRounds []*cache.PreplayResult
	var aborted bool

	defer func() {
		result = &TraceTrieSearchResult{
			node:              node,
			hit:               ok,
			registers:         registers,
			fieldHistory:      fieldHistory,
			accountHistory:    accountHistory,
			env:               env,
			debugOut:          debugOut,
			fOut:              fOut,
			storeInfo:         node.roundResults,
			accountLaneRounds: accountLaneRounds,
			aborted:           aborted,
		}
	}()

	if debugOut != nil {
		debugOut("Start Search Trie for Tx %v\n", tt.Tx.Hash().Hex())
	}

	ok = false
	// start with the AccountLane
	aNode := tt.AccountHead
	node = aNode.LRNode
	var fNode *FieldSearchTrieNode

	for {
		if abort() {
			aborted = true
			return
		}
		aVId, newAccount := aNode.LRNode.GetOrLoadAccountValueID(env, registers, accountHistory)
		if debug {
			MyAssert(newAccount)
		}
		jd := aNode.GetAJumpDef(aVId)
		if jd == nil {
			if debugOut != nil {
				debugOut("  AccountLane Failed #%v %v on AIndex %v AVId %v\n", node.Seq, node.Statement.SimpleNameString(), node.AccountIndex, aVId)
			}
			if debug {
				MyAssert(aNode.Exit != nil)
			}
			fNode = aNode.Exit
			if debug {
				MyAssert(aNode.LRNode == fNode.LRNode)
			}
			break
		} else {
			if debugOut != nil {
				debugOut("AccountLane Jump from (#%v to #%v) %v to %v on AIndex %v AVId %v\n",
					node.Seq, jd.Dest.LRNode.Seq, node.Statement.SimpleNameString(), jd.Dest.LRNode.Statement.SimpleNameString(), node.AccountIndex, aVId)
			}
			if debug {
				MyAssert(jd.RegisterSnapshot != nil)
				MyAssert(jd.FieldHistorySnapshot != nil)
			}
			registers.ApplySnapshot(jd.RegisterSnapshot)
			fieldHistory.ApplySnapshot(jd.FieldHistorySnapshot)
			aNode = jd.Dest
			node = aNode.LRNode
			if debug {
				MyAssert(registers.size == aNode.LRNode.BeforeRegisterSize)
				if !aNode.LRNode.Op.isStoreOp {
					MyAssert(fieldHistory.size == aNode.LRNode.FieldIndex)
				}
			}
		}
		if aNode.LRNode.Op.isStoreOp {
			if debugOut != nil {
				debugOut("AccountLane Hit by reaching: #%v %v\n", node.Seq, node.Statement.SimpleNameString())
			}
			accountLaneRounds = aNode.Rounds
			if debug {
				MyAssert(len(accountLaneRounds) > 0)
			}
			ok = true
			return
		}
	}

	if debug {
		MyAssert(fNode != nil)
	}
	fStart := fNode

	// Now in the FieldLane
	for {
		if abort() {
			aborted = true
			return
		}
		if debug {
			MyAssert(fNode.LRNode == node)
		}

		beforeNode := node
		if fNode.LRNode.Op.isLoadOp {
			aVId, newAccount := fNode.LRNode.GetOrLoadAccountValueID(env, registers, accountHistory)
			if !node.IsANode {
				MyAssert(!newAccount)
			}
			jd := fNode.GetAJumpDef(aVId)
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
					MyAssert(jd.RegisterSnapshot != nil)
				}
				registers.ApplySnapshot(jd.RegisterSnapshot)
				if debug {
					MyAssert(jd.FieldHistorySegment != nil)
					MyAssert(fieldHistory.size == node.FieldIndex, "%v, %v", fieldHistory.size, node.FieldIndex)
				}
				fieldHistory.AppendSegment(fNode.LRNode.FieldIndex, jd.FieldHistorySegment)
				fNode = jd.Dest
				node = fNode.LRNode
				if debug {
					MyAssert(registers.size == node.BeforeRegisterSize)
					MyAssert(fieldHistory.size == node.BeforeFieldHistorySize)
				}
			}
		}

		if beforeNode == node {
			if node.Op.isReadOp {
				_, newAccount := node.GetOrLoadAccountValueID(env, registers, accountHistory)
				if fNode != fStart {
					MyAssert(newAccount)
				}
			}
			var fVId uint
			var fVal interface{}
			fVId, fVal = fNode.LRNode.LoadFieldValueID(env, registers, fieldHistory)
			jd := fNode.GetFJumpDef(fVId)
			if jd == nil {
				if debugOut != nil {
					debugOut("  FieldLane FJump Failed #%v %v on FIndex %v FVId %v\n", node.Seq, node.Statement.SimpleNameString(), node.FieldIndex, fVId)
				}
				registers.AppendRegister(node.OutputRegisterIndex, fVal)
				node = fNode.Exit
				if debug {
					MyAssert(registers.size == node.BeforeRegisterSize)
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
					MyAssert(jd.RegisterSnapshot != nil)
				}
				registers.ApplySnapshot(jd.RegisterSnapshot)
				fNode = jd.Dest
				node = fNode.LRNode
				if debug {
					MyAssert(registers.size == node.BeforeRegisterSize)
				}
			}
		}

		if fNode.LRNode.Op.isStoreOp {
			if debugOut != nil {
				debugOut("FieldLane Hit by reaching: #%v %v\n", node.Seq, node.Statement.SimpleNameString())
			}
			ok = true
			return
		}
	}

	// start the OpLane
	if debug {
		MyAssert(fNode.Exit == node)
		MyAssert(!node.Op.isStoreOp)
	}

	for {
		if abort() {
			aborted = true
			return
		}
		jHead := node.JumpHead
		beforeJumpNode := node
		if jHead != nil {
			aCheckCount := 0
			fCheckCount := 0
			jNode := jHead
			// try to jump
			node = tt.OpLaneJump(debug, jNode, node, aCheckCount, accountHistory, debugOut, fieldHistory, registers, fCheckCount, beforeJumpNode)
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
				_, newAccount := node.GetOrLoadAccountValueID(env, registers, accountHistory)
				if debug {
					MyAssert(newAccount)
				}
			}
			if node.IsRLNode {
				_, fVal = node.LoadFieldValueID(env, registers, fieldHistory)
			} else {
				fVal = node.Execute(env, registers)
			}

			if node.OutputRegisterIndex != 0 {
				registers.AppendRegister(node.OutputRegisterIndex, fVal)
			}

			node = node.GetNextNode(registers)
		}

		if node == nil {
			node = beforeJumpNode
			if debug {
				MyAssert(node.Op.IsGuard())
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
			ok = false
			return
		}

		if node.Op.isStoreOp {
			if debugOut != nil {
				debugOut("OpLane Hit by reaching: #%v %v", node.Seq, node.Statement.SimpleNameString())
			}
			ok = true
			return
		}
	}
}

func GetOpJumKey(rf *RegisterFile, indices []uint) (key OpJumpKey, ok bool) {
	if DEBUG_TRACER {
		MyAssert(len(indices) > 0)
		MyAssert(len(indices) <= OP_JUMP_KEY_SIZE)
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

func (tt *TraceTrie) OpLaneJump(debug bool, jNode *OpSearchTrieNode, node *SNode, aCheckCount int, accountHistory *RegisterFile, debugOut func(fmtStr string, params ...interface{}), fieldHistory *RegisterFile, registers *RegisterFile, fCheckCount int, beforeJumpNode *SNode) *SNode {
	for {
		if debug {
			MyAssert(jNode.OpNode == node)
		}
		if jNode.IsAccountJump {
			aCheckCount += len(jNode.VIndices)

			aKey, aOk := GetOpJumKey(accountHistory, jNode.VIndices)
			var jd *OpNodeJumpDef
			if aOk {
				jd = jNode.GetMapJumpDef(&aKey)
			}
			if jd == nil {
				if debugOut != nil {
					debugOut("  OpLane ACheck Failed #%v %v on AIndex %v AVId %v\n", node.Seq, node.Statement.SimpleNameString(),
						jNode.VIndices, aKey[:len(jNode.VIndices)])
				}
				if jNode.Exit != nil {
					jNode = jNode.Exit.Next
					if debug {
						MyAssert(jNode != nil)
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
						MyAssert(node.FieldIndex != 0)
						MyAssert(node.IsRLNode)
						MyAssert(node.BeforeFieldHistorySize == node.FieldIndex)
					}
					fieldHistory.AppendSegment(node.BeforeFieldHistorySize, jd.FieldHistorySegment)
				}
				if len(jd.RegisterSegment) > 0 {
					if debug {
						MyAssert(registers.size == node.BeforeRegisterSize, "%v, %v", registers.size, node.BeforeRegisterSize)
					}
					registers.AppendSegment(node.BeforeRegisterSize, jd.RegisterSegment)
				}

				jNode = jd.Next

				if jNode != nil {
					node = jNode.OpNode
					if debug {
						MyAssert(jd.Dest == nil)
					}
				} else if jd.Dest != nil {
					node = jd.Dest
				}

				if debug {
					MyAssert(registers.size == node.BeforeRegisterSize)
					MyAssert(fieldHistory.size == node.BeforeFieldHistorySize)
				}
			}
		} else {
			fCheckCount += len(jNode.VIndices)
			fKey, fOk := GetOpJumKey(fieldHistory, jNode.VIndices)
			var jd *OpNodeJumpDef
			if fOk {
				jd = jNode.GetMapJumpDef(&fKey)
			}
			if jd == nil {
				if debugOut != nil {
					debugOut("  OpLane FCheck Failed #%v %v on FIndex %v FVId %v\n", node.Seq, node.Statement.SimpleNameString(),
						jNode.VIndices, fKey[:len(jNode.VIndices)])
					MyAssert(jNode.Exit == nil)
				}
				jNode = nil
			} else {
				if debugOut != nil {
					debugOut("  OpLane FCheck Passed #%v %v on FIndex %v FVId %v\n", node.Seq, node.Statement.SimpleNameString(),
						jNode.VIndices, fKey[:len(jNode.VIndices)])
				}
				if node.IsJSPNode && node.Op.isLoadOp && len(node.FieldDependencies) > 0 && jd.Next == nil {
					if debug {
						MyAssert(len(jd.FieldHistorySegment) == 1)
						MyAssert(len(jd.RegisterSegment) == 1)
					}
					fieldHistory.AppendRegister(node.FieldIndex, jd.FieldHistorySegment[0])
				} else {
					if debug {
						MyAssert(jd.FieldHistorySegment == nil)
					}
				}
				if len(jd.RegisterSegment) > 0 {
					if debug {
						MyAssert(registers.size == node.BeforeRegisterSize, "%v, %v", registers.size, node.BeforeRegisterSize)
					}
					registers.AppendSegment(node.BeforeRegisterSize, jd.RegisterSegment)
				}

				jNode = jd.Next
				if jNode != nil {
					node = jNode.OpNode
					if debug {
						MyAssert(jd.Dest == nil)
					}
				} else if jd.Dest != nil {
					node = jd.Dest
				}

				if debug {
					MyAssert(registers.size == node.BeforeRegisterSize)
					MyAssert(fieldHistory.size == node.BeforeFieldHistorySize)
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
		MyAssert(guardKey != nil)
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

	nodeIndexToAccountValue := GetNodeIndexToAccountSnapMapping(trace, round.ReadDeps)
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
		MyAssert(node.Op.isLoadOp || node.Op.isReadOp)
		s := j.trace.Stats[nodeIndex]
		var aVId, fVId uint
		if node.IsANode {
			var aval interface{}
			var ok bool
			if node.Op.isLoadOp {
				aval, ok = j.nodeIndexToAccountValue[node.Seq]
				MyAssert(ok)
			} else {
				aval = s.output.val
			}
			aVId = node.GetOrCreateAccountValueID(aval)
			accountHistory.AppendRegister(node.AccountIndex, aVId)
			if tt.DebugOut != nil {
				tt.DebugOut("AccountVID for %v of %v is %v\n", s.SimpleNameStringWithRegisterAnnotation(rmap), aval, aVId)
			}
		} else {
			MyAssert(node.Op.isLoadOp)
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
		MyAssert(fSrc.LRNode == snodes[0])
		MyAssert(fSrc.Exit == snodes[1])
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
			MyAssert(jd.RegisterSnapshot == nil && jd.FieldHistorySegment == nil)
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
				MyAssert(fDes.LRNode == desNode)
				MyAssert(fDes.Exit == snodes[desNode.Seq+1])
				MyAssert(jd.FieldHistorySegment == nil)
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
				MyAssert(fDes.LRNode.Op.isLoadOp)
				srcAddr := fSrc.LRNode.Statement.inputs[0].BAddress()
				desAddr := fDes.LRNode.Statement.inputs[0].BAddress()
				if srcAddr != desAddr {
					break
				}
			}
			dJd := fNodeJds[di]
			MyAssert(dJd.Dest == fDes)
			rsnap := dJd.RegisterSnapshot
			MyAssert(rsnap != nil)
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
				MyAssert(jd.RegisterSnapshot == nil && jd.FieldHistorySegment == nil)
				jd.RegisterSnapshot = rsnap
				jd.FieldHistorySegment = fieldSeg
			} else {
				if tt.DebugOut != nil {
					tt.DebugOut("OldFieldLaneAJump from %v to %v on AVId %v adding fields %v\n",
						fSrc.LRNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping),
						fDes.LRNode.Statement.SimpleNameStringWithRegisterAnnotation(registerMapping),
						aVId, fieldSeg)
					MyAssert(jd.Dest == fDes)
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
		MyAssert(aSrc.LRNode == snodes[0])
		MyAssert(aSrc.Exit == fnodes[0])
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
			MyAssert(aDes.Exit.LRNode == desNode)
			jd.Dest = aDes
			MyAssert(jd.RegisterSnapshot == nil && jd.FieldHistorySnapshot == nil)
			MyAssert(j.aNodeFJds[i+1].Dest == aDes.Exit)
			jd.RegisterSnapshot = j.aNodeFJds[i+1].RegisterSnapshot
			MyAssert(jd.RegisterSnapshot != nil)
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
			MyAssert(aDes.LRNode == desNode)
			exitIndex := desNode.FieldIndex - 1
			if desNode.Op.isStoreOp {
				exitIndex = uint(len(fnodes) - 1)
			}
			MyAssert(aDes.Exit == fnodes[exitIndex])
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
			MyAssert(aDes.LRNode.Op.isStoreOp)
			MyAssert(aDes.LRNode.Seq == j.trace.FirstStoreNodeIndex)
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
		MyAssert(node.IsRLNode)
		if !node.IsANode {
			MyAssert(node.Op.isLoadOp)
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
					MyAssert(jn.OpNode == node)
					MyAssert(jn.IsAccountJump == true)
					MyAssert(len(jn.VIndices) == 1)
					MyAssert(jn.VIndices[0] == node.AccountIndex)
				}
			}

			aKey, aOk := GetOpJumKey(j.accountHistory, jn.VIndices)
			if DEBUG_TRACER {
				MyAssert(aOk)
				MyAssert(aKey != EMPTY_JUMP_KEY)
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
					MyAssert(aOk)
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
						MyAssert(aNode != nil)
						MyAssert(aNode.IsAccountJump == true)
						AssertUArrayEqual(aNode.VIndices, aIndices)
						MyAssert(jd.FieldHistorySegment == nil)
						MyAssert(jd.RegisterSegment == nil)
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
				MyAssert(desNode == snodes[desNode.Seq])
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
				MyAssert(node.OutputRegisterIndex != 0)
				jd.RegisterSegment = j.registers.Slice(node.OutputRegisterIndex, desNode.BeforeRegisterSize)
				MyAssert(node.FieldIndex != 0)
				jd.FieldHistorySegment = j.fieldHistroy.Slice(node.FieldIndex, desNode.BeforeFieldHistorySize)
			} else {
				if tt.DebugOut != nil {
					tt.DebugOut("OldOpLaneLAJump from %v to %v on aKey %v\n",
						node.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
						desNode.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
						aKey[:1])
					MyAssert(jd.Next == nil)
					MyAssert(jd.Dest == desNode)
					jd.RegisterSegment.AssertEqual(j.registers.Slice(node.OutputRegisterIndex, desNode.BeforeRegisterSize))
					jd.FieldHistorySegment.AssertEqual(j.fieldHistroy.Slice(node.FieldIndex, desNode.BeforeFieldHistorySize))
				}
			}

			if len(aNodes) > 0 {
				MyAssert(len(node.FieldDependenciesExcludingSelfAccount) > 0)
				if aNodes[0].Exit == nil {
					jd = &OpNodeJumpDef{}
					for _, n := range aNodes {
						MyAssert(n.Exit == nil)
						n.Exit = jd
					}
				} else {
					firstExist := aNodes[0].Exit
					MyAssert(firstExist != nil)
					for _, n := range aNodes[1:] {
						if n.Exit != nil {
							MyAssert(n.Exit == firstExist)
						} else {
							n.Exit = firstExist
						}
					}
					jd = firstExist
				}

				fDeps := node.FieldDependenciesExcludingSelfAccount

				for i := 0; i < len(fDeps); i += OP_JUMP_KEY_SIZE {
					start := i
					end := i + OP_JUMP_KEY_SIZE
					if end > len(fDeps) {
						end = len(fDeps)
					}
					fIndices := fDeps[start:end]
					fKey, fOk := GetOpJumKey(j.fieldHistroy, fIndices)
					if DEBUG_TRACER {
						MyAssert(fOk)
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
						MyAssert(fNode != nil)
						MyAssert(fNode.IsAccountJump == false)
						AssertUArrayEqual(fNode.VIndices, fIndices)
						MyAssert(jd.FieldHistorySegment == nil)
						MyAssert(jd.RegisterSegment == nil)
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
					MyAssert(node.OutputRegisterIndex != 0)
					jd.RegisterSegment = j.registers.Slice(node.OutputRegisterIndex, node.OutputRegisterIndex+1)
					MyAssert(node.FieldIndex != 0)
					jd.FieldHistorySegment = j.fieldHistroy.Slice(node.FieldIndex, node.FieldIndex+1)
				} else {
					if tt.DebugOut != nil {
						tt.DebugOut("OldOpLaneLHJump from %v to %v on aKey %v\n",
							node.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
							desNode.Statement.SimpleNameStringWithRegisterAnnotation(rMap),
							aKey[:1])
						MyAssert(jd.Next == nil)
						MyAssert(jd.Dest == desNode)
						jd.RegisterSegment.AssertEqual(j.registers.Slice(node.OutputRegisterIndex, node.OutputRegisterIndex+1))
						jd.FieldHistorySegment.AssertEqual(j.fieldHistroy.Slice(node.FieldIndex, node.FieldIndex+1))
					}
				}
			}
		}
	}

	// setup for other Compute/Guard JSPs
	nodeBudget := len(j.trace.Stats) * 5 // shared budget

	jumpStations := j.trace.RLNodeIndices[:]
	jumpStations = append(jumpStations, j.trace.FirstStoreNodeIndex)
	for i, nodeIndex := range jumpStations[:len(jumpStations)-1] {
		targetNodeIndex := jumpStations[i+1]
		targetNode := snodes[targetNodeIndex]
		MyAssert(targetNodeIndex > nodeIndex)
		MyAssert(targetNode.IsRLNode || targetNode.Op.isStoreOp)
		MyAssert(targetNode == snodes[targetNode.Seq])
		if targetNodeIndex-nodeIndex == 1 {
			continue
		}
		jumpSrc := snodes[nodeIndex]
		MyAssert(jumpSrc.IsRLNode)
		for {
			// find out JumpSrc
			for {
				jumpSrc = snodes[jumpSrc.Seq+1]
				if jumpSrc == targetNode {
					break
				}
				MyAssert(!jumpSrc.IsRLNode)
				if jumpSrc.IsJSPNode {
					break
				}
			}

			if jumpSrc == targetNode {
				break
			}
			MyAssert(!jumpSrc.IsRLNode && jumpSrc.IsJSPNode)

			// create OpFJumps
			nodeBudget += 5 // allocate 5 nodes budget for the FJumps of each JSP
			nodeIndexToFJumpNodes := make(map[uint]*OpSearchTrieNode)
			jumpDes := jumpSrc
			jd := &OpNodeJumpDef{} // head.Exit
			if jumpSrc.JumpHead != nil {
				jd = jumpSrc.JumpHead.Exit
				MyAssert(jd != nil)
			}
			j.AddRefNode(jd)
			newCreatedJd := false
			jdFromNode := jumpSrc
			var jdFromFIndices []uint
			var jdFromFKey OpJumpKey
			fNodeSeq := 0

			fDeps := make(InputDependence, 0, 100)
			fDepBuffer := make(InputDependence, 0, 100)
			newFDeps := make(InputDependence, 0, 100)
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
							MyAssert(jd.FieldHistorySegment == nil)
							jd.RegisterSegment.AssertEqual(j.registers.Slice(jdFromNode.BeforeRegisterSize, jumpDes.BeforeRegisterSize))
							if jumpDes == targetNode {
								MyAssert(jd.Next == nil)
								MyAssert(jd.Dest != nil)
								MyAssert(jd.Dest == targetNode, "%v", jd.Dest.Statement.SimpleNameStringWithRegisterAnnotation(rMap))
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
						MyAssert(fOk)
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
						MyAssert(fjn.IsAccountJump == false)
						MyAssert(fjn.OpNode == jumpDes)
					}

					if _, ok := nodeIndexToFJumpNodes[jumpDes.Seq]; !ok {
						MyAssert(i == 0)
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

				MyAssert(snodes[jumpDes.Seq] == jumpDes)
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
			aDeps := make(InputDependence, 0, 100)
			aDepBuffer := make(InputDependence, 0, 100)
			aNodeSeq := 0
			newADeps := make(InputDependence, 0, 100)
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
							MyAssert(jd.FieldHistorySegment == nil)
							jd.RegisterSegment.AssertEqual(j.registers.Slice(jdFromNode.BeforeRegisterSize, jumpDes.BeforeRegisterSize))
							if jumpDes == targetNode {
								MyAssert(jd.Next == nil)
								MyAssert(jd.Dest == targetNode)
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
						MyAssert(aOk)
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
							MyAssert(fjn.OpNode == jumpDes)
							MyAssert(fjn.IsAccountJump == false)
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
						MyAssert(ajn.IsAccountJump == true)
						MyAssert(ajn.OpNode == jumpDes)
						if ajn.Exit != nil {
							fjn, ok := nodeIndexToFJumpNodes[jumpDes.Seq]
							MyAssert(ok)
							MyAssert(ajn.Exit.Next == fjn)
							MyAssert(fjn.OpNode == jumpDes)
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

				MyAssert(snodes[jumpDes.Seq] == jumpDes)
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
	MyAssert(node.Op.isStoreOp)
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
