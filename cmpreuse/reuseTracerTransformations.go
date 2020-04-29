package cmpreuse

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
)

type DebugOutFunc func(fmtStr string, args ...interface{})

func PrintStatementsWithWriteOut(stats []*Statement, debugOut DebugOutFunc) {
	PrintStatementsWithWriteOutAndRegisterAnnotation(stats, nil, debugOut)
}

func PrintStatementsWithWriteOutAndRegisterAnnotation(stats []*Statement, registerMapping *map[uint32]uint, debugOut DebugOutFunc) {
	if debugOut == nil {
		return
	}
	if len(stats) > 1 {
		debugOut("Statement Count: %v\n", len(stats))
	}
	for _, s := range stats {
		prefix := ">"
		if s.Reverted {
			prefix = "<"
		}
		if s.op.isStoreOp {
			prefix += "S"
		} else if s.op.isLoadOp {
			prefix += "L"
		} else if s.op.IsGuard() {
			prefix += "G"
		} else if s.op.isReadOp {
			prefix += "R"
		} else {
			prefix += ">"
		}

		debugOut(prefix + " " + s.SimpleNameStringWithRegisterAnnotation(registerMapping) + "\n")
		debugOut("     " + s.RecordedValueString() + "\n")
	}
}

func reversedStatements(inputStats []*Statement) []*Statement {
	length := len(inputStats)
	outStats := make([]*Statement, length)
	for i, s := range inputStats {
		outStats[length-i-1] = s
	}
	return outStats
}

func HasInterestingStatements(stats []*Statement) bool {
	var hasRead, hasStore, hasLog bool
	for _, s := range stats {
		if s.op.IsLog() {
			hasLog = true
		} else if s.op.isStoreOp {
			hasStore = true
		} else if s.op.isReadOp {
			hasRead = true
		}
		if hasRead && hasStore && hasLog {
			return true
		}
	}
	return false
}

type PassFunc func([]*Statement, DebugOutFunc) []*Statement

func ExecutePassAndPrint(pass PassFunc, stats []*Statement) []*Statement {
	return ExecutePassAndPrintWithOut(pass, stats, nil)
}

func ExecutePassAndPrintWithOut(pass PassFunc, stats []*Statement, debugOut DebugOutFunc) []*Statement {
	//if debugOut == nil {
	//	debugOut = func(fmtStr string, args ...interface{}) {
	//		fmt.Printf(fmtStr, args...)
	//	}
	//}

	funcName := GetFuncNameStr(pass)
	if debugOut != nil {
		debugOut("\n")
	}
	if debugOut != nil {
		debugOut("PASS %v\n", funcName)
	}
	after := pass(stats, debugOut)
	PrintStatementsWithWriteOut(after, debugOut)
	return after
}

type NumberedStatement struct {
	s *Statement
	n int
}

func EliminateRevertedStore(inputStats []*Statement, debugOut DebugOutFunc) []*Statement {
	outStats := make([]*Statement, 0, len(inputStats))
	appendOut := func(s *Statement) {
		outStats = append(outStats, s)
	}

	for _, s := range inputStats {
		if s.Reverted && s.op.isStoreOp {
			continue
		}
		appendOut(s)
	}

	return outStats
}

//func EliminateRedundantCodeGuard(inputStats []*Statement, debugOut WriteOutFunc) []*Statement {
//	outStats := make([]*Statement, 0, len(inputStats))
//	appendOut := func(s *Statement) {
//		outStats = append(outStats, s)
//	}
//
//	for _, s := range inputStats {
//		if s.Reverted && s.op.isStoreOp {
//			continue
//		}
//		appendOut(s)
//	}
//
//	return outStats
//}

//func EliminateRedundantLoad(inputStats []*Statement, debugOut WriteOutFunc) []*Statement {
//	outStats := make([]*Statement, 0, len(inputStats))
//	appendOut := func(s *Statement) {
//		outStats = append(outStats, s)
//	}
//
//	currentValueHolder := make(map[string] NumberedStatement)
//	initialValueHolder := make(map[string]*Statement)
//	latestSuicide := make(map[common.Address]int)
//
//	for i, s := range inputStats {
//		if !s.op.isLoadOp {
//			appendOut(s)
//		}
//
//		if !s.op.isLoadOp && !s.op.isStoreOp {
//			continue
//		}
//
//		// Load Empty and Load Exist are special, we don't optimize them here
//		//if !s.op.isLoadOp || (s.op.isLoadOp && (s.op.config.variant == ACCOUNT_EMPTY || s.op.config.variant == ACCOUNT_EXIST)) {
//		//	appendOut(s)
//		//	if !s.op.isStoreOp {
//		//		continue
//		//	}
//		//}
//
//		// s is ether a load or a store
//		accountAddr := s.inputs[0].BAddress()
//		key := *(s.op.config.variant) // load/store account field (e.g. balance, nonce, codehash, state, committedState...)
//		if s.op.IsStates() {
//			key = s.inputs[1].BHash().String() // append the storage key for load/store state or committedStates
//		}
//		key = accountAddr.String() + key
//		//if key == nil {
//		//	key = s.inputs[1].val // load storage
//		//}
//
//		if s.op.isLoadOp {
//			if s.op == OP_LoadCommittedState {
//				vh, exist := initialValueHolder[key]
//				if exist {
//					if vh.output != nil { // first op is load
//						loadedVar := vh.output
//						if !loadedVar.VEqual(s.output) {
//							panic(fmt.Sprintf("previous value %v does not match current %v", vh.SimpleNameString(), s.SimpleNameString()))
//						}
//						newS := newAssignStatement(s.output, loadedVar)
//						appendOut(newS)
//						if debugOut != nil { debugOut(fmt.Sprintf("EliminateRedundantLoad: replace %s by %s due to %s\n", s.SimpleNameString(), newS.SimpleNameString(), vh.SimpleNameString())) }
//					} else { // first op is store
//						panic("No load before store should never happen!")
//						//initialValueHolder[key] = s
//					}
//				} else {
//					initialValueHolder[key] = s
//					appendOut(s)
//				}
//				continue
//			}
//
//			if s.op == OP_LoadState {
//				if _, exist := initialValueHolder[key]; !exist {
//					initialValueHolder[key] = s
//				}
//			}
//
//			vh, exist := currentValueHolder[key]
//			if !exist {
//				appendOut(s)
//				currentValueHolder[key] = NumberedStatement{s:s, n:i}
//			} else {
//				loadedVar := vh.s.output
//				if loadedVar == nil { // vh is a store
//					loadedVar = vh.s.inputs[len(vh.s.inputs)-1]
//				}
//				if !loadedVar.VEqual(s.output) {
//					if sn, ok := latestSuicide[accountAddr]; ok && sn > vh.n && s.output.BHash() == (common.Hash{}) {
//						loadedVar = s.output.tracer.Hash_Empty
//						if debugOut != nil { debugOut("EliminateRedundantLoad: read after suicide %s", s.SimpleNameString()) }
//					}else {
//						panic(fmt.Sprintf("EliminateRedundantLoad: previous value %v does not match current %v", vh.s.SimpleNameString(), s.SimpleNameString()))
//					}
//				}
//				newS := newAssignStatement(s.output, loadedVar)
//				appendOut(newS)
//				if debugOut != nil { debugOut(fmt.Sprintf("EliminateRedundantLoad: replace %s by %s due to %s\n", s.SimpleNameString(), newS.SimpleNameString(), vh.s.SimpleNameString())) }
//			}
//		} else { // store op
//			currentValueHolder[key] = NumberedStatement{s:s, n:i}
//			if s.op == OP_StoreState {
//				if _, exist := initialValueHolder[key]; !exist {
//					panic("No load before store should never happen!")
//					//initialValueHolder[key] = s
//				}
//			}
//			if s.op == OP_StoreSuicide {
//				latestSuicide[accountAddr] = i
//			}
//		}
//	}
//
//	return outStats
//}

func EliminateRedundantStore(inputStats []*Statement, debugOut DebugOutFunc) []*Statement {
	reversedOutStats := make([]*Statement, 0, len(inputStats))
	stored := make(map[common.Address]map[interface{}]*Statement)

	markStored := func(addr common.Address, key interface{}, s *Statement) {
		kv, ok := stored[addr]
		if !ok {
			kv = make(map[interface{}]*Statement)
			stored[addr] = kv
		}
		kv[key] = s
	}

	appendOut := func(s *Statement) {
		reversedOutStats = append(reversedOutStats, s)
	}

	reservedInStats := reversedStatements(inputStats)
	for _, s := range reservedInStats {
		if !s.op.isStoreOp || s.op.IsLog() || s.op.IsVirtual() {
			appendOut(s)
			continue
		}

		if s.op == OP_StoreEmpty || s.op == OP_StoreExist || s.op == OP_StoreCodeHash || s.op == OP_StoreCodeSize {
			if debugOut != nil {
				debugOut(fmt.Sprintf("EliminateRedundantStore: removed superfical store %s\n", s.SimpleNameString()))
			}
			continue
		}

		accountAddr := s.inputs[0].BAddress()
		var key interface{}
		key = s.op.config.variant // store account field (e.g. balance, nonce, codehash...)
		if s.op.IsStates() {
			key = s.inputs[1].BHash() // store storage
		}

		if lastS, exist := stored[accountAddr][key]; exist {
			if debugOut != nil {
				debugOut(fmt.Sprintf("EliminateRedundantStore: removed store %s due to %s\n", s.SimpleNameString(), lastS.SimpleNameString()))
			}
		} else {
			appendOut(s)
			markStored(accountAddr, key, s)
		}
	}

	return reversedStatements(reversedOutStats)
}

func EliminateSuicidedStore(inputStats []*Statement, debugOut DebugOutFunc) []*Statement {
	reversedOutStats := make([]*Statement, 0, len(inputStats))
	suicided := make(map[common.Address]struct{})
	stored := make(map[common.Address]struct{})

	appendOut := func(s *Statement) {
		reversedOutStats = append(reversedOutStats, s)
	}

	reservedInStats := reversedStatements(inputStats)
	for _, s := range reservedInStats {
		if !s.op.isStoreOp || s.op.IsLog() || s.op.IsVirtual() {
			appendOut(s)
			continue
		}

		addr := s.inputs[0].BAddress()

		if s.op == OP_StoreSuicide {
			appendOut(s)
			if _, ok := stored[addr]; ok {
				panic(fmt.Sprintf("Store after suicide %v", addr.Hex()))
			}
			if _, ok := suicided[addr]; ok {
				panic(fmt.Sprintf("Suicided twice %v", addr.Hex()))
			}
			suicided[s.inputs[0].BAddress()] = struct{}{}
			continue
		}

		stored[addr] = struct{}{}
		if _, ok := suicided[addr]; ok {
			if debugOut != nil {
				debugOut(fmt.Sprintf("EliminateRedundantStore: removed suicided store %s\n", s.SimpleNameString()))
			}
		} else {
			appendOut(s)
		}
	}

	return reversedStatements(reversedOutStats)
}

func EliminateUnusedStatement(inputStats []*Statement, debugOut DebugOutFunc) []*Statement {
	reversedOutStats := make([]*Statement, 0, len(inputStats))
	usedVariables := map[uint32]struct{}{}

	appendOut := func(s *Statement) {
		reversedOutStats = append(reversedOutStats, s)
		for _, v := range s.inputs {
			usedVariables[v.id] = struct{}{}
		}
	}

	for _, s := range reversedStatements(inputStats) {
		//if s.op.IsLoadOrStoreOrRead() || s.op.IsGuard() {
		if s.op.isStoreOp || s.op.IsGuard() {
			appendOut(s)
		} else {
			if _, used := usedVariables[s.output.id]; used {
				appendOut(s)
			} else {
				if debugOut != nil {
					debugOut("EliminateUnusedStatement: removed %s\n", s.SimpleNameString())
				}
			}
		}
	}
	return reversedStatements(reversedOutStats)
}

//func EliminateRedundantEqualGeneric(inputStats []*Statement, debugOut DebugOutFunc) []*Statement {
//	outStats := make([]*Statement, 0, len(inputStats))
//	appendOut := func(s *Statement) {
//		outStats = append(outStats, s)
//	}
//	replaceMapping := make(map[uint32]*Variable)
//
//	for _, s := range inputStats {
//		newS := s.NewIfInputsReplaced(replaceMapping)
//		if newS == nil {
//			newS = s
//		} else {
//			if debugOut != nil {
//				debugOut(fmt.Sprintf("EliminateRedundantEqualGeneric: replaced %s by %s\n", s.SimpleNameString(), newS.SimpleNameString()))
//			}
//			if s.output != nil {
//				replaceMapping[s.output.id] = newS.output
//				if debugOut != nil {
//					debugOut(fmt.Sprintf("EliminateRedundantEqualGeneric: to replace %s by %s\n", s.output.Name(), newS.output.Name()))
//				}
//			}
//		}
//
//		if newS.op.IsGuard() && newS.inputs[0].IsConst() {
//			if debugOut != nil {
//				debugOut(fmt.Sprintf("EliminateRedundantEqualGeneric: removed %s\n", newS.SimpleNameString()))
//			}
//			continue
//		}
//
//		if newS.op == OP_EqualGeneric {
//			if newS.inputs[0].id == newS.inputs[1].id {
//				if debugOut != nil {
//					debugOut(fmt.Sprintf("EliminateRedundantEqualGeneric: removed %s\n", newS.SimpleNameString()))
//				}
//				copy := newS.output.Copy()
//				copy.MarkGuardedConst()
//				replaceMapping[newS.output.id] = copy
//				if debugOut != nil {
//					debugOut(fmt.Sprintf("EliminateRedundantEqualGeneric: to replace %s by %s\n", newS.output.Name(), copy.Name()))
//				}
//				continue
//			}
//		}
//		appendOut(newS)
//	}
//
//	return outStats
//}

//func ConstantPropagation(inputStats []*Statement, debugOut DebugOutFunc) []*Statement {
//	outStats := make([]*Statement, 0, len(inputStats))
//	appendOut := func(s *Statement) {
//		outStats = append(outStats, s)
//	}
//	replaceMapping := make(map[uint32]*Variable)
//
//	for _, s := range inputStats {
//		newS := s.NewIfInputsReplaced(replaceMapping)
//		if newS == nil {
//			newS = s
//		}
//
//		if !newS.op.IsLoadOrStoreOrRead() && newS.IsAllInputsConstant() {
//			if !newS.output.IsConst() {
//				newS.output.MarkConst()
//				replaceMapping[s.output.id] = newS.output
//				if debugOut != nil {
//					debugOut(fmt.Sprintf("ConstantPropagation: to replace %s by %s due to %s\n", s.output.Name(), newS.output.Name(), newS.SimpleNameString()))
//				}
//			}
//		}
//		if debugOut != nil {
//			debugOut(fmt.Sprintf("ConstantPropagation: replaced %s by %s\n", s.SimpleNameString(), newS.SimpleNameString()))
//		}
//		appendOut(newS)
//	}
//	return outStats
//}

func PushAllStoresToBottom(inputStats []*Statement, debugOut DebugOutFunc) []*Statement {
	outStats := make([]*Statement, 0, len(inputStats))
	stores := make([]*Statement, 0, len(inputStats))
	logs := make([]*Statement, 0, len(inputStats))
	appendOut := func(s *Statement) {
		outStats = append(outStats, s)
	}
	appendStore := func(s *Statement) {
		stores = append(stores, s)
	}
	appendLogs := func(s *Statement) {
		logs = append(logs, s)
	}

	for _, s := range inputStats {
		if s.op.IsLog() {
			appendLogs(s)
		} else if s.op.isStoreOp {
			appendStore(s)
		} else {
			appendOut(s)
		}
	}
	outStats = append(outStats, logs...)
	outStats = append(outStats, stores...)
	return outStats
}

func PushLoadReadComputationDownToFirstUse(inputStats []*Statement, debugOut DebugOutFunc) []*Statement {
	outStats := make([]*Statement, 0, len(inputStats))
	storeStats := make([]*Statement, 0, 100)
	originStatements := make(map[uint32]*Statement)
	appended := make(map[uint32]struct{})
	var appendOutRecursively func(s *Statement)

	appendOutRecursively = func(s *Statement) {
		if s.output != nil {
			if _, ok := appended[s.output.id]; ok {
				return
			}
		}

		for _, v := range s.inputs {
			if !v.IsConst() {
				appendOutRecursively(originStatements[v.id])
			}
		}

		// make sure all the computations are done before the stores
		if s.op.isStoreOp {
			storeStats = append(storeStats, s)
		} else {
			outStats = append(outStats, s)
		}

		if s.output != nil {
			appended[s.output.id] = struct{}{}
		}
	}

	for _, s := range inputStats {
		//if s.op.IsLoadOrStoreOrRead() || s.op.IsGuard() {
		if s.op.isStoreOp || s.op.IsGuard() {
			appendOutRecursively(s)
		}
		if s.output != nil {
			originStatements[s.output.id] = s
		}
	}

	outStats = append(outStats, storeStats...)

	return outStats
}

//func newGuard(input *Variable) *Statement {
//	output := input.tracer.VarWithName(input.val, input.customNamePart)
//	output.MarkGuardedConst()
//	return NewStatement(OP_Guard, output, input)
//}

//func getAllUnguardedLoadsAndReads(inputStats []*Statement, filter func(*Statement) bool) map[*Statement]struct{} {
//	loadsMap := make(map[*Statement]struct{}, len(inputStats))
//	originStatements := make(map[uint]*Statement)
//	for _, s := range inputStats {
//		if s.op.isLoadOp || s.op.isReadOp {
//			if filter != nil {
//				if !filter(s) {
//					continue
//				}
//			}
//			loadsMap[s] = struct{}{}
//			originStatements[s.output.id] = s
//		} else if s.op.IsGuard() {
//			delete(loadsMap, originStatements[s.inputs[0].id])
//		}
//	}
//	return loadsMap
//}
//
//func guardLoadsAndReads(inputStats []*Statement, loadsMap map[*Statement]struct{}) []*Statement {
//	outStats := make([]*Statement, 0, len(inputStats)+len(loadsMap))
//	appendOut := func(s *Statement) {
//		outStats = append(outStats, s)
//	}
//	for _, s := range inputStats {
//		appendOut(s)
//		if _, ok := loadsMap[s]; ok {
//			appendOut(newGuard(s.output))
//		}
//	}
//	return outStats
//}

//func GuardAllLoadsAndReads(inputStats []*Statement, debugOut WriteOutFunc) []*Statement {
//	loadsMap := getAllUnguardedLoadsAndReads(inputStats, nil)
//	return guardLoadsAndReads(inputStats, loadsMap)
//}
//
//func GuardAllLoadsAndReadsExceptForTxFromBalance(inputStats []*Statement, debugOut WriteOutFunc) []*Statement {
//	excludeTxFromBalance := func(s *Statement) bool {
//		if s.op.config.variant == ACCOUNT_BALANCE && s.inputs[0] == s.output.tracer.txFrom {
//			return false
//		}
//		return true
//	}
//	loadsMap := getAllUnguardedLoadsAndReads(inputStats, excludeTxFromBalance)
//	return guardLoadsAndReads(inputStats, loadsMap)
//}
//
//func GuardAllLoadsAndReadsExceptForTxToBalance(inputStats []*Statement, debugOut WriteOutFunc) []*Statement {
//	excludeTxFromBalance := func(s *Statement) bool {
//		if s.op.config.variant == ACCOUNT_BALANCE && s.inputs[0] == s.output.tracer.txTo {
//			return false
//		}
//		return true
//	}
//	loadsMap := getAllUnguardedLoadsAndReads(inputStats, excludeTxFromBalance)
//	return guardLoadsAndReads(inputStats, loadsMap)
//}
//
//func GuardedConstantPropagation(inputStats []*Statement, debugOut WriteOutFunc) []*Statement {
//	outStats := make([]*Statement, 0, len(inputStats))
//	appendOut := func(s *Statement) {
//		outStats = append(outStats, s)
//	}
//	replaceMapping := make(map[uint]*Variable)
//
//	for _, s := range inputStats {
//		newS := s.NewIfInputsReplaced(replaceMapping)
//		if newS == nil {
//			newS = s
//		} else {
//			if s.output != nil {
//				replaceMapping[s.output.id] = newS.output
//			}
//			if !newS.op.IsLoadOrStoreOrRead() && newS.IsAllInputsConstant() {
//				if !newS.output.IsConst() {
//					newS.output.MarkGuardedConst()
//				}
//			}
//			if debugOut != nil { debugOut(fmt.Sprintf("GuardedConstantPropagation: replaced %s by %s\n", s.SimpleNameString(), newS.SimpleNameString())) }
//		}
//		if s.op == OP_Guard {
//			replaceMapping[s.inputs[0].id] = s.output
//			if debugOut != nil { debugOut(fmt.Sprintf("GuardedConstantPropagation: to replace %s by %s due to %s\n", s.inputs[0].Name(), s.output.Name(), newS.SimpleNameString())) }
//		}
//		appendOut(newS)
//	}
//	return outStats
//}
//
//func EliminateConstantStatement(inputStats []*Statement, debugOut WriteOutFunc) []*Statement {
//	outStats := make([]*Statement, 0, len(inputStats))
//	appendOut := func(s *Statement) {
//		outStats = append(outStats, s)
//	}
//
//	for _, s := range inputStats {
//		if s.op.IsLoadOrStoreOrRead() {
//			appendOut(s)
//		} else {
//			if s.IsAllInputsConstant() {
//				if !s.output.IsConst() {
//					panic(fmt.Sprintf("Wrong const statement: %v", s.SimpleNameString()))
//				}
//				if debugOut != nil { debugOut(fmt.Sprintf("EliminateConstantStatement removed %s\n", s.SimpleNameString())) }
//			} else {
//				appendOut(s)
//			}
//		}
//	}
//	return outStats
//}
//
//func EliminateRedundantRead(inputStats []*Statement, debugOut WriteOutFunc) []*Statement {
//	outStats := make([]*Statement, 0, len(inputStats))
//	previousRead := make(map[string]*Statement)
//
//	appendOut := func(s *Statement) {
//		outStats = append(outStats, s)
//	}
//
//	for _, s := range inputStats {
//		if !s.op.isReadOp {
//			appendOut(s)
//			continue
//		}
//
//		key := *(s.op.config.variant) // read header field (e.g. timestamp, difficulty, blockhash ...)
//		if s.op == OP_ReadBlockHash {
//			key = key + s.inputs[0].BigInt().String()
//		}
//
//		if lastS, exist := previousRead[key]; exist {
//			newS := newAssignStatement(s.output, lastS.output)
//			appendOut(newS)
//			if debugOut != nil { debugOut(fmt.Sprintf("EliminateRedundantRead: replace %s by %s due to %s\n", s.SimpleNameString(), newS.SimpleNameString(), lastS.SimpleNameString())) }
//		} else {
//			appendOut(s)
//			previousRead[key] = s
//		}
//	}
//
//	return outStats
//}
//
//func EliminateRedundantVariable(inputStats []*Statement, debugOut WriteOutFunc) []*Statement {
//	outStats := make([]*Statement, 0, len(inputStats))
//	appendOut := func(s *Statement) {
//		outStats = append(outStats, s)
//	}
//	replaceMapping := make(map[uint]*Variable)
//
//	for _, s := range inputStats {
//		newS := s.NewIfInputsReplaced(replaceMapping)
//		if newS == nil {
//			newS = s
//		} else {
//			if debugOut != nil { debugOut(fmt.Sprintf("EliminateRedundantVariable: replaced %s by %s\n", s.SimpleNameString(), newS.SimpleNameString())) }
//			//replaceMapping[s.output.id] = newS.output
//		}
//		if newS.op == OP_AssignGeneric {
//			if newS.output.id == newS.inputs[0].id {
//				panic(fmt.Sprintf("The input should not equal to output: %v", newS.SimpleNameString()))
//			}
//			replaceMapping[newS.output.id] = newS.inputs[0]
//			if debugOut != nil { debugOut(fmt.Sprintf("EliminateRedundantVariable: to replace %s by %s and removed %s\n", newS.output.Name(), newS.inputs[0].Name(), newS.SimpleNameString())) }
//		} else {
//			appendOut(newS)
//		}
//	}
//
//	return outStats
//}

func CrossCheck(stats []*Statement, rDetail *cmptypes.ReadDetail, wStates state.WriteStates, logs []*types.Log, debugOut DebugOutFunc) () {
	detailSeq := rDetail.ReadDetailSeq
	dumpAndPanic := func() {
		//detailBytes, _ := json.Marshal(rDetail)
		//logBytes, _ := json.Marshal(logs)
		//wStatesBytes, _ := json.Marshal(wStates)
		//if debugOut != nil { debugOut("ReadDetail:\n") }
		//if debugOut != nil { debugOut("%v\n", string(detailBytes)) }
		if debugOut != nil {
			debugOut("Detailseq:\n")
		}
		for _, e := range detailSeq {
			seqBytes, _ := json.Marshal(e)
			if debugOut != nil {
				debugOut("%v\n", string(seqBytes))
			}
		}
		if debugOut != nil {
			debugOut("WriteStates:\n")
		}
		for addr, e := range wStates {
			wStatesBytes, _ := json.Marshal(e)
			if debugOut != nil {
				debugOut("%v: %v\n", addr.Hex(), string(wStatesBytes))
			}
		}
		if debugOut != nil {
			debugOut("Logs:\n")
		}
		for _, e := range logs {
			logBytes, _ := json.Marshal(e)
			if debugOut != nil {
				debugOut("%v\n", string(logBytes))
			}
		}
		panic("Crosscheck failed!")
	}

	LoadsAndReads := make([]*Statement, 0, len(stats))
	StoreLogs := make([]*Statement, 0, len(stats))
	Stores := make([]*Statement, 0, len(stats))

	for _, s := range stats {
		if s.op.isLoadOp || s.op.isReadOp {
			LoadsAndReads = append(LoadsAndReads, s)
		} else if s.op.IsLog() {
			StoreLogs = append(StoreLogs, s)
		} else if s.op.isStoreOp && !s.op.IsVirtual() {
			Stores = append(Stores, s)
		}
	}

	lr := make([]*Statement, 0, len(LoadsAndReads))

	for _, s := range LoadsAndReads {
		if s.op == OP_LoadCode {
			continue
		}
		if s.op == OP_LoadCodeSize {
			continue
		}
		lr = append(lr, s)
	}
	LoadsAndReads = lr

	// check read sets
	detailSeqEssential := make([]*cmptypes.AddrLocValue, 0, len(detailSeq))
	loadedState := make(map[string]struct{})
	for _, alv := range detailSeq {
		switch alv.AddLoc.Field {
		case cmptypes.Storage:
			if alv.AddLoc.Field == cmptypes.Storage {
				key := alv.AddLoc.Address.String() + alv.AddLoc.Loc.(common.Hash).String()
				loadedState[key] = struct{}{}
			}
		case cmptypes.CommittedStorage:
			key := alv.AddLoc.Address.String() + alv.AddLoc.Loc.(common.Hash).String()
			if _, ok := loadedState[key]; !ok {
				if debugOut != nil {
					debugOut("Load commitedStorage with Load State first!\n")
				}
				dumpAndPanic()
			}
			continue
		}
		detailSeqEssential = append(detailSeqEssential, alv)
	}

	detailSeq = detailSeqEssential

	reportMismatch := func(i int, s *Statement, alv interface{}) {
		alvBytes, _ := json.Marshal(alv)
		if debugOut != nil {
			debugOut("%v item mismatch %v (%v) and %v\n", i, s.SimpleNameString(), s.RecordedValueString(), string(alvBytes))
		}
		dumpAndPanic()
	}

	hashNormalize := func(codeHash common.Hash) common.Hash {
		if codeHash == emptyCodeHash {
			return common.Hash{}
		} else {
			return codeHash
		}
	}

	if len(detailSeq) != len(LoadsAndReads) {
		if debugOut != nil {
			debugOut("read set length mismatch %v and %v\n", len(LoadsAndReads), len(detailSeq))
		}
	}

	for i, alv := range detailSeq {
		if i >= len(LoadsAndReads) {
			break
		}
		s := LoadsAndReads[i]
		switch alv.AddLoc.Field {
		case cmptypes.Balance:
			if s.op != OP_LoadBalance || s.inputs[0].BAddress() != alv.AddLoc.Address || common.BigToHash(s.output.BigInt()) != alv.Value.(common.Hash) {
				reportMismatch(i, s, alv)
			}
		case cmptypes.Nonce:
			if s.op != OP_LoadNonce || s.inputs[0].BAddress() != alv.AddLoc.Address || s.output.Uint64() != alv.Value.(uint64) {
				reportMismatch(i, s, alv)
			}
		case cmptypes.CodeHash:
			if s.op != OP_LoadCodeHash || s.inputs[0].BAddress() != alv.AddLoc.Address || hashNormalize(s.output.BHash()) != alv.Value.(common.Hash) {
				reportMismatch(i, s, alv)
			}
		case cmptypes.Empty:
			if s.op != OP_LoadEmpty || s.inputs[0].BAddress() != alv.AddLoc.Address || s.output.Bool() != alv.Value.(bool) {
				reportMismatch(i, s, alv)
			}
		case cmptypes.Exist:
			if s.op != OP_LoadExist || s.inputs[0].BAddress() != alv.AddLoc.Address || s.output.Bool() != alv.Value.(bool) {
				reportMismatch(i, s, alv)
			}
		case cmptypes.Storage:
			if s.op != OP_LoadState || s.inputs[0].BAddress() != alv.AddLoc.Address || s.inputs[1].BHash() != alv.AddLoc.Loc.(common.Hash) || s.output.BHash() != alv.Value.(common.Hash) {
				reportMismatch(i, s, alv)
			}
		case cmptypes.Coinbase:
			if s.op != OP_ReadCoinbase || s.output.BAddress() != alv.Value.(common.Address) {
				reportMismatch(i, s, alv)
			}
		case cmptypes.GasLimit:
			if s.op != OP_ReadGasLimit || s.output.BigInt().Uint64() != alv.Value.(uint64) {
				reportMismatch(i, s, alv)
			}
		case cmptypes.Difficulty:
			if s.op != OP_ReadDifficulty || common.BigToHash(s.output.BigInt()) != alv.Value.(common.Hash) {
				reportMismatch(i, s, alv)
			}
		case cmptypes.Number:
			if s.op != OP_ReadBlockNumber || s.output.BigInt().Uint64() != alv.Value.(uint64) {
				reportMismatch(i, s, alv)
			}
		case cmptypes.Timestamp:
			if s.op != OP_ReadTimestamp || s.output.BigInt().Uint64() != alv.Value.(uint64) {
				reportMismatch(i, s, alv)
			}
		case cmptypes.Blockhash:
			if s.op != OP_ReadBlockHash || s.inputs[0].BigInt().Uint64() != alv.AddLoc.Loc.(uint64) || s.output.BHash() != alv.Value.(common.Hash) {
				reportMismatch(i, s, alv)
			}
		default:
			if debugOut != nil {
				debugOut("Unknown field %v\n", alv.AddLoc.Field)
			}
			dumpAndPanic()
		}
	}

	if len(detailSeq) != len(LoadsAndReads) {
		if debugOut != nil {
			debugOut("read set length mismatch %v and %v\n", len(LoadsAndReads), len(detailSeq))
		}
		dumpAndPanic()
	}

	if len(StoreLogs) != len(logs) {
		if debugOut != nil {
			debugOut("log length mismatch %v and %v\n", len(StoreLogs), len(logs))
		}
	}

	for i, log := range logs {
		if i >= len(StoreLogs) {
			break
		}
		rLog := StoreLogs[i]
		address := rLog.inputs[0].BAddress()
		lenLogInputs := len(rLog.inputs)
		data := rLog.inputs[1]
		topics := rLog.inputs[2:lenLogInputs]
		if address != log.Address || string(data.ByteArray()) != string(log.Data) || len(topics) != len(log.Topics) {
			reportMismatch(i, rLog, log)
		}
		for j, t := range log.Topics {
			if topics[j].BHash() != t {
				reportMismatch(i, rLog, log)
			}
		}
	}

	if len(StoreLogs) != len(logs) {
		if debugOut != nil {
			debugOut("log length mismatch %v and %v\n", len(StoreLogs), len(logs))
		}
		dumpAndPanic()
	}

	totalWrites := 0
	remainingWStates := make(state.WriteStates)
	for addr, ws := range wStates {
		_ds := make(state.Storage)
		for k, v := range ws.DirtyStorage {
			_ds[k] = v
		}
		_ws := &state.WriteState{
			Balance:      ws.Balance,
			Nonce:        ws.Nonce,
			Code:         ws.Code,
			Suicided:     ws.Suicided,
			DirtyStorage: _ds,
		}
		totalWrites += len(_ds)
		if ws.Balance != nil {
			totalWrites += 1
		}
		if ws.Nonce != nil {
			totalWrites += 1
		}
		if ws.Code != nil {
			totalWrites += 1
		}
		if ws.Suicided != nil {
			totalWrites += 1
		}
		remainingWStates[addr] = _ws
	}

	totalStores := len(Stores)

	if totalStores != totalWrites {
		if debugOut != nil {
			debugOut("write count mismatch %v and %v\n", totalStores, totalWrites)
		}
	}

	for i, s := range Stores {
		addr := s.inputs[0].BAddress()

		if ws, ok := remainingWStates[addr]; ok {
			switch s.op.config.variant {
			case ACCOUNT_BALANCE:
				if ws.Balance != nil && ws.Balance.Cmp(s.inputs[1].BigInt()) == 0 {
					ws.Balance = nil
				} else {
					reportMismatch(i, s, ws)
				}
			case ACCOUNT_NONCE:
				if ws.Nonce != nil && (*ws.Nonce) == s.inputs[1].Uint64() {
					ws.Nonce = nil
				} else {
					reportMismatch(i, s, ws)
				}
			case ACCOUNT_CODE:
				if ws.Code != nil && string(*ws.Code) == string(s.inputs[1].ByteArray()) {
					ws.Code = nil
				} else {
					reportMismatch(i, s, ws)
				}
			case ACCOUNT_SUICIDE:
				if ws.Suicided != nil {
					ws.Suicided = nil
				} else {
					reportMismatch(i, s, ws)
				}
			case ACCOUNT_STATE:
				if v, ok := ws.DirtyStorage[s.inputs[1].BHash()]; ok {
					if v == s.inputs[2].BHash() {
						delete(ws.DirtyStorage, s.inputs[1].BHash())
						continue
					}
					reportMismatch(i, s, ws)
				}
			default:
				if debugOut != nil {
					debugOut("store %v should not exist\n", *(s.op.config.variant))
				}
				dumpAndPanic()
			}
		} else {
			if debugOut != nil {
				debugOut("missing addr %v in wstate\n", addr.Hex())
			}
			dumpAndPanic()
		}
	}

	if totalStores != totalWrites {
		//if debugOut != nil { debugOut("write count mismatch %v and %v\n", totalStores, totalWrites) }
		if debugOut != nil {
			debugOut("remaining writes:\n")
		}
		writeBytes, _ := json.Marshal(remainingWStates)
		if debugOut != nil {
			debugOut(string(writeBytes))
		}
		dumpAndPanic()
	}

}

func GetLRSL(stats []*Statement) (loadCount uint, readCount uint, storeCount uint, logCount uint) {
	loadCount = 0
	readCount = 0
	storeCount = 0
	logCount = 0

	for _, s := range stats {
		if s.op.isLoadOp && s.op != OP_LoadCode {
			loadCount++
		}
		if s.op.isReadOp {
			readCount++
		}
		if s.op.isStoreOp && !s.op.IsVirtual() {
			if s.op.IsLog() {
				logCount++
			} else {
				storeCount++
			}
		}
	}

	return
}
