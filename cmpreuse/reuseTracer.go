package cmpreuse

import (
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

type BlockHashNumIDM struct {
	mapping map[uint64]uint32
	mutable bool
}

func NewBlockHashNumIDM() *BlockHashNumIDM {
	return &BlockHashNumIDM{
		mapping: make(map[uint64]uint32),
	}
}

type ReuseTracer struct {
	Statements                 []*Statement
	cachedConstants            map[interface{}]*Variable
	valueCounter               uint32
	callFrameStack             []*TracerCallFrame
	returnData                 *Variable
	snapshotStartingPoints     []uint
	world                      *TracerWorldState
	statedb                    *state.StateDB
	header                     *types.Header
	getHashFunc                vm.GetHashFunc
	cachedHeaderRead           map[string]*Variable
	cachedComputes             map[string]*Variable
	byte32VarToBigIntVar       map[*Variable]*Variable
	bigIntVarToByte32Var       map[*Variable]*Variable
	isCompareOpOutput          map[*Variable]bool
	isZeroCachedResult         map[*Variable]*Variable
	byteArrayCachedSize        map[*Variable]*Variable
	byteArrayOriginCells       map[*Variable][]*MemByteCell
	currentPC                  uint64
	testOpImp                  bool
	EncounterUnimplementedCode bool
	IsExternalTransfer         bool
	IsCompleteTrace            bool
	Int_0                      *Variable
	BigInt_0                   *Variable
	BigInt_1                   *Variable
	BigInt_32                  *Variable
	BigInt_96                  *Variable
	BigInt_64                  *Variable
	Uint64_1                   *Variable
	ByteArray_Empty            *Variable
	EmptyHash                  *Variable
	EmptyCodeHash              *Variable
	txFrom                     *Variable
	txTo                       *Variable
	txGasPrice_BigInt          *Variable
	txGas_uint64               *Variable
	txValue                    *Variable
	txData_byteArray           *Variable
	txNonce_uint64             *Variable
	Bool_false                 *Variable
	Bool_true                  *Variable
	currentGas                 uint64
	txHash                     common.Hash
	ChainID                    *Variable
	chainRules                 params.Rules
	opCount                    int
	DebugBuffer                *DebugBuffer
	DebugFlag                  bool
	guardedBlockHashNum        map[*Variable]bool
	blockHashNumIDs            *BlockHashNumIDM
	blockHashNumIDMVar         *Variable
	hasNonConstBlockHashNum    bool
	methodCache                map[string]reflect.Value
	execEnv                    *ExecEnv
	TraceStartTime             time.Time
}

var ReuseTracerTracedTxCount uint64

//var testedTxs = make(map[common.Hash]int)

func NewReuseTracer(statedb *state.StateDB, header *types.Header, hashFunc vm.GetHashFunc, chainID *big.Int, chainRules params.Rules) *ReuseTracer {
	rt := &ReuseTracer{
		Statements:             make([]*Statement, 0, 100),
		cachedConstants:        make(map[interface{}]*Variable),
		snapshotStartingPoints: make([]uint, 1, 10),
		statedb:                statedb,
		header:                 header,
		getHashFunc:            hashFunc,
		IsExternalTransfer:     true,
		chainRules:             chainRules,
		cachedHeaderRead:       make(map[string]*Variable),
		world:                  NewTracerWorldState(),
		DebugFlag:              false,
		byte32VarToBigIntVar:   make(map[*Variable]*Variable),
		bigIntVarToByte32Var:   make(map[*Variable]*Variable),
		isZeroCachedResult:     make(map[*Variable]*Variable),
		isCompareOpOutput:      make(map[*Variable]bool),
		valueCounter:           1,
		cachedComputes:         make(map[string]*Variable, 1000),
		guardedBlockHashNum:    make(map[*Variable]bool),
		blockHashNumIDs:        NewBlockHashNumIDM(),
		methodCache:            make(map[string]reflect.Value),
		byteArrayCachedSize:    make(map[*Variable]*Variable),
		byteArrayOriginCells:   make(map[*Variable][]*MemByteCell),
		execEnv: &ExecEnv{inputs: make([]interface{}, 0, 10), state: statedb, header: header, getHash: hashFunc,
			precompiles: vm.GetPrecompiledMapping(&(chainRules))},
		DebugBuffer: NewDebugBuffer(nil),
		TraceStartTime: time.Now(),
	}

	rt.snapshotStartingPoints[0] = 0

	//rt.fOut, _ = os.Create("/tmp/lastTxTrace.txt")

	rt.Int_0 = rt.ConstVarWithName(int(0), "int0")
	rt.BigInt_0 = rt.ConstVarWithName(big.NewInt(0), "bi0")
	rt.BigInt_1 = rt.ConstVarWithName(big.NewInt(1), "bi1")
	rt.BigInt_32 = rt.ConstVarWithName(big.NewInt(32), "bi32")
	rt.BigInt_64 = rt.ConstVarWithName(big.NewInt(64), "bi64")
	rt.BigInt_96 = rt.ConstVarWithName(big.NewInt(96), "bi96")
	rt.Uint64_1 = rt.ConstVarWithName(uint64(1), "uint1")
	rt.ByteArray_Empty = rt.ConstVarWithName(make([]byte, 0), "[0]byte")
	rt.EmptyHash = rt.ConstVarWithName(common.Hash{}, "Hash{}")
	rt.EmptyCodeHash = rt.ConstVarWithName(cmptypes.EmptyCodeHash, "emptyCodeHash")
	rt.Bool_false = rt.ConstVarWithName(false, "false")
	rt.Bool_true = rt.ConstVarWithName(true, "true")

	rt.returnData = rt.ByteArray_Empty

	rt.ChainID = rt.ConstVarWithName(chainID, "chainID")
	return rt
}

func (rt *ReuseTracer) Snapshot() uint {
	currentPos := uint(len(rt.Statements))
	id := rt.world.Snapshot()
	if id != uint(len(rt.snapshotStartingPoints)) {
		panic(fmt.Errorf("Unsync snapshots %v, %v", id, len(rt.snapshotStartingPoints)))
	}
	rt.snapshotStartingPoints = append(rt.snapshotStartingPoints, currentPos)
	return id
}

func (rt *ReuseTracer) RevertToSnapshot(snapShotID uint) {

	if snapShotID > uint(len(rt.snapshotStartingPoints)) {
		panic(fmt.Errorf("Cannot revert to the future %d, %d", snapShotID, len(rt.snapshotStartingPoints)))
	}

	rt.world.RevertToSnapshot(snapShotID)

	pos := rt.snapshotStartingPoints[snapShotID]
	rt.snapshotStartingPoints = rt.snapshotStartingPoints[:snapShotID]

	lenS := uint(len(rt.Statements))

	if pos > lenS {
		panic(fmt.Errorf("Cannot revert to the future statements %d, %d", pos, lenS))
	}

	for i := pos; i < lenS; i++ {
		rt.Statements[i].Reverted = true
	}
}

func (rt *ReuseTracer) PeekCallFrame() *TracerCallFrame {
	return rt.callFrameStack[len(rt.callFrameStack)-1]
}

func (rt *ReuseTracer) CF() *TracerCallFrame {
	return rt.PeekCallFrame()
}

func (rt *ReuseTracer) Stack() *TracerStack {
	return rt.CF().stack
}

func (rt *ReuseTracer) Mem() *TracerMem {
	return rt.CF().mem
}

func (rt *ReuseTracer) Pop() *Variable {
	return rt.Stack().pop()
}

func (rt *ReuseTracer) Pop2() (*Variable, *Variable) {
	return rt.Pop(), rt.Pop()
}

func (rt *ReuseTracer) Pop3() (*Variable, *Variable, *Variable) {
	return rt.Pop(), rt.Pop(), rt.Pop()
}

func (rt *ReuseTracer) Push(variable *Variable) {
	switch variable.val.(type) {
	case *MultiTypedValue:
		// pass
	default:
		panic(fmt.Sprintf("Wrong push type %v", variable.varType.String()))
	}
	rt.Stack().push(variable)
}

func (rt *ReuseTracer) PushCallFrame(cf *TracerCallFrame) {
	rt.callFrameStack = append(rt.callFrameStack, cf)
}

func (rt *ReuseTracer) PopCallFrame() *TracerCallFrame {
	cf := rt.PeekCallFrame()
	rt.callFrameStack = rt.callFrameStack[:len(rt.callFrameStack)-1]
	return cf
}

func (rt *ReuseTracer) GetStackSize() int {
	return rt.CF().stack.len()
}

func (rt *ReuseTracer) GetMemSize() int {
	return rt.Mem().Len()
}

func (rt *ReuseTracer) GetTopStack() *big.Int {
	stack := rt.CF().stack
	if stack.len() == 0 {
		return nil
	}
	return rt.CF().stack.Back(0).BigInt()
}

func (rt *ReuseTracer) StartNewCall(caller, contract, codeAddr, value, input *Variable) {
	rt.PushCallFrame(NewCallFrame(rt, caller, contract, codeAddr, value, input))
}

func (rt *ReuseTracer) MarkUnimplementedOp() {
	rt.EncounterUnimplementedCode = true
}

func (rt *ReuseTracer) MarkNotExternalTransfer() {
	rt.IsExternalTransfer = false
}

func (rt *ReuseTracer) MarkCompletedTrace(failed bool) {
	v := rt.Bool_false
	if failed {
		v = rt.Bool_true
	}
	rt.Trace(OP_StoreFailed, nil, v)
	rt.IsCompleteTrace = true
}

func (rt *ReuseTracer) ClearReturnData() {
	rt.returnData = rt.ByteArray_Empty
}

func (rt *ReuseTracer) GetReturnData() []byte {
	return rt.returnData.ByteArray()
}

func (rt *ReuseTracer) InitTx(txHash common.Hash, from common.Address, to *common.Address,
	gasPrice, value *big.Int, gas, nonce uint64, data []byte) {
	rt.txHash = txHash
	rt.txFrom = rt.ConstVarWithName(from, "txFrom")
	if to != nil {
		rt.txTo = rt.ConstVarWithName(*to, "txTo")
	}
	rt.txGasPrice_BigInt = rt.ConstVarWithName(gasPrice, "txGasPrice")
	rt.txValue = rt.ConstVarWithName(value, "txValue")
	rt.txGas_uint64 = rt.ConstVarWithName(gas, "txGasLimit")
	rt.txNonce_uint64 = rt.ConstVarWithName(nonce, "txNonce")
	rt.txData_byteArray = rt.ConstVarWithName(data, "txData")
	rt.StartNewCall(rt.txFrom, rt.txTo, rt.txTo, rt.txValue, rt.txData_byteArray)
	//if rt.CF().callerAddress == nil {
	//	panic("callerAddress nil after txInit")
	//}
}

func CopyIfNeeded(val interface{}) interface{} {
	switch typedValue := val.(type) {
	case *big.Int:
		val = new(big.Int).Set(typedValue)
	}
	return val
}

func (rt *ReuseTracer) NeedRT() bool {
	return !rt.EncounterUnimplementedCode
}

func (rt *ReuseTracer) ConstVar(val interface{}) *Variable {
	return rt.ConstVarWithName(val, "")
}

func IsValCacheable(val interface{}) bool {
	switch val.(type) {
	case *big.Int:
		//return true
		panic("Should not see big int directly ")
	case []byte:
		return true
	case string:
		return true
	case *MultiTypedValue:
		return true
	}

	kind := reflect.TypeOf(val).Kind()
	if kind >= reflect.Int && kind <= reflect.Uint64 {
		return true
	}
	return false
}

func GetCacheKey(val interface{}) interface{} {
	switch bi := val.(type) {
	case *big.Int:
		panic("Should not see big int directly")
		//return "bi" + bi.String()
	case []byte:
		return "[]" + string(bi)
	case string:
		return "str" + bi
	case *MultiTypedValue:
		return bi.GetCacheKey()
	}
	return val
}

func CreateMultiTypedValueIfNeed(val interface{}, env *ExecEnv) interface{} {
	switch bi := val.(type) {
	case *big.Int:
		return NewBigIntValue(bi, env)
	case common.Address:
		return NewAddressValue(bi, env)
	case common.Hash:
		return NewHashValue(bi, env)
	}
	return val
}

func (rt *ReuseTracer) ConstVarWithName(val interface{}, name string) *Variable {
	val = CreateMultiTypedValueIfNeed(val, nil)

	cacheable := IsValCacheable(val)
	var cached *Variable
	var key interface{}
	var ok bool
	if cacheable {
		key = GetCacheKey(val)
		cached, ok = rt.cachedConstants[key]
	}
	if !ok {
		cached = rt.VarWithName(val, name)
		cached.constant = true
		if cacheable {
			if key == nil {
				panic("key should never be nil")
			}
			rt.cachedConstants[key] = cached
		}
	}
	//if cached == nil {
	//	panic(fmt.Sprintf("Nil var created, cacheable %v, ok %v", cacheable, ok))
	//}
	return cached
}

func (rt *ReuseTracer) Var(val interface{}) *Variable {
	return rt.VarWithName(val, "")
}

func (rt *ReuseTracer) VarWithName(val interface{}, name string) *Variable {
	v := &Variable{
		varType:        reflect.TypeOf(val),
		val:            CreateMultiTypedValueIfNeed(CopyIfNeeded(val), nil),
		constant:       false,
		id:             rt.valueCounter,
		customNamePart: name,
		tracer:         rt,
	}
	rt.valueCounter++
	return v
}

func (rt *ReuseTracer) NewStatement(f *OpDef, outputVar *Variable, inputVars ...*Variable) *Statement {
	return NewStatement(f, uint64(rt.opCount), rt.DebugFlag, outputVar, inputVars...)
}

func (rt *ReuseTracer) AppendNewStatement(f *OpDef, outputVar *Variable, inputVars ...*Variable) *Statement {
	s := rt.NewStatement(f, outputVar, inputVars...)
	rt.Statements = append(rt.Statements, s)
	if rt.DebugFlag {
		if len(rt.Statements) == 1 {
			rt.DebugOut("\n\nTx %v trace\n", rt.txHash.Hex())
		}
		PrintStatementsWithWriteOut([]*Statement{s}, func(fmtStr string, args ...interface{}) { rt.DebugOut(fmtStr, args...) })
	}
	return s
}

func (rt *ReuseTracer) ResizeMem(size uint64) {
	rt.Mem().Resize(size)
}

// The generic trace function
func (rt *ReuseTracer) Trace(f *OpDef, output interface{}, inputVars ...*Variable) *Variable {
	return rt.TraceWithName(f, output, "", inputVars...)
}

func (rt *ReuseTracer) TraceWithName(f *OpDef, output interface{}, outputVarNamePart string, inputVars ...*Variable) *Variable {
	if f.isStoreOp {
		if !f.IsLog() && !f.IsVirtual() {
			rt.world.TWStore(f.config.variant, inputVars...)
		}
		rt.AppendNewStatement(f, nil, inputVars...)
		return nil
	}

	cacheKey := ""

	if f.isLoadOp {
		outputVar := rt.world.TWLoad(f.config.variant, inputVars...)
		if outputVar != nil {
			return outputVar // skip read
		}
	} else if f.isReadOp {
		outputVar := rt.GetCachedHeaderRead(f.config.variant, inputVars...)
		if outputVar != nil {
			return outputVar
		}
	} else {
		if len(inputVars) == 0 {
			panic("No parameters!")
		}

		cb := strings.Builder{}
		keySize := (len(inputVars) + 1) * 4
		cb.Grow(keySize)
		cb.Write((*[4]byte)(unsafe.Pointer(&(f.id)))[:])
		for _, v := range inputVars {
			cb.Write((*[4]byte)(unsafe.Pointer(&(v.id)))[:])
		}
		cacheKey = cb.String()
		outputVar, ok := rt.cachedComputes[cacheKey]
		if ok {
			if rt.DebugFlag {
				if outputVar.originStatement != nil {
					rt.DebugOut("Use cached result of %v\n", outputVar.originStatement.SimpleNameString())
				} else {
					rt.DebugOut("Use cached const result of %v\n", outputVar.Name())
				}
				//inputs := ""
				//for _, v := range inputVars {
				//	inputs = inputs + v.Name() + ","
				//}
				//rt.DebugOut("  inputs: %v\n", inputs)
			}
			return outputVar
		}
	}

	if output == nil {
		//inputs := GetInputValues(inputVars, rt.execEnv.inputs[:0])
		env := rt.execEnv // &ExecEnv{inputs: inputs, state: rt.statedb, config: &(f.config), header: rt.header, getHash: rt.getHashFunc, precompiles: vm.GetPrecompiledMapping(&(rt.chainRules))}
		env.inputs = GetInputValues(inputVars, rt.execEnv.inputs[:0])
		env.config = &(f.config)
		output = f.impFuc(env)
		if output == nil {
			panic(fmt.Sprintf("do not get any output from %s", *(f.name)))
		}
	}

	name := InferBaseNameFromInputs(f, inputVars) + outputVarNamePart
	var outputVar *Variable
	// do not create constant statement
	if !f.isLoadOp && !f.isReadOp && !f.IsGuard() && IsAllConstants(inputVars) {
		outputVar = rt.ConstVarWithName(output, name)
		rt.NewStatement(f, outputVar, inputVars...)
	} else {
		outputVar = rt.VarWithName(output, name)
	}

	if f.isLoadOp {
		rt.world.SetLoadValue(f.config.variant, outputVar, inputVars...)
	} else if f.isReadOp {
		rt.SetCachedHeaderRead(f.config.variant, outputVar, inputVars...)
	} else if f.IsGuard() {
		outputVar.MarkGuardedConst()
	}

	/*
		else if f.IsGuard() {
			outputVar.MarkGuardedConst()
			outputVar.SetTypedVal(inputVars[0].tVal)
		}
	*/

	if !f.isLoadOp && !f.isReadOp { //&& (f.IsGuard() || !outputVar.IsConst()) {
		if cacheKey == "" {
			panic("Empty Cache Key!")
		}
		_, ok := rt.cachedComputes[cacheKey]
		if ok {
			panic("Already cached computes!")
		}
		rt.cachedComputes[cacheKey] = outputVar
	}

	if f.IsGuard() || !outputVar.IsConst() {
		rt.AppendNewStatement(f, outputVar, inputVars...)
	}
	return outputVar
}

// trace helpers

func (rt *ReuseTracer) TracePreCheck() {
	if rt.TraceLoadAndCheckNonce() {
		rt.TraceBuyGas()
	} else {
		rt.EncounterUnimplementedCode = true
	}
	return
}

func (rt *ReuseTracer) TraceLoadAndCheckNonce() bool {
	accountNonceVar := rt.CF().callerAddress.LoadNonce(nil).NGuard("nonce")
	return accountNonceVar.Uint64() == rt.txNonce_uint64.Uint64()
}

func (rt *ReuseTracer) TraceBuyGas() {
	fromVar := rt.CF().callerAddress
	gas := new(big.Int).SetUint64(rt.txGas_uint64.Uint64())
	gasPrice := rt.txGasPrice_BigInt.BigInt()
	maxGasCost := new(big.Int).Mul(gas, gasPrice)
	maxGasCostVar := rt.ConstVarWithName(maxGasCost, "maxGasCost")

	balanceVar := fromVar.LoadBalance(nil)
	result := balanceVar.GEBigInt(maxGasCostVar).NGuard("buygas")
	if !result.Bool() {
		rt.EncounterUnimplementedCode = true
		return
	}
	if maxGasCost.Sign() != 0 {
		afterBalanceVar := balanceVar.SubBigInt(maxGasCostVar)
		fromVar.StoreBalance(afterBalanceVar)
	}
}

func (rt *ReuseTracer) TraceIncCallerNonce() {
	fromVar := rt.CF().callerAddress
	fromVar.StoreNonce(fromVar.LoadNonce(nil).AddUint64(rt.Uint64_1))
}

func (rt *ReuseTracer) TraceCanTransfer() {
	cf := rt.CF()
	balanceVar := cf.callerAddress.LoadBalance(nil)
	balanceVar.GEBigInt(cf.value).NGuard("balance")
	return
}

func (rt *ReuseTracer) TraceAssertCodeAddrExistence() {
	rt.CF().codeAddress.LoadExist(nil).NGuard("account_exist")
}

func (rt *ReuseTracer) TraceGuardIsCodeAddressPrecompiled() {
	toVar := rt.CF().codeAddress
	if toVar == nil {
		// make sure this only happens in contract creation transaction
		if rt.txTo != nil {
			panic("this only happens in contract creation transaction")
		}
		return
	}
	gr := toVar.IsPrecompiled().NGuard("account_precompiled")
	if gr.Bool() {
		rt.CF().GuardCodeAddress()
	}
	//rt.CF().GuardCodeAddress()
}

// assert whether value == 0
func (rt *ReuseTracer) TraceAssertValueZeroness() {
	rt.TraceAssertBigIntZeroness(rt.CF().value)
}

func (rt *ReuseTracer) TraceTransfer() {
	cf := rt.CF()
	if cf.value.BigInt().Sign() == 0 {
		rt.TraceAssertValueZeroness()
		return
	}

	fromAfterBalance := cf.callerAddress.LoadBalance(nil).SubBigInt(cf.value)

	// from.balance -= value
	cf.callerAddress.StoreBalance(fromAfterBalance)
	// to.balance += value
	var toBeforeBalance *big.Int
	if cf.callerAddress.BAddress() == cf.contractAddress.BAddress() {
		toBeforeBalance = fromAfterBalance.BigInt()
	}
	cf.contractAddress.StoreBalance(cf.contractAddress.LoadBalance(toBeforeBalance).AddBigInt(cf.value))
	return
}

func (rt *ReuseTracer) TraceGuardCodeLen() {
	cf := rt.CF()
	// here we guard code hash instead to be more efficient
	cf.codeHash = cf.codeHash.NGuard("code_len")
	//	cf.codeAddress.LoadCodeHash(nil).Guard()
}

func (rt *ReuseTracer) TraceRefund(remaining *big.Int, gasUsed uint64) {
	defer rt.Trace(OP_StoreGasUsed, nil, rt.ConstVarWithName(gasUsed, "gasUsed"))
	remainingVar := rt.ConstVarWithName(remaining, "refund")
	balanceVar := rt.txFrom.LoadBalance(nil)
	if remaining.Sign() == 0 {
		return
	}
	afterBalanceVar := balanceVar.AddBigInt(remainingVar)
	rt.txFrom.StoreBalance(afterBalanceVar)
}

func (rt *ReuseTracer) CellsToInputs(arrayLenVar_BigInt *Variable, cells []*MemByteCell) []*Variable {

	inputs := make([]*Variable, 0, len(cells)/32+4)
	inputs = append(inputs, arrayLenVar_BigInt)
	if len(cells) == 0 {
		cmptypes.MyAssert(arrayLenVar_BigInt.BigInt().Sign() == 0)
		return inputs
	}
	//MyAssert(len(cells) > 0)

	GetCellVariableAndOffset := func(c *MemByteCell) (*Variable, uint64) {
		if c == nil {
			return rt.ByteArray_Empty, 0
		}
		return c.variable, c.offset
	}

	currentV, currentStartingOffset := GetCellVariableAndOffset(cells[0])
	currentCount := uint64(1)

	appendNewVariable := func() {
		if currentV == currentV.tracer.ByteArray_Empty {
			inputs = append(inputs, currentV, rt.ConstVar(uint64(0)), rt.ConstVar(currentCount))
		} else {
			inputs = append(inputs, currentV, rt.ConstVar(currentStartingOffset), rt.ConstVar(currentCount))
		}
	}

	for i := 1; i < len(cells); i++ {
		v, offset := GetCellVariableAndOffset(cells[i])
		if v == currentV {
			if v == v.tracer.ByteArray_Empty {
				currentCount++
				continue
			}
			if offset == currentStartingOffset+currentCount {
				currentCount++
				continue
			}
		}

		appendNewVariable()

		currentV = v
		currentStartingOffset = offset
		currentCount = 1
	}
	appendNewVariable()

	return inputs
}

func (rt *ReuseTracer) TraceMemoryRead(arrayLenVar_BigInt *Variable, cells []*MemByteCell) *Variable {
	arrayLen := arrayLenVar_BigInt.BigInt().Int64()
	if int64(len(cells)) != arrayLen {
		panic(fmt.Sprintf("Wrong inputs arrayLen %v, cellLen %v", arrayLen, len(cells)))
	}

	inputs := rt.CellsToInputs(arrayLenVar_BigInt, cells)

	singleCellVariable := false
	offsetFullCoverage := false
	if len(inputs) == 4 && inputs[1] != rt.ByteArray_Empty {
		singleCellVariable = true
	}
	if singleCellVariable {
		start := inputs[2].Uint64()
		if start == 0 {
			v := inputs[1]
			vLen := uint64(len(v.ByteArray()))
			count := inputs[3].Uint64()
			cmptypes.MyAssert(count == uint64(arrayLen))
			if count == vLen {
				offsetFullCoverage = true
			}
		}
	}

	var ret *Variable

	if arrayLen > 0 && singleCellVariable && offsetFullCoverage {
		ret = cells[0].variable
	} else {
		ret = rt.TraceWithName(OP_ConcatBytes, nil, "[]byte", inputs...)
		cmptypes.MyAssert(len(ret.ByteArray()) == len(cells))
		if !ret.IsConst() {
			if oldCells, ok := rt.byteArrayOriginCells[ret]; !ok {
				cellsCopy := make([]*MemByteCell, len(cells))
				copy(cellsCopy, cells)
				rt.byteArrayOriginCells[ret] = cellsCopy
			} else {
				cmptypes.MyAssert(len(oldCells) == len(cells))
				for i, oldCell := range oldCells {
					cell := cells[i]

					if oldCell == nil {
						cmptypes.MyAssert(cell == nil)
					} else {
						if oldCell.variable != cell.variable {
							cmptypes.MyAssert(oldCell.variable == cell.variable, "%v mismatch @%v: old var %v, var %v",
								ret.Name(), i, oldCell.variable.Name(), cell.variable.Name())
						}
						if oldCell.offset != cell.offset {
							cmptypes.MyAssert(oldCell.offset == cell.offset, "%v mismatch @%v %v: old offset %v, offset %v",
								ret.Name(), i, oldCell.variable.Name(), oldCell.offset, cell.offset)
						}
					}
				}
			}
		}
	}
	cmptypes.MyAssert(arrayLenVar_BigInt.IsConst())
	rt.byteArrayCachedSize[ret] = arrayLenVar_BigInt
	return ret
}

func (rt *ReuseTracer) TraceAssertBigIntZeroness(stackVar *Variable) {
	stackVar.EqualBigInt(rt.BigInt_0).NGuard("val_zero_check")
}

func (rt *ReuseTracer) TraceAssertStackValueZeroness() {
	rt.TraceAssertBigIntZeroness(rt.CF().stack.Back(2))
}

func (rt *ReuseTracer) TraceAssertStackValueSelf(backs ...int) {
	stack := rt.CF().stack
	for _, back := range backs {
		stackVar := stack.Back(back)
		stack.ReplaceBack(back, stackVar.NGuard("gas_valueself"))
	}
}

func (rt *ReuseTracer) traceSetReturnMemory() {
	cf := rt.CF()
	rt.Mem().Set(cf.retOffset, cf.retSize, rt.returnData)
}

func (rt *ReuseTracer) TraceCallReturn(err, reverted, nilOrEmptyReturnData bool) {
	rt.PopCallFrame()
	if err {
		rt.Push(rt.BigInt_0)
	} else {
		rt.Push(rt.BigInt_1)
	}
	if !err || reverted {
		rt.traceSetReturnMemory()
	}
	if nilOrEmptyReturnData {
		rt.returnData = rt.ByteArray_Empty
	}
}

func (rt *ReuseTracer) TraceCreateReturn(err, reverted bool) {
	cf := rt.PopCallFrame()
	if err {
		rt.Push(rt.BigInt_0)
	} else {
		rt.Push(cf.codeAddress)
	}

	if !reverted {
		rt.returnData = rt.ByteArray_Empty
	}
}

func (rt *ReuseTracer) TraceCreateAddressAndSetupCode() {
	cf := rt.CF()
	callerAddr := cf.callerAddress
	contractAddr := callerAddr.CreateAddress(callerAddr.LoadNonce(nil))
	cf.contractAddress = contractAddr
	cf.codeAddress = contractAddr
	rt.traceSetCreateCode()
}

func (rt *ReuseTracer) TraceCreateAddress2(salt *Variable) {
	cf := rt.CF()
	callerAddr := cf.callerAddress
	contractAddr := callerAddr.CreateAddress2(salt, cf.input)
	cf.contractAddress = contractAddr
	cf.codeAddress = contractAddr
	rt.traceSetCreateCode()
}

func (rt *ReuseTracer) traceSetCreateCode() {
	cf := rt.CF()
	cf.code = cf.input
	cf.codeHash = rt.EmptyHash
	cf.input = rt.ByteArray_Empty
}

func (rt *ReuseTracer) TraceLoadCodeAndGuardHash() {
	cf := rt.CF()
	// note to be efficient, we guard hash instead of code itself
	cf.codeHash = cf.codeAddress.LoadCodeHash(nil).NGuard("code")
	//cf.code = cf.codeAddress.LoadCode(nil).Guard()
	cf.code = rt.ConstVarWithName(rt.statedb.GetCode(cf.codeAddress.BAddress()), "Code")
}

func (rt *ReuseTracer) TraceAssertNoExistingContract() {
	codeHash := rt.CF().contractAddress.LoadCodeHash(nil)
	nonceVar := rt.CF().contractAddress.LoadNonce(nil)
	nonceVar.NGuard("account_exist_nonce")
	if nonceVar.Uint64() == 0 {
		codeHash.NGuard("code_exist")
	}
}

func (rt *ReuseTracer) TraceSetNonceForCreatedContract() {
	rt.CF().contractAddress.StoreNonce(rt.Uint64_1)
}

func (rt *ReuseTracer) TraceAssertNewCodeLen() {
	if !rt.statedb.HasSuicided(rt.CF().contractAddress.BAddress()) { // suicided no return data
		rt.returnData.LenByteArray().NGuard("code")
	}
}

func (rt *ReuseTracer) TraceCreateContractAccount() {
	rt.CF().contractAddress.StoreCodeHash(rt.EmptyCodeHash)
}

func (rt *ReuseTracer) TraceStoreNewCode() {
	// make sure to not record the code store if the contract is suicided during its creation
	if !rt.statedb.HasSuicided(rt.CF().contractAddress.BAddress()) {
		rt.CF().contractAddress.StoreCode(rt.returnData)
	}
}

func (rt *ReuseTracer) TraceAssertInputLen() {
	rt.CF().input.LenByteArray().NGuard("gas_inputlen")
}

func (rt *ReuseTracer) TraceGasBigModExp() {
	input := rt.CF().input
	inputLenVar := input.LenByteArray().NGuard("gas_bigmodexp")

	var (
		baseLen = input.GetDataBig(rt.BigInt_0, rt.BigInt_32).NGuard("gas").ByteArrayToBigInt()  //  new(big.Int).SetBytes(getData(input, 0, 32))
		expLen  = input.GetDataBig(rt.BigInt_32, rt.BigInt_32).NGuard("gas").ByteArrayToBigInt() // new(big.Int).SetBytes(getData(input, 32, 32))
		_       = input.GetDataBig(rt.BigInt_64, rt.BigInt_32).NGuard("gas").ByteArrayToBigInt() // modeLen = new(big.Int).SetBytes(getData(input, 64, 32))
	)

	inputLen := inputLenVar.BigInt().Int64()

	remainingInputLen := int64(0)
	if inputLen > int64(96) {
		remainingInputLen = inputLen - 96
	}

	// Retrieve the head 32 bytes of exp for the adjusted exponent length
	if big.NewInt(remainingInputLen).Cmp(baseLen.BigInt()) <= 0 {
		// expHead = new(big.Int)
	} else {
		if expLen.BigInt().Cmp(rt.BigInt_32.BigInt()) > 0 {
			input.GetDataBig(baseLen.AddBigInt(rt.BigInt_96), rt.BigInt_32).ByteArrayToBigInt().BitLenBigInt().NGuard("gas")
			//expHead = new(big.Int).SetBytes(getData(input, baseLen.Uint64(), 32))
		} else {
			input.GetDataBig(baseLen.AddBigInt(rt.BigInt_96), expLen).ByteArrayToBigInt().BitLenBigInt().NGuard("gas")
			//expHead = new(big.Int).SetBytes(getData(input, baseLen.Uint64(), expLen.Uint64()))
		}
	}
}

func (rt *ReuseTracer) TraceRunPrecompiled(p cmptypes.PrecompiledContract) {
	funcVar := rt.ConstVarWithName(p, reflect.TypeOf(p).String())
	out := rt.Trace(OP_RunPrecompiled, nil, rt.CF().input, funcVar)
	rt.returnData = out
}

func (rt *ReuseTracer) TraceGasSStore() {
	stack := rt.CF().stack
	yVar, xVar := stack.Back(1), stack.Back(0)
	keyVar := xVar
	currentVar := rt.CF().contractAddress.LoadState(keyVar, nil)
	//currentVar.EqualGeneric(rt.Hash_Empty).Guard()
	currentVar.EqualBigInt(rt.BigInt_0).NGuard("gas")
	yVar.EqualBigInt(rt.BigInt_0).NGuard("gas")
}

func (rt *ReuseTracer) TraceGasSStoreEIP2200() {
	stack := rt.CF().stack
	yVar, xVar := stack.Back(1), stack.Back(0)
	keyVar := xVar
	currentVar := rt.CF().contractAddress.LoadState(keyVar, nil) //.MarkBigIntAsHash()
	yValueHashVar := yVar                                        //.MarkBigIntAsHash()

	/*
		if current == value { // noop (1)
			return params.SstoreNoopGasEIP2200, nil
		}
	*/
	//cmpResult := currentVar.EqualGeneric(yValueHashVar)
	cmpResult := currentVar.EqualBigInt(yValueHashVar)
	cmpResult.NGuard("gas_store")
	if cmpResult.Bool() {
		return
	}

	originalVar := rt.CF().contractAddress.LoadCommittedState(keyVar, nil) //.MarkBigIntAsHash()
	/*
		if original == current {
			if original == (common.Hash{}) { // create slot (2.1.1)
				return params.SstoreInitGasEIP2200, nil
			}
			if value == (common.Hash{}) { // delete slot (2.1.2b)
				evm.StateDB.AddRefund(params.SstoreClearRefundEIP2200)
			}
			return params.SstoreCleanGasEIP2200, nil // write existing slot (2.1.2)
		}
	*/
	cmpResult = originalVar.EqualBigInt(currentVar)
	cmpResult.NGuard("gas_store")
	if cmpResult.Bool() {
		//cmpResult := originalVar.EqualGeneric(rt.Hash_Empty)
		cmpResult := originalVar.EqualBigInt(rt.BigInt_0)
		cmpResult.NGuard("gas_store")
		if cmpResult.Bool() {
			return
		}
		//yValueHashVar.EqualGeneric(rt.Hash_Empty).Guard()
		yValueHashVar.EqualBigInt(rt.BigInt_0).NGuard("gas_store")
		return
	}

	/*
		if original != (common.Hash{}) {
			if current == (common.Hash{}) { // recreate slot (2.2.1.1)
				evm.StateDB.SubRefund(params.SstoreClearRefundEIP2200)
			} else if value == (common.Hash{}) { // delete slot (2.2.1.2)
				evm.StateDB.AddRefund(params.SstoreClearRefundEIP2200)
			}
		}
	*/
	//cmpResult = originalVar.EqualGeneric(rt.Hash_Empty)
	cmpResult = originalVar.EqualBigInt(rt.BigInt_0)
	cmpResult.NGuard("gas_store")
	if !cmpResult.Bool() {
		//cmpResult := currentVar.EqualGeneric(rt.Hash_Empty)
		cmpResult := currentVar.EqualBigInt(rt.BigInt_0) //.EqualGeneric(rt.Hash_Empty)
		cmpResult.NGuard("gas_store")
		if !cmpResult.Bool() {
			//yValueHashVar.EqualGeneric(rt.Hash_Empty).Guard()
			yValueHashVar.EqualBigInt(rt.BigInt_0).NGuard("gas_store")
		}
	}
	/*
		if original == value {
			if original == (common.Hash{}) { // reset to original inexistent slot (2.2.2.1)
				evm.StateDB.AddRefund(params.SstoreInitRefundEIP2200)
			} else { // reset to original existing slot (2.2.2.2)
				evm.StateDB.AddRefund(params.SstoreCleanRefundEIP2200)
			}
		}
		return params.SstoreDirtyGasEIP2200, nil // dirty update (2.2)
	*/
	//cmpResult = originalVar.EqualGeneric(yValueHashVar)
	cmpResult = originalVar.EqualBigInt(yValueHashVar)
	cmpResult.NGuard("gas_store")
	if cmpResult.Bool() {
		//originalVar.EqualGeneric(rt.Hash_Empty).Guard()
		originalVar.EqualBigInt(rt.BigInt_0).NGuard("gas_store")
	}
}

func (rt *ReuseTracer) TraceAssertExpByteLen() {
	stack := rt.CF().stack
	stack.data[stack.len()-2].BitLenBigInt().NGuard("gas_expbytelen")
}

func (rt *ReuseTracer) TraceGasCall() {
	stack := rt.CF().stack
	valueVar := stack.Back(2)
	callGasVar := stack.Back(0)

	transfersValue := valueVar.EqualBigInt(rt.BigInt_0)
	transfersValue.NGuard("gas")

	if rt.chainRules.IsEIP158 {
		if !transfersValue.Bool() {
			addressVar := stack.Back(1)
			addressVar.LoadEmpty(nil).NGuard("gas")
		}
	} else {
		addressVar := stack.Back(1)
		addressVar.LoadExist(nil).NGuard("gas")
	}

	callGasVar.NGuard("gas")
}

func (rt *ReuseTracer) TraceGasCallCode() {
	stack := rt.CF().stack
	valueVar := stack.Back(2)
	callGasVar := stack.Back(0)

	valueVar.EqualBigInt(rt.BigInt_0).NGuard("gas")
	callGasVar.NGuard("gas")
}

func (rt *ReuseTracer) TraceGasDelegateOrStaticCall() {
	stack := rt.CF().stack
	callGasVar := stack.Back(0)
	callGasVar.NGuard("gas")
}

func (rt *ReuseTracer) TraceGasSelfdestruct() {
	stack := rt.CF().stack
	if rt.chainRules.IsEIP150 {
		addressVar := stack.Back(0)
		if rt.chainRules.IsEIP158 {
			g := addressVar.LoadEmpty(nil).NGuard("gas")
			if !g.Bool() {
				return
			}
			rt.CF().contractAddress.LoadBalance(nil).EqualBigInt(rt.BigInt_0).NGuard("gas")
		} else {
			addressVar.LoadExist(nil).NGuard("gas")
		}
	}
}

func (rt *ReuseTracer) DebugOut(fmtStr string, args ...interface{}) {
	rt.DebugBuffer.AppendLog(fmtStr, args...)
}

var EmptyValue = reflect.Value{}

func (rt *ReuseTracer) TraceOpByName(opName string, pc uint64, gas uint64) {
	if rt.DebugFlag {
		rt.DebugOut("%v: %v\n", rt.opCount, opName)
	}
	rt.opCount++
	rt.currentPC = pc
	rt.currentGas = gas
	if strings.Index(opName, "LOG") == 0 {
		size, err := strconv.Atoi(opName[3:])
		if err != nil {
			panic(fmt.Sprintf("Wrong LOG op name %v", opName))
		}
		rt.Trace_opLogN(size)
		return
	}

	if strings.Index(opName, "PUSH") == 0 {
		size, err := strconv.Atoi(opName[4:])
		if err != nil {
			panic(fmt.Sprintf("Wrong PUSH op name %v", opName))
		}
		if size == 1 {
			rt.Trace_opPush1()
		} else {
			rt.Trace_opPushN(size)
		}
		return
	}

	if strings.Index(opName, "DUP") == 0 {
		size, err := strconv.Atoi(opName[3:])
		if err != nil {
			panic(fmt.Sprintf("Wrong DUP op name %v", opName))
		}
		rt.Trace_opDupN(size)
		return
	}
	if strings.Index(opName, "SWAP") == 0 {
		size, err := strconv.Atoi(opName[4:])
		if err != nil {
			panic(fmt.Sprintf("Wrong SWAP op name %v", opName))
		}
		rt.Trace_opSwapN(size)
		return
	}

	name := strings.ToLower(opName)
	name = strings.ToUpper(name[0:1]) + name[1:]
	method := rt.GetTraceMethod(name)
	if method == EmptyValue {
		rt.MarkUnimplementedOp()
		panic(fmt.Sprintf("Op %v not implemented", name))
	}
	method.Call(make([]reflect.Value, 0))
}

func (rt *ReuseTracer) GetTraceMethod(name string) reflect.Value {
	if method, ok := rt.methodCache[name]; ok {
		return method
	} else {
		method = reflect.ValueOf(rt).MethodByName("Trace_op" + name)
		rt.methodCache[name] = method
		return method
	}
}

func (rt *ReuseTracer) traceOneOperandOp(op *OpDef) {
	xVar := rt.Pop()
	r := rt.Trace(op, nil, xVar)
	rt.Push(r)
}

func (rt *ReuseTracer) traceTwoOperandsOp(op *OpDef) *Variable {
	xVar, yVar := rt.Pop2()
	r := rt.Trace(op, nil, xVar, yVar)
	rt.Push(r)
	return r
}

func (rt *ReuseTracer) traceThreeOperandsOp(op *OpDef) {
	xVar, yVar, zVar := rt.Pop3()
	r := rt.Trace(op, nil, xVar, yVar, zVar)
	rt.Push(r)
}

func (rt *ReuseTracer) Trace_opAdd() {
	rt.traceTwoOperandsOp(OP_EVMAdd)
}

func (rt *ReuseTracer) Trace_opSub() {
	rt.traceTwoOperandsOp(OP_EVMSub)
}

func (rt *ReuseTracer) Trace_opMul() {
	rt.traceTwoOperandsOp(OP_EVMMul)
}

func (rt *ReuseTracer) Trace_opDiv() {
	rt.traceTwoOperandsOp(OP_EVMDiv)
}

func (rt *ReuseTracer) Trace_opSdiv() {
	rt.traceTwoOperandsOp(OP_EVMSdiv)
}

func (rt *ReuseTracer) Trace_opMod() {
	rt.traceTwoOperandsOp(OP_EVMMod)
}

func (rt *ReuseTracer) Trace_opSmod() {
	rt.traceTwoOperandsOp(OP_EVMSmod)
}

func (rt *ReuseTracer) Trace_opExp() {
	rt.traceTwoOperandsOp(OP_EVMExp)
}

func (rt *ReuseTracer) Trace_opSignextend() {
	rt.traceTwoOperandsOp(OP_EVMSignExtend)
}

func (rt *ReuseTracer) Trace_opNot() {
	rt.traceOneOperandOp(OP_EVMNot)
}

func (rt *ReuseTracer) Trace_opLt() {
	r := rt.traceTwoOperandsOp(OP_EVMLt)
	rt.isCompareOpOutput[r] = true
}

func (rt *ReuseTracer) Trace_opGt() {
	r := rt.traceTwoOperandsOp(OP_EVMGt)
	rt.isCompareOpOutput[r] = true
}

func (rt *ReuseTracer) Trace_opSlt() {
	r := rt.traceTwoOperandsOp(OP_EVMSlt)
	rt.isCompareOpOutput[r] = true
}

func (rt *ReuseTracer) Trace_opSgt() {
	r := rt.traceTwoOperandsOp(OP_EVMSgt)
	rt.isCompareOpOutput[r] = true
}

func (rt *ReuseTracer) Trace_opEq() {
	r := rt.traceTwoOperandsOp(OP_EVMEq)
	rt.isCompareOpOutput[r] = true
}

func (rt *ReuseTracer) Trace_opIszero() {
	v := rt.Pop()
	rt.Push(v.EVMIsZero())
	//	rt.traceOneOperandOp(OP_EVMIszero)
}

func (rt *ReuseTracer) Trace_opAnd() {
	rt.traceTwoOperandsOp(OP_EVMAnd)
}

func (rt *ReuseTracer) Trace_opOr() {
	rt.traceTwoOperandsOp(OP_EVMOr)
}

func (rt *ReuseTracer) Trace_opXor() {
	rt.traceTwoOperandsOp(OP_EVMXor)
}

func (rt *ReuseTracer) Trace_opByte() {
	rt.traceTwoOperandsOp(OP_EVMByte)
}

func (rt *ReuseTracer) Trace_opAddmod() {
	rt.traceThreeOperandsOp(OP_EVMAddmod)
}

func (rt *ReuseTracer) Trace_opMulmod() {
	rt.traceThreeOperandsOp(OP_EVMMulmod)
}

func (rt *ReuseTracer) Trace_opShl() {
	rt.traceTwoOperandsOp(OP_EVMSHL)
}

func (rt *ReuseTracer) Trace_opShr() {
	rt.traceTwoOperandsOp(OP_EVMSHR)
}

func (rt *ReuseTracer) Trace_opSar() {
	rt.traceTwoOperandsOp(OP_EVMSAR)
}

func (rt *ReuseTracer) Trace_opSha3() {
	offsetVar, sizeVar := rt.Pop2()
	mem := rt.CF().mem
	dataVar := mem.GetPtr(offsetVar, sizeVar)
	r := dataVar.Sha3()
	rt.Push(r)
}

func (rt *ReuseTracer) Trace_opAddress() {
	rt.Push(rt.CF().contractAddress)
}

func (rt *ReuseTracer) Trace_opBalance() {
	slot := rt.Pop()
	rt.Push(slot.LoadBalance(nil))
	//rt.Push(slot.LoadBalance(nil))
}

func (rt *ReuseTracer) Trace_opOrigin() {
	rt.Push(rt.txFrom)
}

func (rt *ReuseTracer) Trace_opCaller() {
	rt.Push(rt.CF().callerAddress)
}

func (rt *ReuseTracer) Trace_opCallvalue() {
	rt.Push(rt.CF().value)
}

func (rt *ReuseTracer) Trace_opCalldataload() {
	rt.Push(rt.CF().input.GetDataBig(rt.Pop(), rt.BigInt_32).ByteArrayToBigInt())
}

func (rt *ReuseTracer) Trace_opCalldatasize() {
	rt.Push(rt.CF().input.LenByteArray())
}

func (rt *ReuseTracer) Trace_opCalldatacopy() {
	memOffset, dataOffset, length := rt.Pop3()
	data := rt.CF().input.GetDataBig(dataOffset, length)
	rt.Mem().Set(memOffset, length, data)
}

func (rt *ReuseTracer) Trace_opReturndatasize() {
	rt.Push(rt.returnData.LenByteArray())
}

func (rt *ReuseTracer) Trace_opReturndatacopy() {
	memOffset, dataOffset, length := rt.Pop3()
	end := dataOffset.AddBigInt(length)
	checkPassed := rt.returnData.ArrayBoundCheck(end)
	checkPassed.NGuard("return_data_bound")
	if !checkPassed.Bool() {
		return
	}
	//condition := end.IsUint64BigInt()
	//condition.AssertBoolSelf()
	//if !condition.Bool() {
	//	// Error return data out of bounds
	//	return
	//}
	//// len(lenData) < end
	//condition = rt.returnData.LenByteArray().CmpBigInt(end).LessInt(rt.Int_0)
	//condition.AssertBoolSelf()
	//if condition.Bool() {
	//	// Error return data out of bounds
	//	return
	//}

	//rt.Mem().Set(memOffset, length, rt.returnData.SliceByteArray(dataOffset, end))
	rt.Mem().Set(memOffset, length, rt.returnData.GetDataBig(dataOffset, length))
}

func (rt *ReuseTracer) Trace_opExtcodesize() {
	rt.Push(rt.Pop().LoadCodeSize(nil))
}

func (rt *ReuseTracer) Trace_opCodesize() {
	rt.Push(rt.ConstVarWithName(big.NewInt(int64(len(rt.GetCurrentCode()))), "codeSze"))
	//l := interpreter.intPool.get().SetInt64(int64(len(contract.Code)))
}

func (rt *ReuseTracer) Trace_opCodecopy() {
	memOffset, codeOffset, length := rt.Pop3()

	codeCopy := rt.CF().code.GetDataBig(codeOffset, length)
	rt.Mem().Set(memOffset, length, codeCopy)
}

func (rt *ReuseTracer) Trace_opExtcodecopy() {
	addr := rt.Pop()
	memOffset, codeOffset, length := rt.Pop3()
	codeCopy := addr.LoadCode(nil).GetDataBig(codeOffset, length)
	rt.Mem().Set(memOffset, length, codeCopy)
}

func (rt *ReuseTracer) Trace_opExtcodehash() {
	addr := rt.Pop()
	emptyCondition := addr.LoadEmpty(nil)
	emptyCondition.NGuard("account_ext_empty")
	if emptyCondition.Bool() {
		rt.Push(rt.BigInt_0)
	} else {
		rt.Push(addr.LoadCodeHash(nil))
	}
}

func (rt *ReuseTracer) Trace_opGasprice() {
	rt.Push(rt.txGasPrice_BigInt)
}

func (rt *ReuseTracer) Trace_opBlockhash() {
	num := rt.Pop() //.NGuard("block_num")
	name := fmt.Sprintf("BlockHash%v", num.BigInt().String())
	r := rt.TraceWithName(OP_ReadBlockHash, nil, name, num)
	rt.Push(r)
}

func (rt *ReuseTracer) Trace_opCoinbase() {
	op := OP_ReadCoinbase
	name := *op.config.variant
	r := rt.TraceWithName(op, nil, name)
	rt.Push(r)
}

func (rt *ReuseTracer) Trace_opTimestamp() {
	op := OP_ReadTimestamp
	name := *op.config.variant
	rt.Push(rt.TraceWithName(op, nil, name))
}

func (rt *ReuseTracer) Trace_opNumber() {
	op := OP_ReadBlockNumber
	name := *op.config.variant
	rt.Push(rt.TraceWithName(op, nil, name))
}

func (rt *ReuseTracer) Trace_opDifficulty() {
	op := OP_ReadDifficulty
	name := *op.config.variant
	rt.Push(rt.TraceWithName(op, nil, name))
}

func (rt *ReuseTracer) Trace_opGaslimit() {
	op := OP_ReadGasLimit
	name := *op.config.variant
	rt.Push(rt.TraceWithName(op, nil, name))
}

func (rt *ReuseTracer) Trace_opPop() {
	rt.Pop()
}

var byteArrayType = reflect.TypeOf([]byte{0})

func (rt *ReuseTracer) Trace_opMload() {
	offset := rt.Pop()
	r := rt.Mem().GetPtrForMLoad(offset, rt.BigInt_32)
	if r.varType == byteArrayType {
		r = r.ByteArrayToBigInt()
	}
	r.BigInt() // check to make sure it is a bigint
	rt.Push(r)
}

func (rt *ReuseTracer) Trace_opMstore() {
	// pop value of the stack
	mStart, val := rt.Pop2()
	rt.Mem().Set32(mStart, val)
}

func (rt *ReuseTracer) Trace_opMstore8() {
	off, val := rt.Pop2()
	byteArrayVar := val.LowestByteBigInt()
	rt.Mem().Set(off, rt.BigInt_1, byteArrayVar)
}

func (rt *ReuseTracer) Trace_opSload() {
	loc := rt.Pop()
	val := rt.CF().contractAddress.LoadState(loc, nil)
	rt.Push(val)
}

func (rt *ReuseTracer) Trace_opSstore() {
	loc, val := rt.Pop2()
	rt.CF().contractAddress.StoreState(loc, val)
}

func (rt *ReuseTracer) Trace_opJump() {
	pos := rt.Pop()
	pos.NGuard("jump_pos")
}

func (rt *ReuseTracer) Trace_opJumpi() {
	pos, cond := rt.Pop2()
	r := cond.IszeroBigInt().NGuard("jump_cond")
	//r := cond.EqualBigInt(rt.BigInt_0)
	//r.Guard()
	if r.BigInt().Uint64() == 0 { //!r.Bool() { // cond != 0
		pos.NGuard("jump_pos")
	}
}

func (rt *ReuseTracer) Trace_opJumpdest() {
	// do nothing
}

func (rt *ReuseTracer) Trace_opPc() {
	rt.Push(rt.ConstVarWithName(new(big.Int).SetUint64(rt.currentPC), "PC"))
}

func (rt *ReuseTracer) Trace_opMsize() {
	rt.Push(rt.ConstVarWithName(new(big.Int).SetInt64(int64(rt.Mem().Len())), "msize"))
}

func (rt *ReuseTracer) Trace_opGas() {
	rt.Push(rt.ConstVarWithName(new(big.Int).SetUint64(rt.currentGas), "txGasLimit"))
}

func (rt *ReuseTracer) Trace_opCreate() {
	value, offset, size := rt.Pop3()
	input := rt.Mem().GetCopy(offset, size)

	cf := rt.CF()

	rt.StartNewCall(cf.contractAddress, nil, nil, value, input)
}

func (rt *ReuseTracer) Trace_opCreate2() {
	var (
		endowment    = rt.Pop()
		offset, size = rt.Pop2()
		salt         = rt.Pop()
		input        = rt.Mem().GetCopy(offset, size)
	)

	cf := rt.CF()

	rt.StartNewCall(cf.contractAddress, nil, nil, endowment, input)
	rt.TraceCreateAddress2(salt)
}

func (rt *ReuseTracer) Trace_opCall() {
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	rt.Pop() // Pop gas
	// Pop other call parameters.
	addr, value, inOffset := rt.Pop3()
	inSize, retOffset, retSize := rt.Pop3()
	toAddr := addr.CropBigIntAddress()
	value = value.U256BigInt()
	args := rt.Mem().GetPtr(inOffset, inSize)
	value.EqualBigInt(rt.BigInt_0).NGuard("call_gas")

	//save return offset and return Size before doing call
	cf := rt.CF()
	cf.retOffset = retOffset
	cf.retSize = retSize

	// push new call stack
	rt.StartNewCall(cf.contractAddress, toAddr, toAddr, value, args)
}

func (rt *ReuseTracer) Trace_opCallcode() {
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	rt.Pop() // Pop gas
	// Pop other call parameters.
	addr, value, inOffset := rt.Pop3()
	inSize, retOffset, retSize := rt.Pop3()
	toAddr := addr.CropBigIntAddress()
	value = value.U256BigInt()
	args := rt.Mem().GetPtr(inOffset, inSize)
	value.EqualBigInt(rt.BigInt_0).NGuard("call_gas")

	//save return offset and return Size before doing call
	cf := rt.CF()
	cf.retOffset = retOffset
	cf.retSize = retSize

	// push new call stack
	rt.StartNewCall(cf.contractAddress, cf.contractAddress, toAddr, value, args)
}

func (rt *ReuseTracer) Trace_opDelegatecall() {
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	rt.Pop() // Pop gas
	// Pop other call parameters.
	addr, inOffset, inSize := rt.Pop3()
	retOffset, retSize := rt.Pop2()
	toAddr := addr.CropBigIntAddress()
	args := rt.Mem().GetPtr(inOffset, inSize)

	//save return offset and return Size before doing call
	cf := rt.CF()
	cf.retOffset = retOffset
	cf.retSize = retSize

	// push new call stack
	rt.StartNewCall(cf.callerAddress, cf.contractAddress, toAddr, cf.value, args)
}

func (rt *ReuseTracer) Trace_opStaticcall() {
	// Pop gas. The actual gas is in interpreter.evm.callGasTemp.
	rt.Pop() // Pop gas
	// Pop other call parameters.
	addr, inOffset, inSize := rt.Pop3()
	retOffset, retSize := rt.Pop2()
	toAddr := addr.CropBigIntAddress() //.NGuard("call_target")
	args := rt.Mem().GetPtr(inOffset, inSize)

	//save return offset and return Size before doing call
	cf := rt.CF()
	cf.retOffset = retOffset
	cf.retSize = retSize

	// push new call stack
	rt.StartNewCall(cf.contractAddress, toAddr, toAddr, rt.BigInt_0, args)
}

func (rt *ReuseTracer) Trace_opReturn() {
	offset, size := rt.Pop2()
	ret := rt.Mem().GetPtr(offset, size)
	rt.returnData = ret
}

func (rt *ReuseTracer) Trace_opRevert() {
	rt.Trace_opReturn()
}

func (rt *ReuseTracer) Trace_opStop() {
	rt.returnData = rt.ByteArray_Empty
}

func (rt *ReuseTracer) Trace_opSelfdestruct() {
	suicider := rt.CF().contractAddress
	toAddr := rt.Pop()
	balance := suicider.LoadBalance(nil)
	if balance.BigInt().Sign() != 0 {
		toAddr.StoreBalance(toAddr.LoadBalance(nil).AddBigInt(balance))
	}

	suicider.StoreSuicide()
}

func (rt *ReuseTracer) Trace_opLogN(size int) {
	topics := make([]*Variable, size)
	mStart, mSize := rt.Pop2()
	for i := 0; i < size; i++ {
		topics[i] = rt.Pop() //.MarkBigIntAsHash()
	}

	d := rt.Mem().GetCopy(mStart, mSize)
	rt.CF().contractAddress.StoreLog(topics, d)
}

func (rt *ReuseTracer) GetCurrentCode() []byte {
	cf := rt.CF()
	if cf.codeCached == nil {
		cf.codeCached = cf.code.ByteArray()
	}
	return cf.codeCached
}

func (rt *ReuseTracer) Trace_opPush1() {
	code := rt.GetCurrentCode()
	var (
		codeLen = uint64(len(code))
	)

	pc := rt.currentPC + 1
	if pc < codeLen {
		rt.Push(rt.ConstVarWithName(new(big.Int).SetUint64(uint64(code[pc])), "push1_"+strconv.Itoa(int(pc))))
	} else {
		rt.Push(rt.BigInt_0)
	}
}

func (rt *ReuseTracer) Trace_opPushN(pushByteSize int) {
	code := rt.GetCurrentCode()
	var (
		codeLen = len(code)
	)

	pc := int(rt.currentPC + 1)
	startMin := codeLen
	if pc < startMin {
		startMin = pc
	}

	endMin := codeLen
	if startMin+pushByteSize < endMin {
		endMin = startMin + pushByteSize
	}

	rBytes := common.RightPadBytes(code[startMin:endMin], pushByteSize)
	rVar := rt.ConstVarWithName(new(big.Int).SetBytes(rBytes), "push"+strconv.Itoa(pushByteSize)+"_"+strconv.Itoa(pc))
	rt.Push(rVar)
}

func (rt *ReuseTracer) Trace_opDupN(size int) {
	rt.CF().stack.dup(size)
}

func (rt *ReuseTracer) Trace_opSwapN(size int) {
	rt.CF().stack.swap(size + 1)
}

func (rt *ReuseTracer) Trace_opSelfbalance() {
	rt.Push(rt.CF().contractAddress.LoadBalance(nil))
}

func (rt *ReuseTracer) Trace_opChainid() {
	rt.Push(rt.ChainID)
}

func (rt *ReuseTracer) GetCachedHeaderRead(variant StringID, vars ...*Variable) *Variable {
	key := *variant
	if variant == BLOCK_HASH {
		rt.guardBlockHashNum(vars[0])
		key += vars[0].BigInt().String()
	}
	return rt.cachedHeaderRead[key]
}

func (rt *ReuseTracer) SetCachedHeaderRead(variant StringID, outputVar *Variable, vars ...*Variable) {
	key := *variant
	if variant == BLOCK_HASH {
		rt.guardBlockHashNum(vars[0])
		key += vars[0].BigInt().String()
	}
	if rt.cachedHeaderRead[key] != nil {
		panic("Should never set the cache twice!")
	}
	rt.cachedHeaderRead[key] = outputVar
}

func (rt *ReuseTracer) guardBlockHashNum(numVar *Variable) {
	if rt.guardedBlockHashNum[numVar] {
		return
	} else {
		rt.guardedBlockHashNum[numVar] = true
	}

	num := numVar.BigInt().Uint64()

	if rt.hasNonConstBlockHashNum {
		ak := rt.blockHashNumIDMVar.GetBlockHashNumID(numVar).NGuard("block_num")
		if ak.Uint32() == 0 {
			rt.blockHashNumIDMVar = rt.blockHashNumIDMVar.SetBlockHashNumID(numVar, numVar.tracer.ConstVarWithName(numVar.id, "nID"+strconv.Itoa(int(numVar.id))))
		}
	} else {
		if numVar.IsConst() {
			if _, ok := rt.blockHashNumIDs.mapping[num]; !ok {
				rt.blockHashNumIDs.mapping[num] = numVar.id
			}
		} else {
			cmptypes.MyAssert(rt.blockHashNumIDMVar == nil)
			rt.blockHashNumIDMVar = numVar.tracer.ConstVarWithName(rt.blockHashNumIDs, "initNIDM")
			ak := rt.blockHashNumIDMVar.GetBlockHashNumID(numVar).NGuard("block_num")
			if ak.Uint32() == 0 {
				rt.blockHashNumIDMVar = rt.blockHashNumIDMVar.SetBlockHashNumID(numVar, numVar.tracer.ConstVarWithName(numVar.id, "nID"+strconv.Itoa(int(numVar.id))))
			}
			rt.hasNonConstBlockHashNum = true
		}
	}

}
