package cmpreuse

import (
	"fmt"
	"github.com/ethereum/go-ethereum/cmpreuse/cmptypes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"math/big"
	"math/bits"
	"reflect"
	"runtime"
	"strconv"
	"strings"
)

type StringID *string

var internedStrings = make(map[string]StringID)

func ToStringID(s string) StringID {
	interned, ok := internedStrings[s]
	if !ok {
		internedStrings[s] = &s
		interned = &s
	}
	return interned
}

func GetFuncNameStr(f interface{}) string {
	name := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	parts := strings.Split(name, ".")
	name = parts[len(parts)-1]
	if name[0] == 'f' {
		name = name[1:]
	}
	return name
}

func GetFuncName(f interface{}) StringID {
	name := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	return ToStringID(name)
}

type Variable struct {
	varType         reflect.Type
	val             interface{}
	originStatement *Statement
	constant        bool
	guardedConstant bool
	id              uint32
	customNamePart  string
	tracer          *ReuseTracer
	tVal            interface{} // hold the original value when we convert it to big.Int for evm internal processing
	cachedName      string
}

func (v *Variable) MarkGuardedConst() {
	v.guardedConstant = true
	v.cachedName = ""
}

func (v *Variable) MarkConst() {
	v.constant = true
}

func (v *Variable) IsConst() bool {
	return v.constant || v.guardedConstant
}

func (v *Variable) IsPureConst() bool {
	return v.constant
}

func (v *Variable) SetTypedVal(tv interface{}) *Variable {
	v.tVal = tv
	return v
}

func (v *Variable) Copy() *Variable {
	return &Variable{
		varType:         v.varType,
		val:             v.val,
		originStatement: nil,
		constant:        v.constant,
		guardedConstant: v.guardedConstant,
		id:              v.id,
		customNamePart:  v.customNamePart,
		tracer:          v.tracer,
		tVal:            v.tVal,
	}
}

func (v *Variable) Name() string {
	if v.cachedName != "" {
		return v.cachedName
	}
	var prefix string
	if v.constant {
		prefix = "c"
	} else if v.guardedConstant {
		prefix = "g"
	} else {
		prefix = "v"
	}

	name := fmt.Sprintf("%s%d", prefix, v.id)
	if v.customNamePart != "" {
		name = name + "_" + v.customNamePart
	}

	v.cachedName = name

	return name
}

func LimitStringLength(vs string, lenLimit int) string {
	r := vs
	if len(vs) > lenLimit {
		firstHalfLen := lenLimit / 2
		secondHalfLen := lenLimit - firstHalfLen
		r = vs[:firstHalfLen] + " ... " + vs[len(vs)-secondHalfLen-1:]
	}
	return r
}

func GetValueString(val interface{}) string {
	vt := reflect.TypeOf(val)
	//parts := strings.Split(vt.Name(), ".")
	//typeName := parts[len(parts)-1]
	typeName := vt.String()
	valueString := ""
	switch typedValue := val.(type) {
	case common.Address:
		valueString = typedValue.Hex()
	case common.Hash:
		valueString = typedValue.Hex()
	case *big.Int:
		valueString = typedValue.String()
	case *StateIDM:
		valueString = fmt.Sprintf("SID{%v}", len(typedValue.mapping))
	case *AddrIDM:
		valueString = fmt.Sprintf("AID{%v}", len(typedValue.mapping))
	case *BlockHashNumIDM:
		valueString = fmt.Sprintf("NID{%v}", len(typedValue.mapping))
	default:
		valueString = fmt.Sprint(typedValue)
	}
	return LimitStringLength(valueString, 200) + "[" + typeName + "]"
}

func (v *Variable) SimpleValueString() string {
	val := v.val
	if v.tVal != nil {
		val = v.tVal
	}
	return GetValueString(val)
}

func (v *Variable) BigInt() *big.Int {
	return v.val.(*big.Int)
}

func (v *Variable) Bool() bool {
	return v.val.(bool)
}

func (v *Variable) BAddress() common.Address {
	return common.BigToAddress(v.val.(*big.Int))
}

func (v *Variable) Int() int {
	return v.val.(int)
}

func (v *Variable) Uint64() uint64 {
	return v.val.(uint64)
}

func (v *Variable) Int64() int64 {
	return v.val.(int64)
}

func (v *Variable) LoadBalance(result *big.Int) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	name := "Balance"
	if result == nil {
		return v.tracer.TraceWithName(OP_LoadBalance, nil, name, v)
	}
	return v.tracer.TraceWithName(OP_LoadBalance, result, name, v)
}

func (v *Variable) LoadNonce(result *uint64) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	name := "Nonce"
	if result == nil {
		return v.tracer.TraceWithName(OP_LoadNonce, nil, name, v)
	}

	return v.tracer.TraceWithName(OP_LoadNonce, *result, name, v)
}

func (v *Variable) LoadExist(result *bool) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	name := "Exist"
	if result == nil {
		return v.tracer.TraceWithName(OP_LoadExist, nil, name, v)
	}
	return v.tracer.TraceWithName(OP_LoadExist, *result, name, v)
}

func (v *Variable) LoadEmpty(result *bool) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	name := "Empty"
	if result == nil {
		return v.tracer.TraceWithName(OP_LoadEmpty, nil, name, v)
	}
	return v.tracer.TraceWithName(OP_LoadEmpty, *result, name, v)
}

func (v *Variable) LoadCodeHash(result *common.Hash) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	name := "CodeHash"
	if result == nil {
		return v.tracer.TraceWithName(OP_LoadCodeHash, nil, name, v).MarkBigIntAsHash()
	}
	return v.tracer.TraceWithName(OP_LoadCodeHash, *result, name, v).MarkBigIntAsHash()
}

func (v *Variable) LoadCodeSize(result *int) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	// artifically add a code hash load to cross check with rwrecord
	codeHash := v.LoadCodeHash(nil)
	// after guarding code hash we can directly
	//codeSize := v.tracer.statedb.GetCodeSize(v.Address())
	//return v.tracer.ConstVarWithName(codeSize, "codeSize")
	//
	//
	name := "CodeSize"
	if result == nil {
		return v.tracer.TraceWithName(OP_LoadCodeSize, nil, name, v, codeHash)
	}
	return v.tracer.TraceWithName(OP_LoadCodeSize, *result, name, v, codeHash)
}

func (v *Variable) IntToBigInt() *Variable {
	return v.tracer.Trace(OP_IntToBigInt, nil, v)
}

func (v *Variable) LoadCode(result []byte) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	// artifically add a code hash load to cross check with rwrecord
	codeHash := v.LoadCodeHash(nil)

	name := "Code"
	if result == nil {
		return v.tracer.TraceWithName(OP_LoadCode, nil, name, v, codeHash)
	}
	return v.tracer.TraceWithName(OP_LoadCode, result, name, v, codeHash)
}

func (v *Variable) LoadState(key *Variable, result *common.Hash) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	//key = key.NGuard("storage_key") // make sure we read the same loc
	name := "state"
	if result == nil {
		return v.tracer.TraceWithName(OP_LoadState, nil, name, v, key)
	}
	return v.tracer.TraceWithName(OP_LoadState, *result, name, v, key)
}

func (v *Variable) LoadCommittedState(key *Variable, result *common.Hash) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	//key = key.NGuard("storage_loc")
	if result == nil {
		return v.tracer.Trace(OP_LoadCommittedState, nil, v, key)
	}
	return v.tracer.Trace(OP_LoadCommittedState, *result, v, key)
}

func (v *Variable) StoreBalance(balanceVar *Variable) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	v.tracer.Trace(OP_StoreBalance, nil, v, balanceVar)
	statedb := v.tracer.statedb
	addr := v.BAddress()

	v.tracer.world.TWStore(ACCOUNT_EXIST, v, v.tracer.Bool_true)

	// Empty
	if balanceVar.BigInt().Sign() != 0 || statedb.OriginalGetNonce(addr) != 0 || statedb.OriginalGetCodeHash(addr) != cmptypes.EmptyCodeHash {
		v.tracer.world.TWStore(ACCOUNT_EMPTY, v, v.tracer.Bool_false)
	} else { // balance equal zero
		v.tracer.world.TWStore(ACCOUNT_EMPTY, v, v.tracer.Bool_true)
	}
	return nil
}

func (v *Variable) StoreNonce(nonceVar *Variable) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	v.tracer.Trace(OP_StoreNonce, nil, v, nonceVar)

	v.tracer.world.TWStore(ACCOUNT_EXIST, v, v.tracer.Bool_true)

	// Empty
	if nonceVar.Uint64() > 0 {
		v.tracer.world.TWStore(ACCOUNT_EMPTY, v, v.tracer.Bool_false)
	} else { // nonce equal zero
		panic("Setting nonce to 0 should never happen")
		//if statedb.GetNonce(addr) > 0 && !statedb.Empty(addr) {
		//	if statedb.GetBalance(addr).Sign() == 0 && statedb.GetCodeHash(addr) == emptyCodeHash {
		//		v.StoreEmpty(v.tracer.Bool_true)
		//	}
		//}
	}
	return nil
}

func (v *Variable) StoreCode(codeVar *Variable) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	v.tracer.Trace(OP_StoreCode, nil, v, codeVar)

	// Exist
	v.tracer.world.TWStore(ACCOUNT_EXIST, v, v.tracer.Bool_true)

	statedb := v.tracer.statedb
	addr := v.BAddress()

	newLen := len(codeVar.ByteArray())
	// Empty
	if newLen > 0 || statedb.OriginalGetBalance(addr).Sign() != 0 || statedb.OriginalGetNonce(addr) != 0 {
		v.tracer.world.TWStore(ACCOUNT_EMPTY, v, v.tracer.Bool_false)
	} else { // balance equal zero
		v.tracer.world.TWStore(ACCOUNT_EMPTY, v, v.tracer.Bool_true)
	}

	//newLen := len(codeVar.ByteArray())
	// CodeSize and CodeHash
	v.tracer.world.TWStore(ACCOUNT_CODESIZE, v, v.tracer.ConstVarWithName(new(big.Int).SetInt64(int64(newLen)), "codeSize"))
	newHash := crypto.Keccak256Hash(codeVar.ByteArray()).Big()
	v.tracer.world.TWStore(ACCOUNT_CODEHASH, v, v.tracer.ConstVarWithName(newHash, "codeHash").MarkBigIntAsHash())

	return nil
}

func (v *Variable) StoreSuicide() *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	return v.tracer.Trace(OP_StoreSuicide, nil, v)
}

func (v *Variable) StoreLog(topics []*Variable, data *Variable) *Variable {
	inputs := make([]*Variable, 0, len(topics)+2)
	inputs = append(inputs, v)
	inputs = append(inputs, data)
	inputs = append(inputs, topics...)
	return v.tracer.Trace(OP_StoreLog, nil, inputs...)
}

func (v *Variable) StoreState(key *Variable, value *Variable) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	//key = key.NGuard("storage_key")
	return v.tracer.Trace(OP_StoreState, nil, v, key, value)
}

func (v *Variable) StoreEmpty(empty *Variable) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	return v.tracer.Trace(OP_StoreEmpty, nil, v, empty)
}

func (v *Variable) StoreExist(exist *Variable) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	return v.tracer.Trace(OP_StoreExist, nil, v, exist)
}

func (v *Variable) StoreCodeHash(codeHash *Variable) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	return v.tracer.Trace(OP_StoreCodeHash, nil, v, codeHash)
}

func (v *Variable) StoreCodeSize(codeSize *Variable) *Variable {
	//v = v.MarkBigIntAsAddress().NGuard("account_addr")
	return v.tracer.Trace(OP_StoreCodeSize, nil, v, codeSize)
}

func (v *Variable) GEBigInt(rhs *Variable) *Variable {
	return v.tracer.Trace(OP_GEBigInt, nil, v, rhs)
}

func (v *Variable) EqualBigInt(rhs *Variable) *Variable {
	if v == rhs {
		return v.tracer.Bool_true
	}
	return v.tracer.Trace(OP_EqualBigInt, nil, v, rhs)
}

func (v *Variable) SubBigInt(rhs *Variable) *Variable {
	return v.tracer.Trace(OP_SubBigInt, nil, v, rhs)
}

func (v *Variable) AddBigInt(rhs *Variable) *Variable {
	return v.tracer.Trace(OP_AddBigInt, nil, v, rhs)
}

func (v *Variable) MulBigInt(rhs *Variable) *Variable {
	return v.tracer.Trace(OP_MulBigInt, nil, v, rhs)
}

func (v *Variable) DivBigInt(rhs *Variable) *Variable {
	return v.tracer.Trace(OP_DivBigInt, nil, v, rhs)
}

func (v *Variable) CmpBigInt(rhs *Variable) *Variable {
	return v.tracer.Trace(OP_CmpBigInt, nil, v, rhs)
}

func (v *Variable) LessInt(rhs *Variable) *Variable {
	return v.tracer.Trace(OP_LessInt, nil, v, rhs)
}

func (v *Variable) IszeroBigInt() *Variable {
	t := v.tracer
	if ret, ok := t.isZeroCachedResult[v]; ok {
		return ret
	}
	return v.tracer.Trace(OP_IszeroBigInt, nil, v)
}

func (v *Variable) EVMIsZero() *Variable {
	t := v.tracer
	if ret, ok := t.isZeroCachedResult[v]; ok {
		return ret
	}
	out := v.tracer.Trace(OP_EVMIszero, nil, v)
	t.isCompareOpOutput[out] = true
	if t.isCompareOpOutput[v] {
		t.isZeroCachedResult[out] = v
	}
	return out
}

func (v *Variable) EqualGeneric(rhs *Variable) *Variable {
	return v.tracer.Trace(OP_EqualGeneric, nil, v, rhs)
}

func (v *Variable) AddUint64(rhs *Variable) *Variable {
	return v.tracer.Trace(OP_AddUint64, nil, v, rhs)
}

//func (v *Variable) MarkBigIntAsHash() *Variable {
//	return v.tracer.Trace(OP_BigIntToHash, nil, v)
//}

func (v *Variable) MarkBigIntAsHash() *Variable {
	hash := v.BHash()
	return v.SetTypedVal(hash)
	//return v.tracer.Trace(OP_HashToBigInt, nil, v)
}

func (v *Variable) BitLenBigInt() *Variable {
	return v.tracer.Trace(OP_BitLenBigInt, nil, v)
}

func (v *Variable) MarkBigIntAsAddress() *Variable {
	addr := common.BigToAddress(v.BigInt())
	return v.SetTypedVal(addr)
	//return v.tracer.Trace(OP_BigIntToAddress, nil, v)
}

//func (v *Variable) MarkBigIntAsAddress() *Variable {
//	return v.tracer.Trace(OP_AddressToBigInt, nil, v)
//}

func (v *Variable) ByteArrayToBigInt() *Variable {
	t := v.tracer
	if bigIntVar, ok := t.byte32VarToBigIntVar[v]; ok {
		return bigIntVar
	}
	bigIntVar := v.tracer.TraceWithName(OP_ByteArrayToBigInt, nil, "_a2BI", v)
	if len(v.ByteArray()) == 32 {
		t.bigIntVarToByte32Var[bigIntVar] = v
	}
	return bigIntVar
}

func (v *Variable) BigIntTo32Bytes() *Variable {
	t := v.tracer
	if byte32Var, ok := t.bigIntVarToByte32Var[v]; ok {
		return byte32Var
	}
	byte32Var := v.tracer.TraceWithName(OP_BigIntTo32Bytes, nil, "_32Bytes", v)
	t.byte32VarToBigIntVar[byte32Var] = v
	return byte32Var
}

func (v *Variable) U256BigInt() *Variable {
	return v.tracer.Trace(OP_U256BigInt, nil, v)
}

func (v *Variable) CropBigIntAddress() *Variable {
	return v.tracer.Trace(OP_CropBigIntAddress, nil, v)
}

func (v *Variable) S256BigInt() *Variable {
	return v.tracer.Trace(OP_S256BigInt, nil, v)
}

func (v *Variable) SignBigInt() *Variable {
	return v.tracer.Trace(OP_SignBigInt, nil, v)
}

func (v *Variable) NegBigInt() *Variable {
	return v.tracer.Trace(OP_NegBigInt, nil, v)
}

func (v *Variable) AbsBigInt() *Variable {
	return v.tracer.Trace(OP_AbsBigInt, nil, v)
}

func (v *Variable) IsUint64BigInt() *Variable {
	return v.tracer.Trace(OP_IsUint64BigInt, nil, v)
}

func (v *Variable) LowestByteBigInt() *Variable {
	return v.tracer.Trace(OP_LowestByteBigInt, nil, v)
}

func (v *Variable) Sha3() *Variable {
	return v.tracer.Trace(OP_Sha3, nil, v)
}

func (v *Variable) IsPrecompiled() *Variable {
	return v.tracer.Trace(OP_IsPrecompiled, nil, v)
}

func (v *Variable) ArrayBoundCheck(end *Variable) *Variable {
	return v.tracer.Trace(OP_ArrayBoundCheck, nil, v, end)
}

func (v *Variable) CreateAddress(nonceVar *Variable) *Variable {
	return v.tracer.Trace(OP_CreateAddress, nil, v, nonceVar).MarkBigIntAsAddress()
}

func (v *Variable) CreateAddress2(saltBigInt, code *Variable) *Variable {
	return v.tracer.Trace(OP_CreateAddress2, nil, v, saltBigInt, code).MarkBigIntAsAddress()
}

func (v *Variable) GetDataBig(start, size *Variable) *Variable {
	var cells []*MemByteCell
	var ok bool
	startU64 := start.BigInt().Uint64()
	sizeU64 := size.BigInt().Uint64()
	vSize := uint64(len(v.ByteArray()))
	noPadding := vSize >= startU64+sizeU64
	constStartAndSize := start.IsConst() && size.IsConst()

	if constStartAndSize && sizeU64 > 0 && noPadding {
		if cells, ok = v.tracer.byteArrayOriginCells[v]; ok {
			cmptypes.MyAssert(uint64(len(cells)) == vSize)
			_, sok := v.tracer.byteArrayCachedSize[v]
			cmptypes.MyAssert(sok)

			singleVariableMatch := true
			for i := uint64(0); i < sizeU64; i++ {
				cell := cells[i+startU64]
				if cell == nil || cell.offset != i {
					singleVariableMatch = false
					break
				}
				if i > 0 && cells[i+startU64-1].variable != cell.variable {
					singleVariableMatch = false
					break
				}
			}

			if singleVariableMatch {
				if uint64(len(cells[startU64].variable.ByteArray())) == sizeU64 {
					ret := cells[startU64].variable
					vArray := v.ByteArray()
					for i, a := range ret.ByteArray() {
						b := vArray[uint64(i)+startU64]
						cmptypes.MyAssert(a == b)
					}
					return ret
				}
			}
		}
	}
	ret := v.tracer.Trace(OP_GetDataBig, nil, v, start, size)
	if size.IsConst() {
		v.tracer.byteArrayCachedSize[ret] = size
	}
	if !ret.IsConst() && ok && constStartAndSize && vSize > 0 && noPadding {
		newCells := cells[startU64 : startU64+sizeU64]
		cmptypes.MyAssert(len(newCells) == len(ret.ByteArray()))

		if oldCells, ok := v.tracer.byteArrayOriginCells[ret]; !ok {
			v.tracer.byteArrayOriginCells[ret] = newCells
		} else {
			cmptypes.MyAssert(len(oldCells) == len(newCells))
			for i, oldCell := range oldCells {
				cell := newCells[i]
				if oldCell == nil {
					cmptypes.MyAssert(cell == nil)
				} else {
					cmptypes.MyAssert(oldCell.variable == cell.variable, "%v mismatch @%v: old var %v, var %v",
						ret.Name(), i, oldCell.variable.Name(), cell.variable.Name())
					cmptypes.MyAssert(oldCell.offset == cell.offset, "%v mismatch @%v %v: old offset %v, offset %v",
						ret.Name(), i, oldCell.variable.Name(), oldCell.offset, cell.offset)
				}
			}
		}

		//v.tracer.byteArrayOriginCells[ret] = newCells
	}
	return ret
}

func (v *Variable) LenByteArray() *Variable {
	//if v.IsConst() {
	//	return v.tracer.ConstVar(new(big.Int).SetInt64(int64(len(v.ByteArray()))))
	//}
	if size, ok := v.tracer.byteArrayCachedSize[v]; ok {
		cmptypes.MyAssert(size.IsConst())
		return size
	}
	return v.tracer.Trace(OP_LenByteArray, nil, v)
}

func (v *Variable) SliceByteArray(start, end *Variable) *Variable {
	return v.tracer.Trace(OP_SliceByteArray, nil, v, start, end)
}

func (v *Variable) Guard() *Variable {
	if v.IsConst() {
		return v
	}
	out := v.tracer.Trace(OP_Guard, nil, v)
	return out
}

func (v *Variable) NGuard(name string) *Variable {
	if v.IsConst() {
		return v
	}
	annotation := v.tracer.ConstVarWithName(name, name)
	out := v.tracer.Trace(OP_Guard, nil, v, annotation)
	return out
}

func (v *Variable) GetStateValueID(keyVar *Variable) *Variable {
	return v.tracer.TraceWithName(OP_GetStateValueID, nil, "sID", v, keyVar)
}

func (v *Variable) SetStateValueID(keyVar *Variable, valueID *Variable) *Variable {
	return v.tracer.TraceWithName(OP_SetStateValueID, nil, "sIDM", v, keyVar, valueID)
}

func (v *Variable) GetAddrID(addrVar *Variable) *Variable {
	return v.tracer.TraceWithName(OP_GetAddrID, nil, "aID", v, addrVar)
}

func (v *Variable) SetAddrID(addrVar *Variable, valueID *Variable) *Variable {
	return v.tracer.TraceWithName(OP_SetAddrID, nil, "aIDM", v, addrVar, valueID)
}

func (v *Variable) GetBlockHashNumID(addrVar *Variable) *Variable {
	return v.tracer.TraceWithName(OP_GetBlockHashNumID, nil, "nID", v, addrVar)
}

func (v *Variable) SetBlockHashNumID(addrVar *Variable, valueID *Variable) *Variable {
	return v.tracer.TraceWithName(OP_SetBlockHashNumID, nil, "nIDM", v, addrVar, valueID)
}

func (v *Variable) ByteArray() []byte {
	return v.val.([]byte)
}

func (v *Variable) BHash() common.Hash {
	return common.BigToHash(v.val.(*big.Int))
}

func (v *Variable) VEqual(rhsV *Variable) bool {
	if v.varType != rhsV.varType {
		return false
	}

	switch lhs := v.val.(type) {
	case *big.Int:
		rhs := rhsV.BigInt()
		return lhs.Cmp(rhs) == 0
	case []byte:
		rhs := rhsV.ByteArray()
		return string(lhs) == string(rhs)
	}
	return v.val == rhsV.val
}

func (v *Variable) Uint32() uint32 {
	return v.val.(uint32)
}

type ExecEnv struct {
	state         *state.StateDB
	inputs        []interface{}
	config        *OpConfig
	hasher        keccakState
	header        *types.Header
	getHash       vm.GetHashFunc
	precompiles   map[common.Address]vm.PrecompiledContract
	isProcess     bool
	addrCache     map[*big.Int]common.Address
	stateKeyCache map[*big.Int]common.Hash
	globalCache   *cache.GlobalCache
}

func NewExecEnvWithCache() *ExecEnv {
	return &ExecEnv{
		addrCache:     make(map[*big.Int]common.Address, 200),
		stateKeyCache: make(map[*big.Int]common.Hash, 200),
	}
}

func (env *ExecEnv) GetNewBigInt() *big.Int {
	if env.isProcess {
		poolSize := len(env.globalCache.BigIntPool)
		if poolSize > 0 {
			ret := env.globalCache.BigIntPool[poolSize-1]
			env.globalCache.BigIntPool = env.globalCache.BigIntPool[:poolSize-1]
			return ret
		}
	}
	return new(big.Int)
}

func (env *ExecEnv) HashToBig(h common.Hash) *big.Int {
	return env.GetNewBigInt().SetBytes(h[:])
}

func (env *ExecEnv) IntToBig(i int) *big.Int {
	return env.GetNewBigInt().SetInt64(int64(i))
}

func (env *ExecEnv) CopyBig(bi *big.Int) *big.Int {
	return env.GetNewBigInt().Set(bi)
}

func (env *ExecEnv) BigToAddress(bi *big.Int) common.Address {
	if env.addrCache != nil {
		if addr, ok := env.addrCache[bi]; ok {
			return addr
		} else {
			//addr := common.BigToAddress(bi)
			addr := FastBigToAddress(bi)
			env.addrCache[bi] = addr
			return addr
		}
	}
	//ret := common.BigToAddress(bi)
	//MyAssert(ret == FastBigToAddress(bi))
	ret := FastBigToAddress(bi)
	return ret
}

func (env *ExecEnv) BigToHash(bi *big.Int, isLoad bool) common.Hash {
	if isLoad {
		//hash := common.BigToHash(bi)
		hash := FastBigToHash(bi)
		if env.stateKeyCache != nil {
			env.stateKeyCache[bi] = hash
		}
		return hash
	} else {
		if env.stateKeyCache != nil {
			if hash, ok := env.stateKeyCache[bi]; ok {
				return hash
			}
		}
		//ret := common.BigToHash(bi)
		//MyAssert(ret == FastBigToHash(bi))
		ret := FastBigToHash(bi)
		return ret
	}
}

const (
	_S = _W / 8        // word size in bytes
	_W = bits.UintSize // word size in bits
)

func FastBigToHash(bi *big.Int) (h common.Hash) {
	words := bi.Bits()
	i := len(h)
	for _, d := range words {
		for j := 0; j < _S; j++ {
			i--
			h[i] = byte(d)
			d >>= 8
			if i == 0 {
				break
			}
		}
		if i == 0 {
			break
		}
	}
	return
}

func FastBigToAddress(bi *big.Int) (h common.Address) {
	words := bi.Bits()
	i := len(h)
	for _, d := range words {
		for j := 0; j < _S; j++ {
			i--
			h[i] = byte(d)
			d >>= 8
			if i == 0 {
				break
			}
		}
		if i == 0 {
			break
		}
	}
	return
}

type OpExecuteFunc func(*ExecEnv) interface{}

type OpConfig struct {
	variant StringID
	param   interface{}
}

type OpDef struct {
	name      StringID
	impFuc    OpExecuteFunc
	config    OpConfig
	isStoreOp bool
	isLoadOp  bool
	isReadOp  bool
	isGuardOp bool
	id        uint32
}

var OPCounter uint32 = 0

func NewOpDef(nameStr string, impFuc OpExecuteFunc, configVariant StringID, configParam interface{}, isStoreOp, isLoadOp bool) *OpDef {
	config := OpConfig{
		variant: configVariant,
		param:   configParam,
	}
	OPCounter++
	opDef := &OpDef{
		name:      ToStringID(nameStr),
		impFuc:    impFuc,
		config:    config,
		isStoreOp: isStoreOp,
		isLoadOp:  isLoadOp,
		id:        OPCounter,
	}
	return opDef
}

func NewLoadOpDef(impFuc OpExecuteFunc, configVariant StringID) *OpDef {
	nameStr := GetFuncNameStr(impFuc) + *configVariant
	return NewOpDef(nameStr, impFuc, configVariant, nil, false, true)
}

func NewStoreOpDef(impFuc OpExecuteFunc, configVariant StringID) *OpDef {
	nameStr := GetFuncNameStr(impFuc) + *configVariant
	return NewOpDef(nameStr, impFuc, configVariant, nil, true, false)
}

func NewReadOpDef(impFuc OpExecuteFunc, configVariant StringID) *OpDef {
	nameStr := GetFuncNameStr(impFuc) + *configVariant
	op := NewOpDef(nameStr, impFuc, configVariant, nil, false, false)
	op.isReadOp = true
	return op
}

func NewSimpleOpDef(impFunc OpExecuteFunc) *OpDef {
	return NewOpDef(GetFuncNameStr(impFunc), impFunc, nil, nil, false, false)
}

var guardID = uint32(0)

func NewGuardOpDef(impFunc OpExecuteFunc, name string) *OpDef {
	ret := NewOpDef(name, impFunc, nil, nil, false, false)
	ret.isGuardOp = true
	// make sure all guard ops have the same id
	if guardID == 0 {
		guardID = ret.id
	} else {
		ret.id = guardID
	}
	return ret
}

func NewParamOpDef(nameStr string, impFuc OpExecuteFunc, configVariant StringID, configParam interface{}) *OpDef {
	return NewOpDef(nameStr, impFuc, configVariant, configParam, false, false)
}

func (op *OpDef) IsGuard() bool {
	return op.isGuardOp
}

func (op *OpDef) IsLoadOrStoreOrRead() bool {
	return op.isLoadOp || op.isStoreOp || op.isReadOp
}

func (op *OpDef) IsLog() bool {
	return op == OP_StoreLog
}

func (op *OpDef) IsStates() bool {
	variant := op.config.variant
	return variant == ACCOUNT_STATE || variant == ACCOUNT_COMMITTED_STATE
}

func (op *OpDef) IsVirtual() bool {
	variant := op.config.variant
	return variant == VIRTUAL_GASUSED || variant == VIRTUAL_FAILED
}

type DebugStatsForStatement struct {
	cachedRecordValueString                   string
	cachedSimpleNameString                    string
	cachedSimpleNameStringWithRegisterMapping map[*map[uint32]uint]string
}

type Statement struct {
	output                                    *Variable
	inputs                                    []*Variable
	op                                        *OpDef
	Reverted                                  bool
	DebugStats                                *DebugStatsForStatement
}

func NewStatement(op *OpDef, debug bool, outVar *Variable, inVars ...*Variable) *Statement {
	s := &Statement{
		output: outVar,
		inputs: inVars,
		op:     op,
	}
	if debug {
		s.DebugStats = &DebugStatsForStatement{
			cachedRecordValueString:                   "",
			cachedSimpleNameString:                    "",
			cachedSimpleNameStringWithRegisterMapping: make(map[*map[uint32]uint]string),
		}
	}
	if outVar != nil {
		outVar.originStatement = s
	}
	return s
}

func (s *Statement) IsAllInputsConstant() bool {
	return IsAllConstants(s.inputs)
}

func (s *Statement) getRegisterAppendix(v *Variable, registerMapping *map[uint32]uint) string {
	ra := ""
	if !v.IsConst() && registerMapping != nil {
		ra = "_R" + strconv.Itoa(int((*registerMapping)[v.id]))
	}
	return ra
}

func (s *Statement) getInputNameList(registerMapping *map[uint32]uint) string {
	inputNames := make([]string, 0, len(s.inputs))
	if s.op == OP_ConcatBytes {
		inputNames = append(inputNames, s.inputs[0].Name()) // len
		cmptypes.MyAssert(len(s.inputs) >= 1)
		cmptypes.MyAssert((len(s.inputs)-1)%3 == 0)

		inputs := s.inputs[1:]
		for i := 0; i < len(inputs); i += 3 {
			currentV := inputs[i]
			currentStartingOffset := inputs[i+1].Uint64()
			currentCount := inputs[i+2].Uint64()
			ra := s.getRegisterAppendix(currentV, registerMapping)
			var name string
			if currentV == currentV.tracer.ByteArray_Empty {
				name = fmt.Sprintf("[0]*%v", currentCount)
			} else {
				name = fmt.Sprintf("%v%v[%v:%v]", currentV.Name(), ra, currentStartingOffset, currentCount)
			}
			inputNames = append(inputNames, name)
		}
	} else {
		for _, v := range s.inputs {
			ra := s.getRegisterAppendix(v, registerMapping)
			inputNames = append(inputNames, v.Name()+ra)
		}
	}
	inputNameList := strings.Join(inputNames, ", ")
	return inputNameList
}

func (s *Statement) SimpleNameString() string {
	return s.SimpleNameStringWithRegisterAnnotation(nil)
}

func (s *Statement) SimpleNameStringWithRegisterAnnotation(registerMapping *map[uint32]uint) string {
	if registerMapping == nil {
		if s.DebugStats.cachedSimpleNameString != "" {
			return s.DebugStats.cachedSimpleNameString
		}
	} else {
		if s.DebugStats.cachedSimpleNameStringWithRegisterMapping[registerMapping] != "" {
			return s.DebugStats.cachedSimpleNameStringWithRegisterMapping[registerMapping]
		}
	}

	inputNameList := s.getInputNameList(registerMapping)
	inputNameList = LimitStringLength(inputNameList, 400)
	result := *(s.op.name) + "(" + inputNameList + ")"
	if s.output != nil {
		ra := s.getRegisterAppendix(s.output, registerMapping)
		result = s.output.Name() + ra + " = " + result
	}

	if registerMapping == nil {
		s.DebugStats.cachedSimpleNameString = result
	} else {
		s.DebugStats.cachedSimpleNameStringWithRegisterMapping[registerMapping] = result
	}

	return result
}

func (s *Statement) RecordedValueString() string {
	if s.DebugStats.cachedRecordValueString != "" {
		return s.DebugStats.cachedRecordValueString
	}

	inputs := make([]interface{}, len(s.inputs))
	var output interface{}
	for i, v := range s.inputs {
		if v.tVal != nil {
			inputs[i] = v.tVal
		} else {
			inputs[i] = v.val
		}
	}

	if s.output != nil {
		if s.output.tVal != nil {
			output = s.output.tVal
		} else {
			output = s.output.val
		}
	}

	ret := s.ValueString(inputs, output)
	s.DebugStats.cachedRecordValueString = ret
	return ret
}

func (s *Statement) ValueString(inputs []interface{}, output interface{}) string {
	inputValues := make([]string, len(inputs))
	for i, v := range inputs {
		inputValues[i] = GetValueString(v)
	}
	inputValueList := strings.Join(inputValues, ", ")
	inputValueList = LimitStringLength(inputValueList, 400)
	result := *(s.op.name) + "(" + inputValueList + ")"
	if output != nil {
		result = GetValueString(output) + " = " + result
	}
	return result
}

func (s *Statement) TypeConvert(variable *Variable, val interface{}) interface{} {
	if variable.tVal == nil {
		return val
	}
	switch src := val.(type) {
	case *big.Int:
		switch variable.tVal.(type) {
		case common.Hash:
			return common.BigToHash(src)
		case common.Address:
			return common.BigToAddress(src)
		default:
			panic(fmt.Sprint("tVal has type %v", reflect.TypeOf(variable.tVal).Name()))
		}
	default:
		panic(fmt.Sprintf("%v has tVal!", reflect.TypeOf(val).Name()))
	}
}

//func (s *Statement) NewIfInputsReplaced(replaceMapping map[uint32]*Variable) *Statement {
//	newInputs := make([]*Variable, len(s.inputs))
//	var replaced bool
//	for i, v := range s.inputs {
//		if newV, ok := replaceMapping[v.id]; ok {
//			newInputs[i] = newV
//			replaced = true
//		} else {
//			newInputs[i] = v
//		}
//	}
//	if replaced {
//		newOut := s.output
//		if newOut != nil {
//			newOut = newOut.Copy()
//		}
//		return NewStatement(s.op, newOut, newInputs...)
//	} else {
//		return nil
//	}
//}

func GetInputValues(inputVars []*Variable, valueBuffer []interface{}) []interface{} {
	if len(valueBuffer) != 0 {
		panic("Buffer initial size should be zero")
	}
	//values := make([]interface{}, len(inputVars))
	for _, v := range inputVars {
		valueBuffer = append(valueBuffer, v.val)
		//values[i] = v.val
	}
	return valueBuffer
}

func IsAllConstants(inputVars []*Variable) bool {
	for _, v := range inputVars {
		if !v.IsConst() {
			return false
		}
	}
	return true
}

// If one and only one of the inputs have non-empty custom name, return it as base
// otherwise return empty string
func InferBaseNameFromInputs(f *OpDef, inputVars []*Variable) string {
	if f.IsGuard() {
		return inputVars[0].customNamePart
	}
	result := ""
	for _, v := range inputVars {
		if v.customNamePart != "" {
			if result == "" {
				result = v.customNamePart
			} else {
				return ""
			}
		}
	}
	if result != "" && result[len(result)-1] != '#' {
		result = result + "#"
	}
	return result
}

func InputVars(values ...*Variable) []*Variable {
	return values
}

type TracerCallFrame struct {
	callerAddress   *Variable
	contractAddress *Variable
	codeAddress     *Variable
	value           *Variable
	input           *Variable
	code            *Variable
	codeHash        *Variable
	mem             *TracerMem
	stack           *TracerStack
	retOffset       *Variable
	retSize         *Variable
	codeCached      []byte
}

func NewCallFrame(tracer *ReuseTracer, caller, contract, codeAddr, value, input *Variable) *TracerCallFrame {
	return &TracerCallFrame{
		callerAddress:   caller,
		contractAddress: contract,
		codeAddress:     codeAddr,
		value:           value,
		input:           input,
		mem:             newTracerMem(tracer),
		stack:           newTracerStack(),
	}
}

func (cf *TracerCallFrame) GuardCodeAddress() {
	if cf.codeAddress.IsConst() {
		return
	}
	gca := cf.codeAddress.NGuard("account_precompiled")

	if cf.contractAddress == cf.codeAddress {
		cf.contractAddress = gca
	}

	cf.codeAddress = gca
}

type MemByteCell struct {
	variable *Variable
	offset   uint64
}

func newMemByteCell(variable *Variable, offset uint64) *MemByteCell {
	return &MemByteCell{
		variable: variable,
		offset:   offset,
	}
}

type TracerMem struct {
	store  []*MemByteCell
	tracer *ReuseTracer
}

func newTracerMem(tracer *ReuseTracer) *TracerMem {
	return &TracerMem{tracer: tracer}
}

func (m *TracerMem) Set(offsetVar, sizeVar, byteArrayValueVariable *Variable) {
	// It's possible the offset is greater than 0 and size equals 0. This is because
	// the calcMemSize (common.go) could potentially return 0 when size is zero (NO-OP)
	offsetVar = offsetVar.NGuard("mem_offset")
	sizeVar = sizeVar.NGuard("mem_size")
	offset, size := offsetVar.BigInt().Uint64(), sizeVar.BigInt().Uint64()
	if size > 0 {
		// length of store may never be less than offset + size.
		// The store should be resized PRIOR to setting the memory
		if offset+size > uint64(len(m.store)) {
			panic("invalid memory: store empty")
		}
		byteArrayValueVariable.LenByteArray().NGuard("mem_vlen")
		if cells, ok := m.tracer.byteArrayOriginCells[byteArrayValueVariable]; ok {
			valueLen := uint64(len(cells))
			bArray := byteArrayValueVariable.ByteArray()
			cmptypes.MyAssert(valueLen == uint64(len(bArray)))
			for i := offset; i < offset+size && i < offset+valueLen; i++ {
				cell := cells[i-offset]
				m.store[i] = cell
				if cell != nil {
					cmptypes.MyAssert(bArray[i-offset] == cell.variable.ByteArray()[cell.offset])
				} else {
					cmptypes.MyAssert(bArray[i-offset] == 0)
				}
			}
		} else {
			valueLen := uint64(len(byteArrayValueVariable.ByteArray()))
			for i := offset; i < offset+size && i < offset+valueLen; i++ {
				m.store[i] = newMemByteCell(byteArrayValueVariable, i-offset)
			}
		}
	}
}

func (m *TracerMem) Set32(offsetVar, bigIntVal *Variable) {
	// length of store may never be less than offset + size.
	// The store should be resized PRIOR to setting the memory
	offsetVar = offsetVar.NGuard("mem_offset")
	offset := offsetVar.BigInt().Uint64()
	if offset+32 > uint64(len(m.store)) {
		panic("invalid memory: store empty")
	}

	bytesVar := bigIntVal.BigIntTo32Bytes()
	byteLen := uint64(len(bytesVar.ByteArray()))
	cmptypes.MyAssert(byteLen == 32)
	for i := uint64(0); i < byteLen; i++ {
		m.store[offset+i] = newMemByteCell(bytesVar, i)
	}

	//// Zero the memory area
	//for i := uint64(0); i < 32; i++ {
	//	m.store[offset+i] = nil
	//}
	//
	//if bigIntVal.BigInt().Sign() != 0 {
}

func (m *TracerMem) Resize(size uint64) {
	if uint64(m.Len()) < size {
		extension := make([]*MemByteCell, size-uint64(m.Len()))
		m.store = append(m.store, extension...)
	}
}

func (m *TracerMem) GetCopy(offsetVar, sizeVar *Variable) *Variable {
	return m.GetPtr(offsetVar, sizeVar)
}

// GetPtr returns the offset + size
func (m *TracerMem) GetPtr(offsetVar_BigInt, sizeVar_BigInt *Variable) *Variable {
	// make sure that we read the same mem loc
	offsetVar_BigInt = offsetVar_BigInt.NGuard("mem_offset")
	sizeVar_BigInt = sizeVar_BigInt.NGuard("mem_sze")
	size := sizeVar_BigInt.BigInt().Int64()
	offset := offsetVar_BigInt.BigInt().Int64()
	if size == 0 {
		return sizeVar_BigInt.tracer.TraceMemoryRead(sizeVar_BigInt, nil)
	}

	if len(m.store) > int(offset) {
		return sizeVar_BigInt.tracer.TraceMemoryRead(sizeVar_BigInt, m.store[offset:offset+size])
	}

	return nil
}

func (m *TracerMem) GetPtrForMLoad(offsetVar_BigInt, sizeVar_BigInt *Variable) *Variable {
	// make sure that we read the same mem loc
	offsetVar_BigInt = offsetVar_BigInt.NGuard("mem_offset")
	sizeVar_BigInt = sizeVar_BigInt.NGuard("mem_size")
	size := sizeVar_BigInt.BigInt().Int64()
	offset := offsetVar_BigInt.BigInt().Int64()
	if size == 0 {
		return sizeVar_BigInt.tracer.TraceMemoryRead(sizeVar_BigInt, nil)
	}

	if len(m.store) > int(offset) {
		if size == 32 && m.store[offset] != nil {
			byte32Var := m.store[offset].variable
			if bigIntVar, ok := m.tracer.byte32VarToBigIntVar[byte32Var]; ok {
				checkPassed := true
				for i := int64(0); i < size; i++ {
					if m.store[offset+i] == nil {
						checkPassed = false
						break
					}
					v := m.store[offset+i].variable
					offset := m.store[offset+i].offset
					if v != byte32Var || offset != uint64(i) {
						checkPassed = false
						break
					}
				}
				if checkPassed {
					return bigIntVar
				}
			}
		}
		return sizeVar_BigInt.tracer.TraceMemoryRead(sizeVar_BigInt, m.store[offset:offset+size])
	}

	return nil
}

func (m *TracerMem) Len() int {
	return len(m.store)
}

func (m *TracerMem) Data() []*MemByteCell {
	return m.store
}

type TracerStack struct {
	data []*Variable
}

func newTracerStack() *TracerStack {
	return &TracerStack{data: make([]*Variable, 0, 1024)}
}

func (st *TracerStack) len() int {
	return len(st.data)
}

func (st *TracerStack) Back(n int) *Variable {
	return st.data[st.len()-n-1]
}

func (st *TracerStack) ReplaceBack(n int, v *Variable) {
	st.data[st.len()-n-1] = v
}

func (st *TracerStack) Data() []*Variable {
	return st.data
}

func (st *TracerStack) push(d *Variable) {
	// NOTE push limit (1024) is checked in baseCheck
	//stackItem := new(Variable).Set(d)
	//st.data = append(st.data, stackItem)
	st.data = append(st.data, d)
}
func (st *TracerStack) pushN(ds ...*Variable) {
	st.data = append(st.data, ds...)
}

func (st *TracerStack) pop() (ret *Variable) {
	ret = st.data[len(st.data)-1]
	st.data = st.data[:len(st.data)-1]
	return
}

func (st *TracerStack) swap(n int) {
	st.data[st.len()-n], st.data[st.len()-1] = st.data[st.len()-1], st.data[st.len()-n]
}

func (st *TracerStack) dup(n int) {
	st.push(st.data[st.len()-n])
}

func (st *TracerStack) peek() *Variable {
	return st.data[st.len()-1]
}

// helper funcs
func CopyBigInt(bi *big.Int) *big.Int {
	return new(big.Int).Set(bi)
}
