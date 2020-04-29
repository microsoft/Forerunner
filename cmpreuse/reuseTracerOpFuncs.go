package cmpreuse

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/sha3"
	"hash"
	"math/big"
	"reflect"
)

func fLoad(env *ExecEnv) interface{} {
	addr := common.BigToAddress(env.inputs[0].(*big.Int))
	switch env.config.variant {
	case ACCOUNT_NONCE:
		return env.state.GetNonce(addr)
	case ACCOUNT_BALANCE:
		return CopyBigInt(env.state.GetBalance(addr))
	case ACCOUNT_EXIST:
		return env.state.Exist(addr)
	case ACCOUNT_EMPTY:
		return env.state.Empty(addr)
	case ACCOUNT_CODEHASH:
		return new(big.Int).SetBytes(env.state.GetCodeHash(addr).Bytes())
	case ACCOUNT_CODESIZE:
		return new(big.Int).SetInt64(int64(env.state.GetCodeSize(addr)))
	case ACCOUNT_CODE:
		return env.state.GetCode(addr)
	case ACCOUNT_STATE:
		key := common.BigToHash(env.inputs[1].(*big.Int))
		return env.state.GetState(addr, key).Big()
	case ACCOUNT_COMMITTED_STATE:
		key := common.BigToHash(env.inputs[1].(*big.Int))
		return env.state.GetCommittedState(addr, key).Big()
	default:
		panic("Unknown fLoad variant!")
	}
	return nil
}

func fStore(env *ExecEnv) interface{} {
	addr := common.BigToAddress(env.inputs[0].(*big.Int))
	switch env.config.variant {
	case ACCOUNT_NONCE:
		nonce := env.inputs[1].(uint64)
		env.state.SetNonce(addr, nonce)
	case ACCOUNT_BALANCE:
		balance := env.inputs[1].(*big.Int)
		env.state.SetBalance(addr, balance)
	case ACCOUNT_CODE:
		code := env.inputs[1].([]byte)
		env.state.SetCode(addr, code)
	case ACCOUNT_STATE:
		key := common.BigToHash(env.inputs[1].(*big.Int))
		value := common.BigToHash(env.inputs[2].(*big.Int))
		env.state.SetState(addr, key, value)
	case ACCOUNT_SUICIDE:
		env.state.Suicide(addr)
	case ACCOUNT_LOG:
		log := &types.Log{
			Address: addr,
			Data:    env.inputs[1].([]byte),
			BlockNumber: env.header.Number.Uint64(),
		}
		if len(env.inputs) > 2 {
			topics := make([]common.Hash, len(env.inputs)-2)
			for i, v := range env.inputs[2:] {
				topics[i] = common.BigToHash(v.(*big.Int))
			}
			log.Topics = topics
		}
		env.state.AddLog(log)
	case VIRTUAL_FAILED:
		// pass
	case VIRTUAL_GASUSED:
		// pass
	default:
		panic("Unknown fLoad variant!")
	}
	return nil
}

func fRead(env *ExecEnv) interface{} {
	switch env.config.variant {
	case BLOCK_COINBASE:
		return AddressToBigInt(env.header.Coinbase)
	case BLOCK_TIMESTAMP:
		return new(big.Int).SetUint64(env.header.Time)
	case BLOCK_NUMBER:
		return new(big.Int).Set(env.header.Number)
	case BLOCK_DIFFICULTY:
		return new(big.Int).Set(env.header.Difficulty)
	case BLOCK_GASLIMIT:
		return new(big.Int).SetUint64(env.header.GasLimit)
	case BLOCK_HASH:
		num := env.inputs[0].(*big.Int)
		currentBlockNumber := env.header.Number
		n := new(big.Int).Sub(currentBlockNumber, common.Big257)
		if num.Cmp(n) > 0 && num.Cmp(currentBlockNumber) < 0 {
			return env.getHash(num.Uint64()).Big()
		} else {
			//return common.Hash{}
			return new(big.Int).SetInt64(0)
		}
	default:
		panic("Unknown fRead variant")
	}

}

// big.Ints
func _GetTwoBigInts(env *ExecEnv) (lhs, rhs *big.Int) {
	lhs = env.inputs[0].(*big.Int)
	rhs = env.inputs[1].(*big.Int)
	return
}

func fCmpBigInt(env *ExecEnv) interface{} {
	lhs, rhs := _GetTwoBigInts(env)
	return lhs.Cmp(rhs)
}

func fEqualBigInt(env *ExecEnv) interface{} {
	return (fCmpBigInt(env)).(int) == 0
}

func fGEBigInt(env *ExecEnv) interface{} {
	return (fCmpBigInt(env)).(int) >= 0
}

func fSubBigInt(env *ExecEnv) interface{} {
	lhs, rhs := _GetTwoBigInts(env)
	return new(big.Int).Sub(lhs, rhs)
}

func fAddBigInt(env *ExecEnv) interface{} {
	lhs, rhs := _GetTwoBigInts(env)
	return new(big.Int).Add(lhs, rhs)
}

func fMulBigInt(env *ExecEnv) interface{} {
	lhs, rhs := _GetTwoBigInts(env)
	return new(big.Int).Mul(lhs, rhs)
}

func fDivBigInt(env *ExecEnv) interface{} {
	lhs, rhs := _GetTwoBigInts(env)
	return new(big.Int).Div(lhs, rhs)
}

func fBigIntToHash(env *ExecEnv) interface{} {
	bi := env.inputs[0].(*big.Int)
	return common.BigToHash(bi)
}

func fLowestByteBigInt(env *ExecEnv) interface{} {
	bi := env.inputs[0].(*big.Int)
	ba := make([]byte, 1)
	ba[0] = byte(bi.Int64() & 0xff)
	return ba
}

func fHashToBigInt(env *ExecEnv) interface{} {
	h := env.inputs[0].(common.Hash)
	return new(big.Int).SetBytes(h.Bytes())
}

func fBigIntToAddress(env *ExecEnv) interface{} {
	bi := env.inputs[0].(*big.Int)
	return common.BigToAddress(bi)
}

func fCropBigIntAddress(env *ExecEnv) interface{} {
	bi := env.inputs[0].(*big.Int)
	return new(big.Int).SetBytes(common.BigToAddress(bi).Bytes())
}

func fAddressToBigInt(env *ExecEnv) interface{} {
	addr := env.inputs[0].(common.Address)
	return new(big.Int).SetBytes(addr.Bytes())
}

func fByteArrayToBigInt(env *ExecEnv) interface{} {
	ba := env.inputs[0].([]byte)
	return new(big.Int).SetBytes(ba)
}

func fBigIntTo32Bytes(env *ExecEnv) interface{} {
	bi := env.inputs[0].(*big.Int)
	return math.PaddedBigBytes(bi, 32)
}

func fIntToBigInt(env *ExecEnv) interface{} {
	i := env.inputs[0].(int)
	return new(big.Int).SetInt64(int64(i))
}

func fU256BigInt(env *ExecEnv) interface{} {
	bi := env.inputs[0].(*big.Int)
	r := new(big.Int).Set(bi)
	return math.U256(r)
}

func fS256BigInt(env *ExecEnv) interface{} {
	bi := env.inputs[0].(*big.Int)
	r := new(big.Int).Set(bi)
	return math.S256(r)
}

func fSignBigInt(env *ExecEnv) interface{} {
	bi := env.inputs[0].(*big.Int)
	return bi.Sign()
}

func fNegBigInt(env *ExecEnv) interface{} {
	bi := env.inputs[0].(*big.Int)
	r := new(big.Int).Set(bi)
	return r.Neg(bi)
}

func fAbsBigInt(env *ExecEnv) interface{} {
	bi := env.inputs[0].(*big.Int)
	r := new(big.Int).Set(bi)
	return r.Abs(bi)
}

func fIsUint64BigInt(env *ExecEnv) interface{} {
	bi := env.inputs[0].(*big.Int)
	return bi.IsUint64()
}

func fBitLenBigInt(env *ExecEnv) interface{} {
	bi := env.inputs[0].(*big.Int)
	return bi.BitLen()
}

// direct evm opcode implementation

var big1 = new(big.Int).SetUint64(1)
var tt255 = math.BigPow(2, 255)
var bigZero = new(big.Int).SetUint64(0)

func _GetOneBigIntCopy(env *ExecEnv) *big.Int {
	x := env.inputs[0].(*big.Int)
	return new(big.Int).Set(x)
}

func _GetTwoBigIntsCopy(env *ExecEnv) (*big.Int, *big.Int) {
	x, y := env.inputs[0].(*big.Int), env.inputs[1].(*big.Int)
	return new(big.Int).Set(x), new(big.Int).Set(y)
}

func _GetThreeBigIntsCopy(env *ExecEnv) (*big.Int, *big.Int, *big.Int) {
	x, y, z := env.inputs[0].(*big.Int), env.inputs[1].(*big.Int), env.inputs[2].(*big.Int)
	return new(big.Int).Set(x), new(big.Int).Set(y), new(big.Int).Set(z)
}

func fEVMAdd(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)
	math.U256(y.Add(x, y))
	return y
}

func fEVMSub(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)
	math.U256(y.Sub(x, y))
	return y
}

func fEVMMul(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)
	math.U256(x.Mul(x, y))
	return x
}

func fEVMDiv(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)
	if y.Sign() != 0 {
		math.U256(y.Div(x, y))
	} else {
		y.SetUint64(0)
	}
	return y
}

func fEVMSdiv(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)
	x, y = math.S256(x), math.S256(y)
	res := new(big.Int).SetUint64(0)

	if y.Sign() == 0 || x.Sign() == 0 {
		//
	} else {
		if x.Sign() != y.Sign() {
			res.Div(x.Abs(x), y.Abs(y))
			res.Neg(res)
		} else {
			res.Div(x.Abs(x), y.Abs(y))
		}
		math.U256(res)
	}
	return res
}

func fEVMMod(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)
	if y.Sign() == 0 {
		x.SetUint64(0)
	} else {
		math.U256(x.Mod(x, y))
	}
	return x
}

func fEVMSmod(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)
	x, y = math.S256(x), math.S256(y)
	res := new(big.Int).SetUint64(0)

	if y.Sign() == 0 {
		//stack.push(res)
	} else {
		if x.Sign() < 0 {
			res.Mod(x.Abs(x), y.Abs(y))
			res.Neg(res)
		} else {
			res.Mod(x.Abs(x), y.Abs(y))
		}
		math.U256(res)
	}

	return res
}

func fEVMExp(env *ExecEnv) interface{} {
	base, exponent := _GetTwoBigIntsCopy(env)
	// some shortcuts
	cmpToOne := exponent.Cmp(big1)
	if cmpToOne < 0 { // Exponent is zero
		// x ^ 0 == 1
		base.SetUint64(1)
	} else if base.Sign() == 0 {
		// 0 ^ y, if y != 0, == 0
		base.SetUint64(0)
	} else if cmpToOne == 0 { // Exponent is one
		// x ^ 1 == x
		//stack.push(base)
	} else {
		base = math.Exp(base, exponent)
	}
	return base
}

func fEVMSignExtend(env *ExecEnv) interface{} {
	back, num := _GetTwoBigIntsCopy(env)
	if back.Cmp(big.NewInt(31)) < 0 {
		bit := uint(back.Uint64()*8 + 7)
		mask := back.Lsh(common.Big1, bit)
		mask.Sub(mask, common.Big1)
		if num.Bit(int(bit)) > 0 {
			num.Or(num, mask.Not(mask))
		} else {
			num.And(num, mask)
		}

		num = math.U256(num)
	}
	return num
}

func fEVMNot(env *ExecEnv) interface{} {
	x := _GetOneBigIntCopy(env)
	math.U256(x.Not(x))
	return x
}

func fEVMLt(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)
	if x.Cmp(y) < 0 {
		y.SetUint64(1)
	} else {
		y.SetUint64(0)
	}
	return y
}

func fEVMGt(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)
	if x.Cmp(y) > 0 {
		y.SetUint64(1)
	} else {
		y.SetUint64(0)
	}
	return y
}

func fEVMSlt(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)

	xSign := x.Cmp(tt255)
	ySign := y.Cmp(tt255)

	switch {
	case xSign >= 0 && ySign < 0:
		y.SetUint64(1)

	case xSign < 0 && ySign >= 0:
		y.SetUint64(0)

	default:
		if x.Cmp(y) < 0 {
			y.SetUint64(1)
		} else {
			y.SetUint64(0)
		}
	}
	return y
}

func fEVMSgt(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)

	xSign := x.Cmp(tt255)
	ySign := y.Cmp(tt255)

	switch {
	case xSign >= 0 && ySign < 0:
		y.SetUint64(0)

	case xSign < 0 && ySign >= 0:
		y.SetUint64(1)

	default:
		if x.Cmp(y) > 0 {
			y.SetUint64(1)
		} else {
			y.SetUint64(0)
		}
	}
	return y
}

func fEVMEq(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)
	if x.Cmp(y) == 0 {
		y.SetUint64(1)
	} else {
		y.SetUint64(0)
	}
	return y
}

func fEVMIszero(env *ExecEnv) interface{} {
	x := _GetOneBigIntCopy(env)
	if x.Sign() > 0 {
		x.SetUint64(0)
	} else {
		x.SetUint64(1)
	}
	return x
}

func fIszeroBigInt(env *ExecEnv) interface{} {
	x := _GetOneBigIntCopy(env)
	if x.Sign() != 0 {
		x.SetUint64(0)
	} else {
		x.SetUint64(1)
	}
	return x
}

func fEVMAnd(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)
	return x.And(x, y)
}

func fEVMOr(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)
	y.Or(x, y)
	return y
}

func fEVMXor(env *ExecEnv) interface{} {
	x, y := _GetTwoBigIntsCopy(env)
	y.Xor(x, y)
	return y
}

func fEVMByte(env *ExecEnv) interface{} {
	th, val := _GetTwoBigIntsCopy(env)
	if th.Cmp(common.Big32) < 0 {
		b := math.Byte(val, 32, int(th.Int64()))
		val.SetUint64(uint64(b))
	} else {
		val.SetUint64(0)
	}
	return val
}

func fEVMAddmod(env *ExecEnv) interface{} {
	x, y, z := _GetThreeBigIntsCopy(env)
	if z.Cmp(bigZero) > 0 {
		x.Add(x, y)
		x.Mod(x, z)
		math.U256(x)
	} else {
		x.SetUint64(0)
	}
	return x
}

func fEVMMulmod(env *ExecEnv) interface{} {
	x, y, z := _GetThreeBigIntsCopy(env)
	if z.Cmp(bigZero) > 0 {
		x.Mul(x, y)
		x.Mod(x, z)
		math.U256(x)
	} else {
		x.SetUint64(0)
	}
	return x
}

// opSHL implements Shift Left
// The SHL instruction (shift left) pops 2 values from the stack, first arg1 and then arg2,
// and pushes on the stack arg2 shifted to the left by arg1 number of bits.
func fEVMSHL(env *ExecEnv) interface{} {
	// Note, second operand is left in the stack; accumulate result into it, and no need to push it afterwards
	shift, value := _GetTwoBigIntsCopy(env)
	shift, value = math.U256(shift), math.U256(value)

	if shift.Cmp(common.Big256) >= 0 {
		value.SetUint64(0)

	}
	n := uint(shift.Uint64())
	math.U256(value.Lsh(value, n))
	return value
}

// opSHR implements Logical Shift Right
// The SHR instruction (logical shift right) pops 2 values from the stack, first arg1 and then arg2,
// and pushes on the stack arg2 shifted to the right by arg1 number of bits with zero fill.
func fEVMSHR(env *ExecEnv) interface{} {
	// Note, second operand is left in the stack; accumulate result into it, and no need to push it afterwards
	x, y := _GetTwoBigIntsCopy(env)
	shift, value := math.U256(x), math.U256(y)

	if shift.Cmp(common.Big256) >= 0 {
		value.SetUint64(0)
	}
	n := uint(shift.Uint64())
	math.U256(value.Rsh(value, n))
	return value
}

// opSAR implements Arithmetic Shift Right
// The SAR instruction (arithmetic shift right) pops 2 values from the stack, first arg1 and then arg2,
// and pushes on the stack arg2 shifted to the right by arg1 number of bits with sign extension.
func fEVMSAR(env *ExecEnv) interface{} {
	// Note, S256 returns (potentially) a new bigint, so we're popping, not peeking this one
	x, y := _GetTwoBigIntsCopy(env)
	shift, value := math.U256(x), math.S256(y)

	if shift.Cmp(common.Big256) >= 0 {
		if value.Sign() >= 0 {
			value.SetUint64(0)
		} else {
			value.SetInt64(-1)
		}
		math.U256(value)
	}
	n := uint(shift.Uint64())
	value.Rsh(value, n)
	math.U256(value)
	return value
}

type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

func fSha3(env *ExecEnv) interface{} {
	data := env.inputs[0].([]byte)

	interpreter := env
	if interpreter.hasher == nil {
		interpreter.hasher = sha3.NewLegacyKeccak256().(keccakState)
	} else {
		interpreter.hasher.Reset()
	}
	var hasherBuf common.Hash
	interpreter.hasher.Write(data)
	interpreter.hasher.Read(hasherBuf[:])

	r := new(big.Int).SetBytes(hasherBuf[:])
	return r
}

func fCreateAddress(env *ExecEnv) interface{} {
	callerAddr := common.BigToAddress(env.inputs[0].(*big.Int))
	callerNonce := env.inputs[1].(uint64)

	return AddressToBigInt(crypto.CreateAddress(callerAddr, callerNonce))
}

func fCreateAddress2(env *ExecEnv) interface{} {
	callerAddr := common.BigToAddress(env.inputs[0].(*big.Int))
	salt := common.BigToHash(env.inputs[1].(*big.Int))
	code := env.inputs[2].([]byte)
	codeHashBytes := crypto.Keccak256Hash(code).Bytes()

	return AddressToBigInt(crypto.CreateAddress2(callerAddr, salt, codeHashBytes))
}

func fRunPrecompiled(env *ExecEnv) interface{} {
	input := env.inputs[0].([]byte)
	p := env.inputs[1].(vm.PrecompiledContract)
	ret, _ := p.Run(input)
	return ret
}

func fIsPrecompiled(env *ExecEnv) interface{} {
	input := env.inputs[0].(*big.Int)
	_, ok := env.precompiles[common.BigToAddress(input)]
	return ok
}

func fArrayBoundCheck(env *ExecEnv) interface{} {
	var (
		returnData = env.inputs[0].([]byte)
		end        = env.inputs[1].(*big.Int)
	)

	if !end.IsUint64() || uint64(len(returnData)) < end.Uint64() {
		return false
	}

	return true
}

//  ints
func _LessINT(lhs, rhs int) bool {
	return lhs < rhs
}

func fLessINT(env *ExecEnv) interface{} {
	lhs := env.inputs[0].(int)
	rhs := env.inputs[1].(int)
	return _LessINT(lhs, rhs)
}

func fAddUint64(env *ExecEnv) interface{} {
	lhs := env.inputs[0].(uint64)
	rhs := env.inputs[1].(uint64)
	return lhs + rhs
}

// bytearray
func fConcatBytes(env *ExecEnv) interface{} {
	arrayLen := env.inputs[0].(*big.Int).Int64()
	if int64(len(env.inputs)-1) != 2*arrayLen {
		panic(fmt.Sprintf("Wrong number of mem cells. arraylen: %v, input len: %v", arrayLen, len(env.inputs)))
	}
	result := make([]byte, arrayLen)
	byteCache := make(map[*big.Int][]byte)
	for i := int64(0); i < arrayLen; i += 1 {
		cell := env.inputs[2*i+1]
		offset := env.inputs[2*i+1+1].(uint64)
		switch cellTyped := cell.(type) {
		case []byte:
			if len(cellTyped) == 0 {
				if offset != 0 {
					panic("Wrong zero byte cell")
				}
			} else {
				if offset >= uint64(len(cellTyped)) {
					panic("Too large array offset for array cell")
				}
				result[i] = cellTyped[offset]
			}
		case *big.Int:
			panic("Should never be big.Int now!")
			if cellTyped.Sign() == 0 {
				if offset != 0 {
					panic(fmt.Sprintf("Zero big but offset not zero: %v", offset))
				}
				result[i] = 0
			} else {
				bytes, ok := byteCache[cellTyped]
				if !ok {
					bytes = cellTyped.Bytes()
					byteCache[cellTyped] = bytes
				}

				if offset >= uint64(len(bytes)) {
					panic("Too large offset for bit.Int cell")
				}
				result[i] = bytes[offset]
			}
		default:
			panic(fmt.Sprintf("Unknown cell type %v", reflect.TypeOf(cellTyped).Name()))
		}
	}
	return result
}

func fGetDataBig(env *ExecEnv) interface{} {
	data, start, size := env.inputs[0].([]byte), env.inputs[1].(*big.Int), env.inputs[2].(*big.Int)
	dlen := big.NewInt(int64(len(data)))

	s := math.BigMin(start, dlen)
	e := math.BigMin(new(big.Int).Add(s, size), dlen)
	return common.RightPadBytes(data[s.Uint64():e.Uint64()], int(size.Uint64()))
}

func fLenByteArray(env *ExecEnv) interface{} {
	data := env.inputs[0].([]byte)
	dlen := big.NewInt(int64(len(data)))
	return dlen
}

func fSliceByteArray(env *ExecEnv) interface{} {
	data := env.inputs[0].([]byte)
	start := env.inputs[1].(*big.Int)
	end := env.inputs[2].(*big.Int)
	return data[start.Uint64():end.Uint64()]
}

// Generic
func fEqualGeneric(env *ExecEnv) interface{} {
	return env.inputs[0] == env.inputs[1]
}

// Asserts
func fAssertTrue(env *ExecEnv) interface{} {
	in := env.inputs[0].(bool)
	return in == true
}

func fAssertFalse(env *ExecEnv) interface{} {
	in := env.inputs[0].(bool)
	return in == false
}

// special funcs used in trace transformation
func fAssignGeneric(env *ExecEnv) interface{} {
	in := env.inputs[0]
	return in
}

func fGuard(env *ExecEnv) interface{} {
	return env.inputs[0]
}

func fGetStateValueID(env *ExecEnv) interface{} {
	stateValueIDMap := env.inputs[0].(map[common.Hash]uint32)
	key := env.inputs[1].(*big.Int)
	keyHash := common.BigToHash(key)
	return stateValueIDMap[keyHash]
}

func fSetStateValueID(env *ExecEnv) interface{} {
	stateValueIDMap := env.inputs[0].(map[common.Hash]uint32)
	key := env.inputs[1].(*big.Int)
	keyHash := common.BigToHash(key)
	valueID := env.inputs[2].(uint32)
	mCopy := make(map[common.Hash]uint32, len(stateValueIDMap)+1)
	for k, v := range stateValueIDMap {
		mCopy[k] = v
	}
	mCopy[keyHash] = valueID
	return mCopy
}

func fGetAddrID(env *ExecEnv) interface{} {
	addrIDMap := env.inputs[0].(map[common.Address]uint32)
	key := env.inputs[1].(*big.Int)
	keyAddr := common.BigToAddress(key)
	return addrIDMap[keyAddr]
}

func fSetAddrID(env *ExecEnv) interface{} {
	addrIDMap := env.inputs[0].(map[common.Address]uint32)
	key := env.inputs[1].(*big.Int)
	keyAddr := common.BigToAddress(key)
	valueID := env.inputs[2].(uint32)
	mCopy := make(map[common.Address]uint32, len(addrIDMap)+1)
	for k, v := range addrIDMap {
		mCopy[k] = v
	}
	mCopy[keyAddr] = valueID
	return mCopy
}

func fGetBlockHashNumID(env *ExecEnv) interface{} {
	addrIDMap := env.inputs[0].(map[uint64]uint32)
	num := env.inputs[1].(*big.Int)
	numUint64 := num.Uint64()
	return addrIDMap[numUint64]
}

func fSetBlockHashNumID(env *ExecEnv) interface{} {
	addrIDMap := env.inputs[0].(map[uint64]uint32)
	num := env.inputs[1].(*big.Int)
	numUint64 := num.Uint64()
	valueID := env.inputs[2].(uint32)
	mCopy := make(map[uint64]uint32, len(addrIDMap)+1)
	for k, v := range addrIDMap {
		mCopy[k] = v
	}
	mCopy[numUint64] = valueID
	return mCopy
}

// other things
func fCmpUINT64(env *ExecEnv) interface{} {
	lhs := env.inputs[0].(uint64)
	rhs := env.inputs[1].(uint64)
	if lhs < rhs {
		return -1
	} else if lhs == rhs {
		return 0
	} else {
		return 1
	}
}

func fEqualUINT64(env *ExecEnv) interface{} {
	lhs := env.inputs[0].(uint64)
	rhs := env.inputs[1].(uint64)
	return lhs == rhs
}

func _equalINT(lhs, rhs int) bool {
	return lhs == rhs
}

func fEqualINT(env *ExecEnv) interface{} {
	lhs := env.inputs[0].(int)
	rhs := env.inputs[1].(int)
	return _equalINT(lhs, rhs)
}

func _cmpINT(lhs, rhs int) int {
	if lhs < rhs {
		return -1
	} else if lhs == rhs {
		return 0
	} else {
		return 1
	}
}

func fCmpINT(env *ExecEnv) interface{} {
	lhs := env.inputs[0].(int)
	rhs := env.inputs[1].(int)
	return _cmpINT(lhs, rhs)
}
