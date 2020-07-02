package cmptypes

import (
	"github.com/ethereum/go-ethereum/common"
	"math/big"
)

type PrecompiledContract interface {
	RequiredGas(input []byte) uint64  // RequiredPrice calculates the contract gas use
	Run(input []byte) ([]byte, error) // Run runs the precompiled contract
}

type IReuseTracer interface {
	Snapshot() uint
	RevertToSnapshot(snapShotID uint)
	InitTx(txHash common.Hash, from common.Address, to *common.Address,
		gasPrice, value *big.Int, gas, nonce uint64, data []byte)
	TracePreCheck()
	TraceIncCallerNonce()
	TraceCanTransfer()
	TraceAssertCodeAddrExistence()
	TraceGuardIsCodeAddressPrecompiled()
	TraceAssertValueZeroness()
	TraceTransfer()
	TraceGuardCodeLen()
	TraceRefund(*big.Int, uint64)
	TraceAssertStackValueZeroness()
	TraceAssertStackValueSelf(...int)
	TraceGasSStore()
	TraceGasSStoreEIP2200()
	TraceAssertExpByteLen()
	TraceGasCall()
	TraceGasCallCode()
	TraceGasDelegateOrStaticCall()
	TraceGasSelfdestruct()
	TraceOpByName(string, uint64, uint64)
	TraceCallReturn(err, reverted, nilOrEmptyReturnData bool)
	TraceCreateReturn(err, reverted bool)
	TraceCreateAddressAndSetupCode()
	TraceAssertNoExistingContract()
	MarkUnimplementedOp()
	ClearReturnData()
	NeedRT() bool
	TraceSetNonceForCreatedContract()
	TraceAssertNewCodeLen()
	TraceStoreNewCode()
	TraceAssertInputLen()
	TraceGasBigModExp()
	TraceRunPrecompiled(p PrecompiledContract)
	MarkNotExternalTransfer()
	MarkCompletedTrace(bool)
	ResizeMem(size uint64)
	GetStackSize() int
	GetMemSize() int
	GetTopStack() *big.Int
	TraceLoadCodeAndGuardHash()
	GetReturnData() []byte
	TraceCreateContractAccount()
}

type ISTrace interface {

}

