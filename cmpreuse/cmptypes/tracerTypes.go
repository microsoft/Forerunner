// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

package cmptypes

import (
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"sync/atomic"
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

type ITraceTrieSearchResult interface {
}

type SimpleTryLock struct {
	// lock state
	// if state == 0, no lock holds
	// if state == -1, lock holds
	state *int32

	// a broadcast channel
	ch chan struct{}
}

func NewSimpleTryLock() *SimpleTryLock {
	return &SimpleTryLock{
		state: new(int32),
		ch:    make(chan struct{}, 1),
	}
}

func (m *SimpleTryLock) TryLock() bool {
	if atomic.CompareAndSwapInt32(m.state, 0, -1) {
		// acquire OK
		return true
	}
	return false
}

func (m *SimpleTryLock) Lock() {
	for {
		if atomic.CompareAndSwapInt32(m.state, 0, -1) {
			// acquire OK
			return
		}

		// waiting for broadcast signal or timeout
		select {
		case <-m.ch:
			// wake up to try again
		}
	}
}

func (m *SimpleTryLock) Unlock() {
	if ok := atomic.CompareAndSwapInt32(m.state, -1, 0); !ok {
		panic("Unlock() failed")
	}
	select {
	case m.ch <- struct{}{}:
		return
	default:
		return
	}
}
