// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

package core

import (
	"github.com/ethereum/go-ethereum/core/vm"
	"sync"
	"sync/atomic"
)

type Controller struct {
	evm    *vm.EVM
	evmMu  sync.Mutex
	finish int32
	ReuseDone sync.WaitGroup
}

const Continue = 0
const abort = 1

func NewController() (c *Controller) {
	return new(Controller)
}

func (c *Controller) Reset() {
	c.evm = nil
	c.finish = Continue
}

func (c *Controller) IsAborted() bool {
	return atomic.LoadInt32(&c.finish) == abort
}

func (c *Controller) TryAbortCounterpart() bool {
	return atomic.CompareAndSwapInt32(&c.finish, Continue, abort)
}

func (c *Controller) SetEvm(evm *vm.EVM) {
	c.evmMu.Lock()
	c.evm = evm
	c.evmMu.Unlock()
}

func (c *Controller) StopEvm() {
	c.evmMu.Lock()
	empty := c.evm == nil
	c.evmMu.Unlock()
	if !empty {
		c.evm.Cancel()
	}
}
