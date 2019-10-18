package cmpreuse

import (
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/optipreplayer/cache"
	"sync"
	"sync/atomic"
	"time"
)

type ReuseResult struct {
	gasUsed  uint64
	rwrecord *cache.RWRecord

	duration [2]time.Duration
	cmpCnt   int64
}

type ApplyResult struct {
	gasUsed uint64
	failed  bool
	err     error

	duration time.Duration
}

type Controller struct {
	reuseStatus uint64

	evm        *vm.EVM
	reuseAbort int32
	abort      int32

	wg          sync.WaitGroup
	reuseDoneCh chan ReuseResult
	applyDoneCh chan ApplyResult
}

const hit = 1
const noCache = 2
const cacheNoIn = 3
const cacheNoMatch = 4
const unknown = 5
const Continue = 0
const abort = 1

func NewController() (c *Controller) {
	c = &Controller{
		reuseDoneCh: make(chan ReuseResult),
		applyDoneCh: make(chan ApplyResult),
	}
	return c
}

func (c *Controller) IsReuseAbort() bool {
	return atomic.LoadInt32(&c.reuseAbort) == abort
}

func (c *Controller) Finish(reuse bool) (ok bool) {
	c.wg.Done()
	if ok = atomic.CompareAndSwapInt32(&c.abort, Continue, abort); ok {
		if reuse {
			if c.evm != nil {
				c.evm.Cancel()
			}
		} else {
			atomic.StoreInt32(&c.reuseAbort, abort)
		}
		c.wg.Wait()
	}
	return
}

func (c *Controller) Close() {
	close(c.reuseDoneCh)
	close(c.applyDoneCh)
}