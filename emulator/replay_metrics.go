package emulator

import (
	"github.com/ethereum/go-ethereum/log"
	"sync/atomic"
	"time"
)

type ReplayMetrics struct {
	initialized bool

	recordedStart time.Time
	replayStart   time.Time

	count    int64
	curLag   time.Duration
	totalLag int64 // milli second
}

func (r *ReplayMetrics) Initialize(recordedStart, replayStart time.Time) {
	if !r.initialized {
		r.recordedStart = recordedStart
		r.replayStart = replayStart
		r.initialized = true
		r.totalLag = 0
	}
}

func (r *ReplayMetrics) ReplayTimeMeter(msgTime time.Time, replayTime time.Time, isBlock bool) {
	if !r.initialized {
		return
	}

	expected := msgTime.Sub(r.recordedStart)
	actual := replayTime.Sub(r.replayStart)

	if isBlock {
		log.Warn("lagging meter", "gap", actual - expected)
	}
	r.replayTimeMeter(actual - expected)
}

func (r *ReplayMetrics) replayTimeMeter(duration time.Duration) {
	atomic.AddInt64(&r.count, 1)
	r.curLag = duration
	atomic.AddInt64(&r.totalLag, duration.Milliseconds())
}
