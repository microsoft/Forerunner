package emulator

import (
	"github.com/ethereum/go-ethereum/log"
	"math/big"
	"time"
)

type ReplayMetrics struct {
	initialized bool

	recordedStart time.Time
	replayStart   time.Time

	count    int64
	curLag   time.Duration
	totalLag *big.Int // milli second
}

func (r *ReplayMetrics) Initialize(recordedStart, replayStart time.Time) {
	if !r.initialized {
		r.recordedStart = recordedStart
		r.replayStart = replayStart
		r.initialized = true
		r.totalLag = big.NewInt(0)
	}
}

func (r *ReplayMetrics) ReplayTimeMeter(msg ReplayMsg, replayTime time.Time) {
	if !r.initialized {
		return
	}

	expected := msg.GetTime().Sub(r.recordedStart)
	actual := replayTime.Sub(r.replayStart)

	if msg.GetType() == ReplayMsgBlocks {
		log.Warn("lagging meter", "gap", actual - expected)
	}
	r.replayTimeMeter(actual - expected)
}

func (r *ReplayMetrics) replayTimeMeter(duration time.Duration) {
	r.count += 1
	r.curLag = duration
	r.totalLag = new(big.Int).Add(r.totalLag, big.NewInt(duration.Milliseconds()))
}
