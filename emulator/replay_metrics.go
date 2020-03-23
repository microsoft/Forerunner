package emulator

import "time"

type ReplayMetrics struct {
	initialized bool

	recordedStart time.Time
	replayStart   time.Time

	count    int64
	totalLag time.Duration
}

func (r *ReplayMetrics) Initialize(recordedStart, replayStart time.Time) {
	if !r.initialized {
		r.recordedStart = recordedStart
		r.replayStart = replayStart
		r.initialized = true
	}
}

func (r *ReplayMetrics) ReplayTimeMeter(recordedTime, replayTime time.Time) {
	if !r.initialized {
		return
	}

	expected := recordedTime.Sub(r.recordedStart)
	actual := replayTime.Sub(r.replayStart)

	r.replayTimeMeter(actual - expected)
}

func (r *ReplayMetrics) replayTimeMeter(duration time.Duration) {
	r.count += 1
	r.totalLag += duration
}
