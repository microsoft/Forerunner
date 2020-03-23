package emulator

import (
	"github.com/ethereum/go-ethereum/log"
	"time"
)

type GethReplayer struct {
	consumer *replayMsgConsumer
	broker   Broker

	realtime bool
}

var GlobalGethReplayer *GethReplayer = nil

func NewGethReplayer(blockchain ReplayBlockChain, txPool ReplayTxPool) *GethReplayer {
	consumer := &replayMsgConsumer{
		BlockChain: blockchain,
		TxPool:     txPool,
	}

	// use sequential for initial, switch to realtime once tx pool is loaded
	broker := NewSequentialBroker(consumer)

	_, realtime := Broker(broker).(*RealtimeBroker)

	return &GethReplayer{
		consumer: consumer,
		broker:   broker,
		realtime: realtime,
	}
}

func (e *GethReplayer) SetRealtimeMode() {
	if !e.realtime {
		log.Warn("Emulator switched to realtime mode")

		e.broker = NewRealtimeBroker(e.consumer)
		e.realtime = true
	}
}

func (e *GethReplayer) RunReplay(reader LogReader) {
	for {
		line, ok := reader.readln()
		if !ok {
			return
		}

		msg, err := deserializeLine(line)
		if err != nil {
			panic(err)
		}

		e.broker.Publish(msg)
	}
}

func (e *GethReplayer) IsRealtimeMode() bool {
	return e.realtime
}

type Broker interface {
	Publish(msg ReplayMsg)
}

type SequentialBroker struct {
	Consumer Consumer
}

func NewSequentialBroker(consumer Consumer) *SequentialBroker {
	return &SequentialBroker{Consumer: consumer}
}

func (b *SequentialBroker) Publish(msg ReplayMsg) {
	b.Consumer.Accept(msg)
}

type RealtimeBroker struct {
	Consumer Consumer

	Metrics *ReplayMetrics

	recordedStart *time.Time
	replayStart   time.Time

	waitingCh chan struct{}
}

func NewRealtimeBroker(consumer Consumer) *RealtimeBroker {
	return &RealtimeBroker{
		Consumer: consumer,

		Metrics: &ReplayMetrics{},

		waitingCh: make(chan struct{}, 64),
	}
}

func (b *RealtimeBroker) Publish(msg ReplayMsg) {
	b.waitingCh <- struct{}{}

	t := msg.GetTime()

	var realDelta time.Duration
	if b.recordedStart == nil {
		b.recordedStart = &t
		b.replayStart = time.Now()
		realDelta = 0

		b.Metrics.Initialize(*b.recordedStart, b.replayStart)
	} else {
		realDelta = time.Since(b.replayStart)
	}
	expectedDelta := t.Sub(*b.recordedStart)

	execMsg := func() {
		<-b.waitingCh

		b.Metrics.ReplayTimeMeter(msg.GetTime(), time.Now())
		b.Consumer.Accept(msg)
	}

	if expectedDelta-realDelta > 0 {
		time.AfterFunc(expectedDelta-realDelta, execMsg)
	} else {
		execMsg()
	}
}
