package emulator

import (
	"github.com/ethereum/go-ethereum/log"
	"time"
)

const RawLineChanCount = 10000
const MsgChanCount = 4000
const DeserializerCount = 200
const PublisherCount = 120

type GethReplayer struct {
	consumer *replayMsgConsumer
	broker   Broker

	rawLineChan chan []byte
	msgChan     chan ReplayMsg

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

		rawLineChan: make(chan []byte, RawLineChanCount),
		msgChan:     make(chan ReplayMsg, MsgChanCount),
	}
}

func (e *GethReplayer) GetChanLen() (int, int) {
	return len(e.rawLineChan), len(e.msgChan)
}

func (e *GethReplayer) SetRealtimeMode() {
	if !e.realtime {
		log.Warn("Emulator switched to realtime mode")

		e.broker = NewRealtimeBroker(e.consumer)
		e.realtime = true
	}
}

func (e *GethReplayer) RunReplay(reader LogReader) {
	go e.readLog(reader)
	go e.deserializeLog()
	go e.publishLog()
}

func (e *GethReplayer) readLog(reader LogReader) {
	for {
		line, ok := reader.readln()
		if !ok {
			return
		}
		e.rawLineChan <- line
	}
}

func (e *GethReplayer) deserializeLog() {
	deserializeWaitChan := make(chan struct{}, DeserializerCount)
	for {
		select {
		case line := <-e.rawLineChan:

			if e.IsRealtimeMode() {
				deserializeWaitChan <- struct{}{}
				go func(raw []byte) {
					msg, err := deserializeLine(raw)
					if err != nil {
						panic(err)
					}
					e.msgChan <- msg
					<-deserializeWaitChan
				}(line)
			} else {
				msg, err := deserializeLine(line)
				if err != nil {
					panic(err)
				}
				e.msgChan <- msg
			}

		}
	}
}

func (e *GethReplayer) publishLog() {
	waitChan := make(chan struct{}, PublisherCount)

	for {
		select {
		case msg := <-e.msgChan:
			if e.IsRealtimeMode() {
				waitChan <- struct{}{}
				go func(m ReplayMsg) {
					e.broker.Publish(m)
					<-waitChan
				}(msg)
			} else {
				e.broker.Publish(msg)
			}
		}
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

		waitingCh: make(chan struct{}, 100),
	}
}

func (b *RealtimeBroker) GetWaitingBufferSize() int {
	return len(b.waitingCh)
}

func (b *RealtimeBroker) Publish(msg ReplayMsg) {
	b.waitingCh <- struct{}{}
	execMsg := func() {
		b.Metrics.ReplayTimeMeter(msg, time.Now())
		b.Consumer.Accept(msg)
		<-b.waitingCh
	}

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

	gap := expectedDelta - realDelta
	if gap > 0 {
		time.AfterFunc(gap, execMsg)
	} else {
		go execMsg()
	}
}
