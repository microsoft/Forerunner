package emulator

import (
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const RawLineChanCount = 50000
const PublisherCount = 50
const LagBatchSize = 4000
const EarlyBatchSize = 100
const EarlyBatchGap = 2 * time.Millisecond

type GethReplayer struct {
	consumer *replayMsgConsumer
	broker   Broker

	rawLineChan      chan []byte
	seqLineChan      chan []byte
	blockLineChan    chan []byte
	txLineChan       chan []byte
	txLineMutex      sync.Mutex
	txLineBatchIndex int
	msgBatchChan     chan *batchRemoteTxs
	nextBatchIndex   int32

	lagBatch      *batchRemoteTxs
	lagBatchMutex sync.Mutex

	realtime int32
}

var GlobalGethReplayer *GethReplayer = nil

func NewGethReplayer(blockchain ReplayBlockChain, txPool ReplayTxPool) *GethReplayer {
	consumer := &replayMsgConsumer{
		BlockChain: blockchain,
		TxPool:     txPool,
	}

	// use sequential for initial, switch to realtime once tx pool is loaded
	broker := NewSequentialBroker(consumer)

	return &GethReplayer{
		consumer: consumer,
		broker:   broker,
		realtime: 0,

		rawLineChan:   make(chan []byte, RawLineChanCount),
		seqLineChan:   make(chan []byte, 0),
		blockLineChan: make(chan []byte, 100),
		txLineChan:    make(chan []byte, RawLineChanCount),
		msgBatchChan:  make(chan *batchRemoteTxs, 10000),
		lagBatch:      &batchRemoteTxs{Txs: make([]*types.Transaction, 0, LagBatchSize), Times: make([]time.Time, 0, LagBatchSize)},
	}
}

func (e *GethReplayer) GetChanLen() (int, int, int, int) {
	return len(e.rawLineChan), len(e.txLineChan), len(e.msgBatchChan), len(e.blockLineChan)
}

func (e *GethReplayer) SetRealtimeMode(firstLogTime time.Time) {
	if !e.IsRealtimeMode() {
		rb := NewRealtimeBroker(e.consumer)
		rb.InitRealtimeBroker(firstLogTime)
		e.broker = rb

		atomic.StoreInt32(&e.realtime, 1)
		log.Warn("Emulator switched to realtime mode             ! ! ! ! ! ! ! ! ! ! !")
	}
}

func (e *GethReplayer) AddNextBatchIndex() {
	atomic.AddInt32(&e.nextBatchIndex, 1)
}

func (e *GethReplayer) GetNextBatchIndex() int32 {
	return atomic.LoadInt32(&e.nextBatchIndex)
}

func (e *GethReplayer) IsRealtimeMode() bool {
	return atomic.LoadInt32(&e.realtime) == 1
}

func (e *GethReplayer) RunReplay(reader LogReader) {
	go e.readLog(reader)
	go e.shuntLog()
	go e.seqHandleLogs()
	go e.preBatchTxMsg()
	go e.handleTxBatch()
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

func (e *GethReplayer) shuntLog() {
	for {
		select {
		case line := <-e.rawLineChan:
			if e.IsRealtimeMode() {
				if strings.Contains(string(line), "\"type\":17,") {
					e.blockLineChan <- line
				} else {
					e.txLineChan <- line
				}
			} else {
				if strings.Contains(string(line), "\"type\":97,") {
					continue
				}
				e.seqLineChan <- line
			}
		}
	}
}

func (e *GethReplayer) preBatchTxMsg() {
	batcher := func() {
		for {
			e.txLineMutex.Lock()
			curLineBatchIndex := e.txLineBatchIndex
			curLineBatch := make([][]byte, 0, EarlyBatchSize)
			for ; len(curLineBatch) < EarlyBatchSize; {
				select {
				case line := <-e.txLineChan:
					if !strings.Contains(string(line), "\"type\":97,") {
						// skip pool log
						continue
					}
					curLineBatch = append(curLineBatch, line)
				}
			}
			e.txLineBatchIndex++
			e.txLineMutex.Unlock()

			msgs := make([]*addRemotesData, 0, EarlyBatchSize)
			for _, line := range curLineBatch {
				msg, err := deserializeLine(line)
				if err != nil {
					panic(err)
				}
				msgs = append(msgs, msg.(*addRemotesData))
			}
			// merge msgs into batches:
			msgBatches := preBatchMsg(msgs)
			// insert batch into
			for {
				if e.GetNextBatchIndex() == int32(curLineBatchIndex) {
					for _, mBatch := range msgBatches {
						e.msgBatchChan <- mBatch
					}
					e.AddNextBatchIndex()
					break
				} else {
					time.Sleep(1 * time.Millisecond)
				}
			}
		}
	}
	for i := 0; i < PublisherCount; i++ {
		go batcher()
	}

}

func preBatchMsg2(msgs []*addRemotesData) []*batchRemoteTxs {
	if len(msgs) == 0 {
		panic("empty msgs")
	}
	res := make([]*batchRemoteTxs, 0, EarlyBatchSize)
	curBatch := &batchRemoteTxs{Txs: make([]*types.Transaction, 0, EarlyBatchSize), Times: make([]time.Time, 0, EarlyBatchSize)}
	curBatch.AppendMsg(msgs[0])
	for i := 1; i < len(msgs); i++ {
		curMsgTime := msgs[i].GetTime()
		if curMsgTime.Before(msgs[i-1].GetTime()) || curMsgTime.Sub(curBatch.Times[0]) > EarlyBatchGap {
			res = append(res, curBatch)

			curBatch = &batchRemoteTxs{Txs: make([]*types.Transaction, 0, EarlyBatchSize), Times: make([]time.Time, 0, EarlyBatchSize)}
			curBatch.AppendMsg(msgs[i])
		} else {
			curBatch.AppendMsg(msgs[i])
		}
	}
	res = append(res, curBatch)
	return res
}


func preBatchMsg(msgs []*addRemotesData) []*batchRemoteTxs {
	if len(msgs) == 0 {
		panic("empty msgs")
	}
	res := make([]*batchRemoteTxs, 0, EarlyBatchSize)
	for _, msg:= range msgs{
		curBatch := &batchRemoteTxs{Times: make([]time.Time, 1, 1)}
		curBatch.Times[0] = msg.GetTime()
		curBatch.Txs = msg.Txs
		res = append(res, curBatch)

	}
	return res
}

func (e *GethReplayer) handleTxBatch() {
	handler := func() {
		for {
			select {
			case batch := <-e.msgBatchChan:
				if !e.broker.EarlyBatchPublish(batch) {
					e.handleLagBatch(batch)
				} else {
					e.lagBatchMutex.Lock()
					if len(e.lagBatch.Txs) == 0 {
						e.lagBatchMutex.Unlock()
					} else {
						nb := &batchRemoteTxs{Txs: e.lagBatch.Txs, Times: e.lagBatch.Times}
						e.lagBatch.Clear()
						e.lagBatchMutex.Unlock()
						e.broker.(*RealtimeBroker).batchPublish(nb, true)
					}
				}
			}
		}
	}
	for i := 0; i < PublisherCount; i++ {
		go handler()
	}
}

func (e *GethReplayer) handleLagBatch(b *batchRemoteTxs) {
	e.lagBatchMutex.Lock()
	e.lagBatch.AppendBatch(b)
	if len(e.lagBatch.Txs) >= 1000 {
		nb := &batchRemoteTxs{Txs: e.lagBatch.Txs, Times: e.lagBatch.Times}
		e.lagBatch.Clear()
		e.lagBatchMutex.Unlock()

		e.broker.(*RealtimeBroker).batchPublish(nb, true)
		return
	} else {
		e.lagBatchMutex.Unlock()
	}
}

func (e *GethReplayer) seqHandleLogs() {
	for {
		select {
		case line := <-e.seqLineChan:
			msg, err := deserializeLine(line)
			if err != nil {
				panic(err)
			}
			e.broker.SeqPublish(msg)
		case line := <-e.blockLineChan:
			msg, err := deserializeLine(line)
			if err != nil {
				panic(err)
			}
			e.broker.SeqPublish(msg)
		}

	}
}

type Broker interface {
	EarlyBatchPublish(batch *batchRemoteTxs) bool
	SeqPublish(msg ReplayMsg)
}

type SequentialBroker struct {
	Consumer Consumer
}

func NewSequentialBroker(consumer Consumer) *SequentialBroker {
	return &SequentialBroker{Consumer: consumer}
}

func (b *SequentialBroker) EarlyBatchPublish(batch *batchRemoteTxs) bool {
	//b.Consumer.Accept(msg)
	panic("wrong call")
}

func (b *SequentialBroker) SeqPublish(msg ReplayMsg) {
	b.Consumer.Accept(msg)
}

type RealtimeBroker struct {
	Consumer Consumer

	Metrics *ReplayMetrics

	recordedStart time.Time
	replayStart   time.Time

	waitingCh chan struct{}

	lagLogCh chan ReplayMsg

	batchCount   int32
	batchTxCount int32
}

func NewRealtimeBroker(consumer Consumer) *RealtimeBroker {
	return &RealtimeBroker{
		Consumer: consumer,

		Metrics: &ReplayMetrics{},

		waitingCh: make(chan struct{}, PublisherCount),
	}
}

func (b *RealtimeBroker) InitRealtimeBroker(firstLogTime time.Time) {
	b.recordedStart = firstLogTime
	b.replayStart = time.Now()
	b.Metrics.Initialize(b.recordedStart, b.replayStart)
}

func (b *RealtimeBroker) GetWaitingBufferSize() int {
	return len(b.waitingCh)
}

func (b *RealtimeBroker) AddLagBatch(txCount int) {
	atomic.AddInt32(&b.batchCount, 1)
	atomic.AddInt32(&b.batchTxCount, int32(txCount))
}

func (b *RealtimeBroker) GetBatchInfo() (int32, int32) {
	return atomic.LoadInt32(&b.batchCount), atomic.LoadInt32(&b.batchTxCount)
}

//
//func (b *RealtimeBroker) Publish(m ReplayMsg, batch *batchRemoteTxs) {
//
//	msg, ok := m.(*addRemotesData)
//	if !ok {
//		panic("type of msg should be `addRemotesData`")
//	}
//
//	gap := b.calculateGap(msg.GetTime())
//	if gap > 0 {
//		if len(batch.Txs) > 0 {
//			b.lagBatchPublish(batch)
//		}
//		gap = b.calculateGap(msg.GetTime())
//		if gap > 0 {
//			time.Sleep(gap)
//			b.Metrics.ReplayTimeMeter(msg.GetTime(), time.Now(), false)
//			b.Consumer.Accept(msg)
//			return
//		}
//	}
//
//	batch.AppendMsg(msg)
//	if len(batch.Txs) >= LagBatchSize {
//		b.lagBatchPublish(batch)
//	}
//}

// deprecated
func (b *RealtimeBroker) lagBatchPublish(batch *batchRemoteTxs) {
	biggestGap := b.calculateGap(batch.Times[0])
	b.AddLagBatch(len(batch.Txs))
	if biggestGap < (-2)*time.Second {
		bc, btc := b.GetBatchInfo()
		log.Warn("lag batch publish", "firstGap", biggestGap.String(), "lastGap", b.calculateGap(batch.Times[len(batch.Times)-1]).String(),
			"txCount", len(batch.Txs), "cumBatchCount", bc, "cumBatchTxCount", btc)
	}
	msg := &addRemotesData{Txs: batch.Txs}
	curTime := time.Now()
	for _, msgTime := range batch.Times {
		b.Metrics.ReplayTimeMeter(msgTime, curTime, false)
	}
	b.Consumer.Accept(msg)
	batch.Clear()
}

// return isLag
func (b *RealtimeBroker) EarlyBatchPublish(batch *batchRemoteTxs) bool {
	txCount := len(batch.Txs)

	biggestGap := b.calculateGap(batch.Times[0])
	startTime := time.Now()
	if biggestGap > 0 {
		time.Sleep(biggestGap)
		// publish curBatch
		realGap := time.Since(startTime)
		sleepTooMuch := realGap > (biggestGap + 10*time.Millisecond)
		b.batchPublish(batch, false)
		//if biggestGap > 500*time.Millisecond {
		//	log.Warn("early batch publish", "firstGap", biggestGap.String(), "txCount", txCount)
		//}
		if sleepTooMuch {
			log.Warn("sleep too much", "actual", realGap, "expected", biggestGap, "actual-expected", realGap-biggestGap, "txCount", txCount)
		}
		return true
	} else {
		return false
	}
}

func (b *RealtimeBroker) batchPublish(batch *batchRemoteTxs, isLag bool) {

	if isLag {
		biggestGap := b.calculateGap(batch.Times[0])
		b.AddLagBatch(len(batch.Txs))
		if biggestGap < (-1)*time.Second {
			bc, btc := b.GetBatchInfo()
			log.Warn("lag batch publish (lag)", "firstGap", biggestGap.String(), "lastGap", b.calculateGap(batch.Times[len(batch.Times)-1]).String(),
				"txCount", len(batch.Txs), "cumBatchCount", bc, "cumBatchTxCount", btc)
		}
	}

	msg := &addRemotesData{Txs: batch.Txs}
	curTime := time.Now()
	for _, msgTime := range batch.Times {
		b.Metrics.ReplayTimeMeter(msgTime, curTime, false)
	}
	b.Consumer.Accept(msg)
	batch.Clear()
}

func (b *RealtimeBroker) SeqPublish(msg ReplayMsg) {

	gap := b.calculateGap(msg.GetTime())

	if gap > 0 {
		time.Sleep(gap)
	}
	b.Metrics.ReplayTimeMeter(msg.GetTime(), time.Now(), true)
	b.Consumer.Accept(msg)

}

func (b *RealtimeBroker) calculateGap(t time.Time) time.Duration {

	realDelta := time.Since(b.replayStart)
	expectedDelta := t.Sub(b.recordedStart)

	gap := expectedDelta - realDelta
	return gap
}
