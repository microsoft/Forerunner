package emulator

import (
	"encoding/json"
	"github.com/ethereum/go-ethereum/log"
)

type JsonLogSerializer struct {
}

func (*JsonLogSerializer) Serialize(enc interface{}) ([]byte, error) {
	return json.Marshal(enc)
}

// A recorder is essentially a consumer that composes serializer and writer
type recorder struct {
	Serializer LogSerializer
	Writer     LogWriter
}

func newRecorder(serializer LogSerializer, writer LogWriter) Consumer {
	return &recorder{Serializer: serializer, Writer: writer}
}

func (r *recorder) Accept(record interface{}) {
	line, err := r.Serializer.Serialize(record)
	if err != nil {
		log.Error("Error serializing in recorder", "err", err)
		return
	}
	r.Writer.writeln(line)
}

// see replay_types.go
type GethRecorder struct {
	recorder     Consumer
	realRecorder Consumer

	stopList []Stoppable
}

var GlobalGethRecorder *GethRecorder

func NewGethRecorder(writer LogWriter, async, initialStarted bool) *GethRecorder {
	var stopList []Stoppable
	recorder := newRecorder(&JsonLogSerializer{}, writer)
	if async {
		a := NewAsyncConsumer(recorder)
		stopList = append(stopList, a)
		recorder = a
	}

	var initialRecorder Consumer = &NilConsumer{}
	if initialStarted {
		initialRecorder = recorder
	}

	return &GethRecorder{
		recorder:     initialRecorder,
		realRecorder: recorder,
		stopList:     stopList,
	}
}

func (r *GethRecorder) Start() {
	if r.recorder != r.realRecorder {
		r.recorder = r.realRecorder

		log.Warn("Sync done, emulator recording started")
	}
}

func (r *GethRecorder) Stop() {
	for _, stoppable := range r.stopList {
		stoppable.Stop()
	}
}
