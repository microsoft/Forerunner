package emulator

import (
	"fmt"
	"testing"
	"time"
)

type mockLogWriter struct {
}

func (m *mockLogWriter) writeln(x []byte) {
	fmt.Printf("%#v\n", string(x))
}

func TestChainRecorder(t *testing.T) {
	a := NewGethRecorder(&mockLogWriter{}, false, false)

	a.RecordInsertChain(time.Now(), nil)
	a.RecordInsertChain(time.Now(), nil)
	a.RecordInsertChain(time.Now(), nil)
}
