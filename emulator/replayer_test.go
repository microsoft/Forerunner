package emulator

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestChainReplayer1(t *testing.T) {
	var a replayMsg
	err := json.Unmarshal([]byte(`{"type": 1, "hello": "world"}`), &a)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%#v\n", a)
}

var lines = [][]byte{
	[]byte(`{"type": 17, "hello": "world"}`),
	[]byte(`{"type": 97, "hello": "world"}`),
}

func TestChainReplayer2(t *testing.T) {
	for _, line := range lines {
		obj, err := deserializeLine(line)
		if err != nil {
			panic(err)
		}

		fmt.Printf("%#v\n", obj)
	}
}

type mockFileLogReader struct {
	i int
}

func (m *mockFileLogReader) readln() (line []byte, ok bool) {
	i := m.i
	if i >= len(lines) {
		return nil, false
	}
	m.i += 1
	return lines[i], true
}

func TestRunReplay(t *testing.T) {
	mock := &mockFileLogReader{}
	replayer := GethReplayer{}
	replayer.RunReplay(mock)
}

func TestRunReplay2(t *testing.T) {
	reader := NewFileLogReader("/anadrive/emulator", "my.json")
	replayer := GethReplayer{}
	replayer.RunReplay(reader)
}
