package emulator

import (
	"testing"
	"time"
)

func TestFileLogWriter(t *testing.T) {
	a := NewFileLogWriter("/anadrive/emulator")

	bytes := []byte("123")
	a.writeln(bytes)
	a.writeln(bytes)
	a.writeln(bytes)

	time.Sleep(1000)

	a.Stop()
}
