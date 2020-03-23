package emulator

import (
	"fmt"
	"testing"
)

func TestFileLogReader(t *testing.T) {
	a := NewFileLogReader("/anadrive/emulator", "my.json")

	for {
		bytes, ok := a.readln()
		if !ok {
			break
		}

		fmt.Printf("%#v\n", string(bytes))
	}

	a.Stop()
}
