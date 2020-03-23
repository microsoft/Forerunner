package emulator

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	"io"
	"os"
	"sync"
)

type FileLogReader struct {
	logDir      string
	emulateFile string

	stopCh chan struct{} // Quit channel to signal termination
	lineCh chan []byte

	wg sync.WaitGroup
}

func NewFileLogReader(logDir, emulateFile string) *FileLogReader {
	w := &FileLogReader{
		logDir:      logDir,
		emulateFile: emulateFile,
		stopCh:      make(chan struct{}),
		lineCh:      make(chan []byte, 16),
	}

	go w.loop()

	return w
}

func (w *FileLogReader) Stop() {
	close(w.stopCh)
	// never close lineCh here

	w.wg.Wait()
}

func (w *FileLogReader) loop() {
	w.wg.Add(1)
	defer w.wg.Done()

	// todo
	//sFilePath := fmt.Sprintf("%s/%s.json",
	//	w.logDir, time.Now().Format("20060102"))
	sFilePath := fmt.Sprintf("%s/%s", w.logDir, w.emulateFile)

	file, err := os.Open(sFilePath)
	if err != nil {
		panic(err)
	}
	defer func() { _ = file.Close() }()

	reader := bufio.NewReader(file)

L:
	for {
		// quit as early as possible
		select {
		case <-w.stopCh:
			break L
		default:
		}

		var line []byte
		line, err = reader.ReadBytes(byte('\n'))
		//fmt.Printf(" > Read %d characters\n", len(line))
		log.Trace(" > Emulate read %d characters\n", len(line))

		if err != nil {
			if len(line) > 0 {
				panic(errors.New(fmt.Sprintf("line length %d > 0 on eof", len(line))))
			}
			break
		}

		select {
		case <-w.stopCh:
			break L
		case w.lineCh <- line[:len(line)-1]:
		}
	}

	close(w.lineCh)

	if err != nil && err != io.EOF {
		panic(err)
	}
}

func (w *FileLogReader) readln() ([]byte, bool) {
	// quit as early as possible
	select {
	case <-w.stopCh:
		return nil, false
	default:
	}

	bytes, ok := <-w.lineCh
	return bytes, ok
}
