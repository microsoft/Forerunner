package emulator

import (
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	"os"
	"time"
)

type FileLogWriter struct {
	logDir string
}

func NewFileLogWriter(logDir string) *FileLogWriter {
	return &FileLogWriter{
		logDir: logDir,
	}
}

// O_APPEND is atomic only when data is small, so use dedicated loop to guarantee.
func (w *FileLogWriter) writeln(bytes []byte) {
	sFilePath := fmt.Sprintf("%s/%s.json",
		w.logDir, time.Now().Format("20060102"))

	f, err := os.OpenFile(sFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Error("Error open emulator log file", "err", err)
		return
	}
	defer func() { _ = f.Close() }()

	bytes = append(bytes, byte('\n'))
	_, err = f.Write(bytes)
	if err != nil {
		log.Error("Error while writing emulator log file", "err", err)
		return
	}
}

func (w *FileLogWriter) Stop() {
}

// async wrapper

type logWriterConsumer struct {
	writer LogWriter
}

func (l logWriterConsumer) Accept(x interface{}) {
	l.writer.writeln(x.([]byte))
}

type AsyncFileLogWriter struct {
	*AsyncConsumer
}

func NewAsyncFileLogWriter(writer *FileLogWriter) *AsyncFileLogWriter {
	return &AsyncFileLogWriter{
		AsyncConsumer: NewAsyncConsumer(logWriterConsumer{writer: writer}),
	}
}

func (a AsyncFileLogWriter) writeln(x []byte) {
	a.AsyncConsumer.Accept(x)
}
