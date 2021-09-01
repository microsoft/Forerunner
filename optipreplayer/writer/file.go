// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

package writer

import (
	"io"
	"os"
	"time"

	"github.com/pkg/errors"
)

type File struct {
	filepath  string
	file      *os.File
	bytesCh   chan []byte
	canFinish bool
	closed    bool
}

func NewFile(filepath string) *File {
	file := &File{}

	if filepath == "" {
		return file
	}

	f, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		return file
	}

	file.filepath = filepath
	file.file = f
	file.bytesCh = make(chan []byte, 10000)
	file.canFinish = false
	file.closed = false
	return file
}

func (f *File) Open() error {
	if f.filepath == "" {
		return errors.New("filepath is nil")
	}
	file, err := os.OpenFile(f.filepath, os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		return err
	}
	f.file = file
	return nil
}

func (f *File) SendMsg(msg []byte) {
	f.bytesCh <- msg
}

func (f *File) WriteMsg(msg []byte) error {
	if f.file == nil {
		err := f.Open()
		if err != nil {
			return err
		}
	}

	return ProtectRun(func() error {
		n, err := f.file.WriteString(string(msg) + "\n")
		if err == nil && n < len(msg) {
			err = io.ErrShortWrite
		}
		return err
	})
}

func (f *File) Start() {
	for {
		if len(f.bytesCh) == 0 {
			if f.canFinish {
				break
			}
			time.Sleep(10 * time.Microsecond)
			continue
		}
		msg := <-f.bytesCh

		for {
			err := f.WriteMsg(msg)
			if err == nil {
				break
			}
		}
	}
	f.closed = true
}

func (f *File) SetFinish() {
	f.canFinish = true
}

func (f *File) Close() {
	if f.file == nil {
		return
	}
	for {
		if !f.closed {
			time.Sleep(10 * time.Microsecond)
			continue
		} else {
			break
		}
	}
	f.file.Close()
}
