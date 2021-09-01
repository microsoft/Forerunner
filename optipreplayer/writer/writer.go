// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

package writer

import "sync"

type Writer struct {
	conn *Conn
	file *File
}

func NewWriter(filePath string) *Writer {
	return &Writer{
		conn: NewConn(),
		file: NewFile(filePath),
	}
}

func (w *Writer) Start() {
	go w.file.Start()
	go w.conn.Start()
}

func (w *Writer) SendMsg(msg []byte) {
	go w.file.SendMsg(msg)
	go w.conn.SendMsg(msg)
}

func (w *Writer) SetFinish() {
	w.file.SetFinish()
	w.conn.SetFinish()
}

func (w *Writer) Close() {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		w.file.Close()
		defer wg.Done()
	}()
	go func() {
		w.conn.Close()
		defer wg.Done()
	}()
	wg.Wait()
}
