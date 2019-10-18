package writer

import (
	"io"
	"net"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/pkg/errors"
)

type Conn struct {
	conn      net.Conn
	bytesCh   chan []byte
	canFinish bool
	closed    bool
}

func NewConn() *Conn {
	c := &Conn{}
	var err error

	c.conn, err = net.Dial("tcp", "127.0.0.1:3345")
	if err != nil || c.conn == nil {
		return c
	}

	c.bytesCh = make(chan []byte, 10000)
	c.canFinish = false
	c.closed = false
	return c
}

func (c *Conn) SetFinish() {
	c.canFinish = true
}

func (c *Conn) Connect() error {
	conn, err := net.Dial("tcp", "127.0.0.1:3345")
	if err != nil || conn == nil {
		return errors.New("error!")
	}
	c.conn = conn
	return nil
}

func (c *Conn) WriteMsg(msg []byte) error {
	if c.conn == nil {
		err := c.Connect()
		if err != nil {
			return err
		}
	}

	return ProtectRun(func() error {
		n, err := c.conn.Write(msg)
		if err == nil && n < len(msg) {
			err = io.ErrShortWrite
		}
		return err
	})
}

func (c *Conn) SendMsg(msg []byte) {
	c.bytesCh <- msg
}

func (c *Conn) Start() {
	for {
		if len(c.bytesCh) == 0 {
			if c.canFinish {
				break
			}
			time.Sleep(10 * time.Microsecond)
			continue
		}

		msg := <-c.bytesCh
		rewriteCnt := 0

		for {
			err := c.WriteMsg(msg)
			if err == nil {
				break
			} else {
				log.Info("conn", "error", err)
				rewriteCnt++
				if rewriteCnt == 3 {
					break
				}
			}
		}
	}
	c.closed = true
}

func (c *Conn) Close() {
	if c.conn == nil {
		return
	}
	for {
		if !c.closed {
			time.Sleep(10 * time.Microsecond)
			continue
		} else {
			break
		}
	}
	c.conn.Close()
}
