package emulator

import "sync"

type Consumer interface {
	Accept(interface{})
}

type NilConsumer struct {
}

func (n NilConsumer) Accept(interface{}) {
	// discards
}

// wraps a Consumer to make it async
type AsyncConsumer struct {
	consumer Consumer
	reqCh    chan interface{}
	stopCh   chan struct{} // Quit channel to signal termination

	wg sync.WaitGroup
}

func NewAsyncConsumer(consumer Consumer) *AsyncConsumer {
	a := &AsyncConsumer{
		consumer: consumer,
		reqCh:    make(chan interface{}, 65535),
		stopCh:   make(chan struct{}),
	}

	go a.loop()

	return a
}

func (a *AsyncConsumer) loop() {
	a.wg.Add(1)
	defer a.wg.Done()

	for {
		select {
		case req := <-a.reqCh:
			if req == nil {
				return
			}
			a.consumer.Accept(req)
		}
	}
}

func (a *AsyncConsumer) Stop() {
	close(a.stopCh)

	go func() {
		select {
		case a.reqCh <- nil:
		}
	}()

	a.wg.Wait()
}

func (a *AsyncConsumer) Accept(x interface{}) {
	select {
	case <-a.stopCh:
		return
	default:
	}

	select {
	case <-a.stopCh:
		return
	case a.reqCh <- x:
	}
}
