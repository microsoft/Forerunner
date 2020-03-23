package emulator

type LogSerializer interface {
	Serialize(interface{}) ([]byte, error)
}

type LogDeserializer interface {
	Deserialize([]byte) interface{}
}

type LogWriter interface {
	writeln([]byte)
}

type LogReader interface {
	readln() (line []byte, ok bool)
}

type Stoppable interface {
	Stop()
}
