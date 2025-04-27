package consts

import "time"

const (
	MyHost = "localhost:8080"
	// MyHost = "192.168.0.106:8080"
	// CodeUrl           = "http://localhost:8080/code" // адрес канального уровня
	CodeUrl           = "http://192.168.1.4:3500/code"
	ReceiveUrl        = "http://localhost:8001/receive" // адрес websocket-сервера прикладного уровня
	SegmentSize       = 200
	ReadTimeout       = 10 * time.Second
	WriteTimeout      = 10 * time.Second
	ReadHeaderTimeout = 10 * time.Second
	SegmentLostError  = "lost"
	KafkaAddr         = "localhost:29092"
	KafkaTopic        = "segments"
	KafkaReadPeriod   = 2 * time.Second
	AckTimeout        = 5 * time.Second
	MaxRetries        = 3
)
