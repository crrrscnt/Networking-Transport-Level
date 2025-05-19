package consts

import "time"

const (
	MyHost = "192.168.179.254:8080"
	// MyHost = "192.168.0.106:8080"
	// CodeUrl           = "http://localhost:8080/code" // адрес канального уровня
	CodeUrl           = "http://192.168.179.183:3050/Code"      // 192.168.80.183 192.168.1.4
	ReceiveUrl        = "http://localhost:8005/ReceiveResponse" // адрес websocket-сервера прикладного уровня 192.168.80.152
	SegmentSize       = 200
	ReadTimeout       = 10 * time.Second
	WriteTimeout      = 10 * time.Second
	ReadHeaderTimeout = 30 * time.Second
	SegmentLostError  = "lost"
	KafkaAddr         = "localhost:29092"
	KafkaTopic        = "segments"
	KafkaReadPeriod   = 2 * time.Second // scanStorage
	AckTimeout        = 5 * time.Second // ожидаем ACK в течение 5 секунд
	MaxRetries        = 3
)
