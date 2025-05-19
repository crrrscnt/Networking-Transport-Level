// transport-layer-mars

package consts

import "time"

const (
	// MyHost - Адрес и порт, на котором слушает транспортный уровень Марса
	// MyHost = "0.0.0.0:8081" // Слушаем на всех интерфейсах
	MyHost = "192.168.80.254:8081" // Или конкретный IP, если нужно //  192.168.80.254

	// ReceiveUrl - Адрес прикладного уровня Марса (WebSocket сервер) для отправки собранных сообщений
	ReceiveUrl = "http://localhost:8010/ReceiveMessage" // Убедитесь, что этот адрес доступен с машины, где запущен transport-layer-mars

	// EarthTransportURL - Базовый адрес транспортного уровня Земли для отправки ACK
	EarthTransportURL = "http://192.168.80.254:8080" // Убедитесь, что этот адрес доступен // 192.168.1.4

	ChannelLayerURL = "http://192.168.80.183:3050" // 192.168.1.4

	ReadTimeout       = 10 * time.Second
	WriteTimeout      = 10 * time.Second
	ReadHeaderTimeout = 10 * time.Second

	// SegmentLostError - Текст ошибки при потере сегмента или таймауте сборки
	SegmentLostError = "message_assembly_timeout_or_segment_lost"

	// MessageAssemblyCheckPeriod - Как часто проверять storage на собранные сообщения
	MessageAssemblyCheckPeriod = 1 * time.Second

	// MessageTimeout - Максимальное время ожидания сборки сообщения с момента получения последнего сегмента
	MessageTimeout = 15 * time.Second // Например, 15 секунд
)
