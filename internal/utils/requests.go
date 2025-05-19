// transport-layer-mars

package utils

import (
	"bytes"
	"encoding/json"
	"fmt"

	// "math" // Больше не нужен
	"net/http"
	"time"
	"transport-layer-mars/internal/consts"
)

// Segment - структура сегмента, получаемого от канального уровня Земли
// @Description Структура сегмента сообщения
type Segment struct {
	// Номер сегмента в сообщении
	SegmentNumber int `json:"segment_number" example:"1"`
	// Общее количество сегментов в сообщении
	TotalSegments int `json:"total_segments" example:"3"`
	// Имя пользователя, отправившего сообщение
	Username string `json:"username" example:"user123"`
	// Время отправки сообщения
	SendTime time.Time `json:"send_time" example:"2023-01-01T12:00:00Z"`
	// Полезная нагрузка сегмента (часть сообщения)
	SegmentPayload string `json:"payload" example:"Часть сообщения"`
}

// Message - структура для хранения собираемого сообщения в storage
type Message struct {
	Received int       // Количество полученных сегментов
	Total    int       // Общее количество сегментов
	Last     time.Time // Время получения последнего сегмента
	Username string
	Segments []string // Массив для хранения текста сегментов
}

// ReceiveRequest - структура тела запроса на прикладной уровень Марса (WebSocket сервер)
// @Description Структура запроса на прикладной уровень Марса
type ReceiveRequest struct {
	// Имя пользователя, отправившего сообщение
	Username string `json:"Username" example:"user123"` // `json:"username"
	// Текст сообщения
	Text string `json:"Text" example:"Полный текст сообщения"` //`json:"data"
	// Время отправки сообщения
	SendTime time.Time `json:"SendTime" example:"2023-01-01T12:00:00Z"` // `json:"send_time"
	// Текст ошибки (если есть)
	Error bool `json:"Error" example:"message_assembly_timeout_or_segment_lost"` // `json:"error,omitempty"
}

// SendReceiveRequest отправляет собранное сообщение (или ошибку) на прикладной уровень Марса
func SendReceiveRequest(body ReceiveRequest) {
	reqBody, err := json.Marshal(body)
	if err != nil {
		fmt.Printf("Error marshalling ReceiveRequest: %v\n", err)
		return
	}

	req, err := http.NewRequest("POST", consts.ReceiveUrl, bytes.NewBuffer(reqBody))
	if err != nil {
		fmt.Printf("Error creating request to Mars App Layer: %v\n", err)
		return
	}
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second} // Добавим таймаут
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error sending request to Mars App Layer (%s): %v\n", consts.ReceiveUrl, err)
		return // Не возвращаем ошибку, просто логируем
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Mars App Layer returned non-OK status: %d\n", resp.StatusCode)
	} else {
		fmt.Printf("  Successfully sent message/error to Mars App Layer (ID: %s)\n", body.SendTime.Format(time.RFC3339Nano))
	}
}

// AckRequest - структура для отправки ACK на транспортный уровень Земли
// @Description Структура подтверждения получения сегмента
type AckRequest struct {
	// Время отправки исходного сообщения
	SendTime time.Time `json:"send_time" example:"2023-01-01T12:00:00Z"`
	// Номер сегмента, для которого отправляется подтверждение
	SegmentNumber int `json:"segment_number" example:"1"`
}

// SendAck отправляет ACK на транспортный уровень Земли
func SendAck(ack AckRequest) {
	reqBody, err := json.Marshal(ack)
	if err != nil {
		fmt.Printf("Error marshalling AckRequest: %v\n", err)
		return
	}

	// Формируем URL для отправки ACK
	ackUrl := consts.ChannelLayerURL + "/ack"
	req, err := http.NewRequest("POST", ackUrl, bytes.NewBuffer(reqBody))
	if err != nil {
		fmt.Printf("Error creating ACK request to Earth Transport Layer: %v\n", err)
		return
	}
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second} // Добавим таймаут
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error sending ACK to Earth Transport Layer (%s): %v\n", ackUrl, err)
		return
	}
	defer resp.Body.Close()

	// Проверяем статус ответа. Если не OK, логируем ошибку.
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Earth Transport Layer returned non-OK status for ACK: %d\n", resp.StatusCode)
	}
}
