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
type Segment struct {
	SegmentNumber  int       `json:"segment_number"`
	TotalSegments  int       `json:"total_segments"`
	Username       string    `json:"username"`
	SendTime       time.Time `json:"send_time"`
	SegmentPayload string    `json:"payload"`
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
type ReceiveRequest struct {
	Username string    `json:"username"`
	Text     string    `json:"data"` // Поле изменено на data для совместимости с app-layer-mars-ws
	SendTime time.Time `json:"send_time"`
	Error    string    `json:"error,omitempty"` // Сделаем ошибку опциональной
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
type AckRequest struct {
	SendTime      time.Time `json:"send_time"`
	SegmentNumber int       `json:"segment_number"`
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
