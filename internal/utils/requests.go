package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"
	"transport-layer-earth/internal/consts"
)

type Segment struct {
	SegmentNumber  int       `json:"segment_number"`
	TotalSegments  int       `json:"total_segments"`
	Username       string    `json:"username"`
	SendTime       time.Time `json:"send_time"`
	SegmentPayload string    `json:"payload"`
}

// SendRequest структура тела запроса на /code канального уровня
type SendRequest struct {
	Id       int       `json:"id,omitempty"` // если пустой, то не включается в JSON
	Username string    `json:"username"`
	Text     string    `json:"data"`
	SendTime time.Time `json:"send_time"`
}

type Message struct {
	Received int
	Total    int
	Last     time.Time
	Username string
	Segments []string
}

// ReceiveRequest структура тела запроса на прикладной уровень
type ReceiveRequest struct {
	Username string    `json:"username"`
	Text     string    `json:"data"`
	SendTime time.Time `json:"send_time"`
	Error    bool      `json:"status"`
}

// type ReceiveRequest struct {
// 	Username string    `json:"username"`
// 	Text     string    `json:"data"`
// 	SendTime time.Time `json:"send_time"`
// 	Error    string    `json:"error"`
// }

func SplitMessage(payload string, segmentSize int) []string {
	result := make([]string, 0)

	// RUSSIAN CHARACTERS
	// Since most Russian characters take 2 bytes
	// runes := []rune(payload)
	// // Adjust segment size for runes (approximately)
	// runeSegmentSize := segmentSize / 2 // Since most Russian characters take 2 bytes
	// length := len(runes)
	// segmentCount := int(math.Ceil(float64(length) / float64(runeSegmentSize)))

	length := len(payload) // длина в байтах
	segmentCount := int(math.Ceil(float64(length) / float64(segmentSize)))

	for i := 0; i < segmentCount; i++ {
		// RUSSIAN CHARACTERS
		// start := i * runeSegmentSize
		// end := min((i+1)*runeSegmentSize, length)
		// result = append(result, string(runes[start:end]))

		result = append(result, payload[i*segmentSize:min((i+1)*segmentSize, length)]) // срез делается также по байтам
	}

	return result
}

func SendSegment(body Segment) {
	// Marshal the request body into JSON
	reqBody, _ := json.Marshal(body)

	// Create an HTTP POST request
	req, err := http.NewRequest("POST", consts.CodeUrl, bytes.NewBuffer(reqBody))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return
	}
	//req, _ := http.NewRequest("POST", consts.CodeUrl, bytes.NewBuffer(reqBody))
	// Set the Content-Type header
	req.Header.Add("Content-Type", "application/json")

	// Perform the HTTP request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}

	// Ensure the response body is closed after use
	defer resp.Body.Close()
}

func SendReceiveRequest(body ReceiveRequest) {
	reqBody, err := json.Marshal(body)
	if err != nil {
		fmt.Printf("Error marshalling ReceiveRequest: %v\n", err)
		return
	}

	req, err := http.NewRequest("POST", consts.ReceiveUrl, bytes.NewBuffer(reqBody))
	if err != nil {
		fmt.Printf("Error creating request to App Layer (%s): %v\n", consts.ReceiveUrl, err)
		return
	}
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error sending request to App Layer (%s): %v\n", consts.ReceiveUrl, err)
		return
	}
	defer resp.Body.Close()

	// Проверка статуса ответа
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("App Layer returned non-OK status: %d\n", resp.StatusCode)
		return
	}

	fmt.Printf("Successfully sent message/error to App Layer: %s\n", string(reqBody))
}

type AckRequest struct {
	SendTime      time.Time `json:"send_time"`
	SegmentNumber int       `json:"segment_number"`
}

func SendAck(ack AckRequest) {
	reqBody, _ := json.Marshal(ack)

	req, _ := http.NewRequest("POST", consts.MyHost+"/ack", bytes.NewBuffer(reqBody))
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending ACK:", err)
		return
	}
	defer resp.Body.Close()
}
