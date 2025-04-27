package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"transport-layer-earth/internal/consts"
	"transport-layer-earth/internal/kafka"
	"transport-layer-earth/internal/storage"
	"transport-layer-earth/internal/utils"
)

func HandleSend(w http.ResponseWriter, r *http.Request) {
	// Read request body (message from application layer)
	body, err := io.ReadAll(r.Body)
	fmt.Printf(">From App-Layer: Earth sends messages.>\n")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse message into structure
	message := utils.SendRequest{}
	if err = json.Unmarshal(body, &message); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Respond immediately with 200 OK
	w.WriteHeader(http.StatusOK)

	// Split message into segments
	segments := utils.SplitMessage(message.Text, consts.SegmentSize)
	total := len(segments)

	// Send segments to Kafka
	for i, segment := range segments {
		payload := utils.Segment{
			SegmentNumber:  i + 1,
			TotalSegments:  total,
			Username:       message.Username,
			SendTime:       message.SendTime,
			SegmentPayload: segment,
		}
		if err := kafka.WriteToKafka(payload); err != nil {
			fmt.Printf("Error writing to Kafka: %v\n", err)
			continue
		}
		fmt.Printf("<To Kafka: queued segment:< %+v\n", payload)
	}
}

func HandleTransfer(w http.ResponseWriter, r *http.Request) {
	// Read request body (segment from data link layer)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse segment into structure
	segment := utils.Segment{}
	if err = json.Unmarshal(body, &segment); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Add segment to storage
	fmt.Printf(">From C-Layer: received segment:> %+v\n", segment)
	storage.AddSegment(segment)

	// Отправляем ACK обратно на Землю
	ack := utils.AckRequest{
		SendTime:      segment.SendTime,
		SegmentNumber: segment.SegmentNumber,
	}
	go utils.SendAck(ack)

	// Respond with 200 OK
	w.WriteHeader(http.StatusOK)
}

// Обработчик для получения ACK от C-Layer
func HandleACK(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var ack utils.AckRequest
	if err = json.Unmarshal(body, &ack); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	fmt.Printf(">From C-Layer: received ACKfor segment: %+v\n", ack)
	storage.AckReceived(ack.SendTime, ack.SegmentNumber)

	w.WriteHeader(http.StatusOK)
}
