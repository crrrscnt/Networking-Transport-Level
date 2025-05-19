// transport-layer-mars

package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"transport-layer-mars/internal/storage"
	"transport-layer-mars/internal/utils"
)

// HandleTransfer обрабатывает получение сегментов от канального уровня Земли
// @Summary Получение сегмента сообщения
// @Description Принимает сегмент сообщения от канального уровня Земли, сохраняет его и отправляет ACK
// @Tags segments
// @Accept json
// @Produce json
// @Param segment body utils.Segment true "Сегмент сообщения"
// @Success 200 "Сегмент успешно получен"
// @Failure 400 "Некорректный запрос"
// @Router /transfer [post]
func HandleTransfer(w http.ResponseWriter, r *http.Request) {
	// Read request body (segment from data link layer)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Printf("Error reading /transfer request body: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse segment into structure
	segment := utils.Segment{}
	if err = json.Unmarshal(body, &segment); err != nil {
		fmt.Printf("Error unmarshalling segment in /transfer: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Add segment to storage
	fmt.Printf(">From C-Layer (Earth): received segment:> %+v\n", segment)
	storage.AddSegment(segment) // Добавляем сегмент для последующей сборки

	// Отправляем ACK обратно на транспортный уровень Земли
	ack := utils.AckRequest{
		SendTime:      segment.SendTime,
		SegmentNumber: segment.SegmentNumber,
	}
	fmt.Printf("<To T-Layer (Earth): sending ACK for segment %d\n", ack.SegmentNumber)
	go utils.SendAck(ack) // Отправляем ACK напрямую

	// Respond with 200 OK
	w.WriteHeader(http.StatusOK)
}

// HandleACK обрабатывает получение ACK от канального уровня Земли
// @Summary Получение подтверждения (ACK)
// @Description Принимает подтверждение (ACK) от канального уровня Земли
// @Tags ack
// @Accept json
// @Produce json
// @Param ack body utils.AckRequest true "Подтверждение получения сегмента"
// @Success 200 "ACK успешно получен"
// @Failure 400 "Некорректный запрос"
// @Router /ack [post]
func HandleACK(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Printf("Error reading /ack request body: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var ack utils.AckRequest
	if err = json.Unmarshal(body, &ack); err != nil {
		fmt.Printf("Error unmarshalling ACK in /ack: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	fmt.Printf(">From C-Layer (Earth): received ACK for segment: %+v\n", ack)

	// Просто подтверждаем получение ACK
	w.WriteHeader(http.StatusOK)
}
