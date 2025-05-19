// transport-layer-mars

package storage

import (
	"fmt"
	"sync"
	"time"
	"transport-layer-mars/internal/consts"
	"transport-layer-mars/internal/utils"
)

type Storage map[time.Time]utils.Message

var storage = Storage{}
var mu sync.Mutex // Мьютекс для доступа к storage

// addMessage создает запись для нового сообщения при получении первого сегмента
func addMessage(segment utils.Segment) {
	storage[segment.SendTime] = utils.Message{
		Received: 0,
		Total:    segment.TotalSegments,
		Last:     time.Now().UTC(),
		Username: segment.Username,
		Segments: make([]string, segment.TotalSegments), // заранее выделяем память
	}
	fmt.Printf("  New message started (ID: %s, Total Segments: %d)\n", segment.SendTime.Format(time.RFC3339Nano), segment.TotalSegments)
}

// AddSegment добавляет полученный сегмент в хранилище
func AddSegment(segment utils.Segment) {
	mu.Lock()
	defer mu.Unlock()

	sendTime := segment.SendTime
	message, found := storage[sendTime]

	// Если сообщение не найдено, и номер сегмента 1, создаем новое сообщение
	if !found && segment.SegmentNumber == 1 {
		addMessage(segment)
		message, _ = storage[sendTime] // Перечитываем, чтобы получить созданное сообщение
	} else if !found {
		// Получен не первый сегмент для неизвестного сообщения - игнорируем или логируем ошибку
		fmt.Printf("  Warning: Received segment %d for unknown message ID %s. Ignoring.\n", segment.SegmentNumber, sendTime.Format(time.RFC3339Nano))
		return
	}

	// Проверяем, не был ли этот сегмент уже получен
	if segment.SegmentNumber > 0 && segment.SegmentNumber <= message.Total && message.Segments[segment.SegmentNumber-1] == "" {
		message.Received++
		message.Last = time.Now().UTC()
		message.Segments[segment.SegmentNumber-1] = segment.SegmentPayload // сохраняем в правильном порядке
		storage[sendTime] = message
		fmt.Printf("  Segment %d/%d added to message ID %s. Received count: %d\n", segment.SegmentNumber, message.Total, sendTime.Format(time.RFC3339Nano), message.Received)
	} else if segment.SegmentNumber > 0 && segment.SegmentNumber <= message.Total {
		fmt.Printf("  Warning: Received duplicate segment %d/%d for message ID %s. Ignoring.\n", segment.SegmentNumber, message.Total, sendTime.Format(time.RFC3339Nano))
	} else {
		fmt.Printf("  Error: Received invalid segment number %d for message ID %s (Total: %d). Ignoring.\n", segment.SegmentNumber, sendTime.Format(time.RFC3339Nano), message.Total)
	}
}

// getMessageText собирает текст сообщения из сегментов
func getMessageText(sendTime time.Time) string {
	result := ""
	// Блокировка не нужна, так как вызывается из ScanStorage, который уже заблокирован
	message, found := storage[sendTime]
	if !found {
		return "" // На всякий случай
	}
	for _, segment := range message.Segments {
		result += segment
	}
	return result
}

// sendFunc - тип функции для отправки собранного сообщения или ошибки
type sendFunc func(body utils.ReceiveRequest)

// ScanStorage периодически сканирует хранилище и отправляет собранные сообщения или ошибки таймаута
func ScanStorage(sender sendFunc) {
	mu.Lock()
	defer mu.Unlock()

	now := time.Now().UTC()
	for sendTime, message := range storage {
		// Если пришли все сегменты
		if message.Received == message.Total {
			fullText := getMessageText(sendTime)
			payload := utils.ReceiveRequest{
				Username: message.Username,
				Text:     fullText,
				SendTime: sendTime,
				Error:    false, // ""
			}
			fmt.Printf("<To App-Layer (Mars WS): transfer assembled message ID %s\n", sendTime.Format(time.RFC3339Nano))
			go sender(payload)        // Запускаем отправку на прикладной уровень Марса
			delete(storage, sendTime) // Удаляем собранное сообщение
		} else if now.Sub(message.Last) > consts.MessageTimeout { // Если сообщение не собирается слишком долго
			fmt.Printf("XXX Message assembly timeout for ID %s (Received %d/%d). Sending error. XXX\n", sendTime.Format(time.RFC3339Nano), message.Received, message.Total)
			payload := utils.ReceiveRequest{
				Username: message.Username,
				Text:     "",
				SendTime: sendTime,
				Error:    true, // consts.SegmentLostError, // Ошибка таймаута/потери сегмента
			}
			go sender(payload)        // Отправляем ошибку на прикладной уровень Марса
			delete(storage, sendTime) // Удаляем незавершенное сообщение
		}
	}
}
