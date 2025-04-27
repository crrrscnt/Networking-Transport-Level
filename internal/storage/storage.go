package storage

import (
	"fmt"
	"sync"
	"time"
	"transport-layer-earth/internal/consts"
	"transport-layer-earth/internal/utils"
)

type Storage map[time.Time]utils.Message

var storage = Storage{}

func addMessage(segment utils.Segment) {
	storage[segment.SendTime] = utils.Message{
		Received: 0,
		Total:    segment.TotalSegments,
		Last:     time.Now().UTC(),
		Username: segment.Username,
		Segments: make([]string, segment.TotalSegments), // заранее выделяем память, это важно!
	}
}

func AddSegment(segment utils.Segment) {
	// используем мьютекс, чтобы избежать конкуретного доступа к хранилищу
	mu := &sync.Mutex{}
	mu.Lock()
	defer mu.Unlock()

	// если это первый сегмент сообщения, создаем пустое сообщение
	sendTime := segment.SendTime
	_, found := storage[sendTime]
	if !found {
		addMessage(segment)
	}

	// добавляем в сообщение информацию о сегменте
	message, _ := storage[sendTime]
	message.Received++
	message.Last = time.Now().UTC()
	message.Segments[segment.SegmentNumber-1] = segment.SegmentPayload // сохраняем правильный порядок сегментов
	storage[sendTime] = message
}

func getMessageText(sendTime time.Time) string {
	result := ""
	message, _ := storage[sendTime]
	for _, segment := range message.Segments {
		result += segment
	}
	return result
}

// Функция для сканирования хранилища и отправки сообщений, которые готовы
var pendingSegments = map[string]*PendingSegment{}
var muPending sync.Mutex // отдельный мьютекс для pending

type PendingSegment struct {
	Segment    utils.Segment
	SentAt     time.Time
	RetryCount int
}

func getSegmentKey(sendTime time.Time, segmentNumber int) string {
	return fmt.Sprintf("%d_%d", sendTime.UnixNano(), segmentNumber)
}

// Добавляем сегмент в ожидающие подтверждения
func AddPending(segment utils.Segment) {
	muPending.Lock()
	defer muPending.Unlock()

	key := getSegmentKey(segment.SendTime, segment.SegmentNumber)
	pendingSegments[key] = &PendingSegment{
		Segment:    segment,
		SentAt:     time.Now().UTC(),
		RetryCount: 0,
	}
}

// Пришёл ACK — удаляем сегмент
func AckReceived(sendTime time.Time, segmentNumber int) {
	muPending.Lock()
	defer muPending.Unlock()

	key := getSegmentKey(sendTime, segmentNumber)
	delete(pendingSegments, key)
}

// Периодически сканируем хранилище и отправляем сообщения, которые готовы
type sendFunc func(body utils.ReceiveRequest)

func ScanStorage(sender sendFunc) {
	mu := &sync.Mutex{}
	mu.Lock()
	defer mu.Unlock()

	// Сначала проверяем pending сегменты
	muPending.Lock()
	for key, pending := range pendingSegments {
		if time.Since(pending.SentAt) > consts.AckTimeout {
			if pending.RetryCount >= consts.MaxRetries {
				fmt.Printf("XXX Segment failed after max retries: %+v XXX\n", pending.Segment)

				payload := utils.ReceiveRequest{
					Username: pending.Segment.Username,
					Text:     "",
					SendTime: pending.Segment.SendTime,
					Error:    consts.SegmentLostError,
				}
				go sender(payload)
				delete(pendingSegments, key)
			} else {
				fmt.Printf("!!! Retrying segment: %+v (attempt %d) !!!\n", pending.Segment, pending.RetryCount+1)
				go utils.SendSegment(pending.Segment)
				pending.SentAt = time.Now().UTC()
				pending.RetryCount++
			}
		}
	}
	muPending.Unlock()

	payload := utils.ReceiveRequest{}
	for sendTime, message := range storage {
		if message.Received == message.Total { // если пришли все сегменты
			payload = utils.ReceiveRequest{
				Username: message.Username,
				Text:     getMessageText(sendTime), // склейка сообщения
				SendTime: sendTime,
				Error:    "",
			}
			fmt.Printf("<To App-Layer: transfer message to Mars:< %+v\n", payload)
			go sender(payload)        // запускаем горутину с отправкой на прикладной уровень, не будем дожидаться результата ее выполнения
			delete(storage, sendTime) // не забываем удалять
		} else if time.Since(message.Last) > consts.KafkaReadPeriod+time.Second { // если канальный уровень потерял сегмент
			payload = utils.ReceiveRequest{
				Username: message.Username,
				Text:     "",
				SendTime: sendTime,
				Error:    consts.SegmentLostError, // ошибка
			}
			fmt.Printf("sent error: %+v\n", payload)
			go sender(payload)        // запускаем горутину с отправкой на прикладной уровень, не будем дожидаться результата ее выполнения
			delete(storage, sendTime) // не забываем удалять
		}
	}
}
