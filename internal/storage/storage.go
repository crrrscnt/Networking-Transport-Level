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
var storageMu sync.Mutex // один на всех мьютекс

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
	// mu := &sync.Mutex{}
	// mu.Lock()
	// defer mu.Unlock()
	storageMu.Lock()
	defer storageMu.Unlock()

	// fmt.Printf("DEBUG AddSegment: Received segment: %+v\n", segment) // Не вызывается на отправителе

	// если это первый сегмент сообщения, создаем пустое сообщение
	sendTime := segment.SendTime
	_, found := storage[sendTime]
	if !found {
		// fmt.Printf("DEBUG AddSegment: Message not found for %v. Calling addMessage with TotalSegments: %d\n", sendTime, segment.TotalSegments) // Не вызывается
		addMessage(segment)
	}

	// добавляем в сообщение информацию о сегменте
	message, _ := storage[sendTime]
	message.Received++
	message.Last = time.Now().UTC()
	message.Segments[segment.SegmentNumber-1] = segment.SegmentPayload // сохраняем правильный порядок сегментов
	storage[sendTime] = message
	// fmt.Printf("DEBUG AddSegment: Stored message for %v: %+v\n", sendTime, storage[sendTime]) // Не вызывается
}

// если сообщение завершено
// На отправителе она вернет пустую строку, если срез Segments не был заполнен.
// Для отправителя Text в ReceiveRequest может быть нерелевантен, только статус Error.
func getMessageText(sendTime time.Time) string {
	result := ""
	message, found := storage[sendTime]
	if found {
		for _, segmentPayload := range message.Segments {
			result += segmentPayload
		}
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

// Она инициализирует отслеживание для всего сообщения в основном 'storage'.
func StartTrackingOutgoingMessage(sendTime time.Time, username string, totalSegmentsInMessage int) {
	storageMu.Lock()
	defer storageMu.Unlock()

	if _, found := storage[sendTime]; !found {
		// fmt.Printf("DEBUG StartTrackingOutgoingMessage: Initializing tracking for message at %v, User: %s, TotalSegments: %d\n", sendTime, username, totalSegmentsInMessage)
		storage[sendTime] = utils.Message{
			Received: 0, // Будет считать полученные ACK для этого сообщения
			Total:    totalSegmentsInMessage,
			Last:     time.Now().UTC(),
			Username: username,
			Segments: nil, // Отправителю может не потребоваться хранить полезную нагрузку сегментов для отслеживания ACK
			// Если текст сообщения нужен в payload для App-Layer, здесь можно сохранить сегменты:
			// Segments: make([]string, totalSegmentsInMessage), // и заполнять их по мере отправки, если нужно
		}
	}
}

// Пришёл ACK — удаляем сегмент
func AckReceived(sendTime time.Time, segmentNumber int) {
	// muPending.Lock()
	// defer muPending.Unlock()

	// key := getSegmentKey(sendTime, segmentNumber)
	// delete(pendingSegments, key)

	muPending.Lock()
	key := getSegmentKey(sendTime, segmentNumber)
	delete(pendingSegments, key)
	muPending.Unlock()

	// Затем обновляем общий статус сообщения в основном 'storage'
	storageMu.Lock()
	defer storageMu.Unlock()

	if message, found := storage[sendTime]; found {
		// Увеличиваем счетчик полученных ACK, только если сообщение еще не завершено
		if message.Received < message.Total {
			message.Received++
		}
		message.Last = time.Now().UTC()
		storage[sendTime] = message // Обновляем сообщение в хранилище
		// fmt.Printf("DEBUG AckReceived: Updated message in main storage: SendTime: %v, Received ACKs: %d, Total Segments: %d\n", sendTime, message.Received, message.Total)
	} else {
		// Этот случай означает, что StartTrackingOutgoingMessage не была вызвана для этого sendTime,
		// или сообщение уже было обработано и удалено ScanStorage.
		// fmt.Printf("DEBUG AckReceived: Message for SendTime %v not found in main storage. Cannot update ACK count.\n", sendTime)
	}
}

// Периодически сканируем хранилище и отправляем сообщения, которые готовы
type sendFunc func(body utils.ReceiveRequest)

func ScanStorage(sender sendFunc) {
	// mu := &sync.Mutex{}
	// mu.Lock()
	// defer mu.Unlock()

	// Сначала проверяем pending сегменты
	muPending.Lock()
	for key, pending := range pendingSegments {
		if time.Since(pending.SentAt) > consts.AckTimeout {
			if pending.RetryCount >= consts.MaxRetries {
				fmt.Printf("XXX Segment failed after max retries: %+v XXX\n", pending.Segment)

				// payload := utils.ReceiveRequest{
				// 	Username: pending.Segment.Username,
				// 	Text:     "",
				// 	SendTime: pending.Segment.SendTime,
				// 	Error:    consts.SegmentLostError,
				// }
				payload := utils.ReceiveRequest{
					Username: pending.Segment.Username,
					Text:     "",
					SendTime: pending.Segment.SendTime,
					Error:    true, // MODIFIED: Set to true for error
				}
				go sender(payload)
				delete(pendingSegments, key)

				// Если сегмент окончательно не удался, удаляем также основное сообщение из отслеживания,
				// чтобы оно не вызвало таймаут позже.
				storageMu.Lock()
				if _, ok := storage[pending.Segment.SendTime]; ok {
					// fmt.Printf("DEBUG ScanStorage: Segment failure for message %v. Removing message from main tracking.\n", pending.Segment.SendTime)
					delete(storage, pending.Segment.SendTime)
				}
				storageMu.Unlock()
			} else {
				fmt.Printf("!!! Retrying segment: %+v (attempt %d) !!!\n", pending.Segment, pending.RetryCount+1)
				go utils.SendSegment(pending.Segment)
				pending.SentAt = time.Now().UTC()
				pending.RetryCount++
			}
		}
	}
	muPending.Unlock()

	storageMu.Lock()
	defer storageMu.Unlock()

	// payload := utils.ReceiveRequest{}
	for sendTime, message := range storage {
		// fmt.Printf("\n DEBUG ScanStorage: Checking message in main storage: SendTime: %v, Received ACKs: %d, Total Segments: %d\n", sendTime, message.Received, message.Total)
		if message.Received == message.Total { // если пришли все сегменты
			// payload = utils.ReceiveRequest{
			// 	Username: message.Username,
			// 	Text:     getMessageText(sendTime), // склейка сообщения
			// 	SendTime: sendTime,
			// 	Error:    "",
			// }
			payload := utils.ReceiveRequest{
				Username: message.Username,
				Text:     getMessageText(sendTime), // склейка сообщения
				SendTime: sendTime,
				Error:    false, // MODIFIED: Set to false for success
			}
			fmt.Printf("<To App-Layer: transfer message to Mars:< %+v\n", payload)
			go sender(payload)        // запускаем горутину с отправкой на прикладной уровень, не будем дожидаться результата ее выполнения
			delete(storage, sendTime) // не забываем удалять
		}
		// --- Исх ---

		// else if time.Since(message.Last) > consts.KafkaReadPeriod+time.Second { // если канальный уровень потерял сегмент
		// 	// payload = utils.ReceiveRequest{
		// 	// 	Username: message.Username,
		// 	// 	Text:     "",
		// 	// 	SendTime: sendTime,
		// 	// 	Error:    consts.SegmentLostError, // ошибка
		// 	// }
		// 	payload = utils.ReceiveRequest{
		// 		Username: message.Username,
		// 		Text:     "",
		// 		SendTime: sendTime,
		// 		Error:    true, // MODIFIED: Set to true for error
		// 	}
		// 	fmt.Printf("sent error: %+v\n", payload)
		// 	go sender(payload)        // запускаем горутину с отправкой на прикладной уровень, не будем дожидаться результата ее выполнения
		// 	delete(storage, sendTime) // не забываем удалять
		// }

		// --- Исправ ---
		//  else if time.Since(message.Last) > consts.AckTimeout*consts.MaxRetries { // Таймаут для всего сообщения (определите consts.MessageTimeout)
		// 	payload := utils.ReceiveRequest{
		// 		Username: message.Username,
		// 		Text:     "",
		// 		SendTime: sendTime,
		// 		Error:    true,
		// 	}
		// 	fmt.Printf("XXX Message timed out: SendTime: %v, Received ACKs: %d, Total Segments: %d XXX\n", sendTime, message.Received, message.Total)
		// 	go sender(payload)
		// 	delete(storage, sendTime)
		// }
	}
}
