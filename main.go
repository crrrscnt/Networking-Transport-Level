package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"transport-layer-earth/internal/consts"
	"transport-layer-earth/internal/handlers"
	"transport-layer-earth/internal/kafka"
	"transport-layer-earth/internal/storage"
	"transport-layer-earth/internal/utils"

	"github.com/gorilla/mux"
	"github.com/rs/cors"
)

func main() {
	done := make(chan struct{})

	// Start Kafka consumer to send segments to the data link layer
	go func() {
		defer close(done)
		if err := kafka.ReadFromKafka(); err != nil {
			fmt.Printf("Kafka reader error: %v\n", err)
		}
	}()

	// Periodically scan storage
	go func() {
		ticker := time.NewTicker(consts.KafkaReadPeriod) // передача канальному ур
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				storage.ScanStorage(utils.SendReceiveRequest)
				// fmt.Print("отсканено")
			case <-done:
				return
			}
		}
	}()

	// Set up router
	r := mux.NewRouter()
	r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	})
	// r.HandleFunc("/transfer", handlers.HandleTransfer).Methods(http.MethodPost, http.MethodOptions)
	r.HandleFunc("/send", handlers.HandleSend).Methods(http.MethodPost, http.MethodOptions)
	r.HandleFunc("/ack", handlers.HandleACK).Methods(http.MethodPost, http.MethodOptions)
	http.Handle("/", r)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	corsHandler := cors.Default().Handler(r)
	// Start HTTP server
	srv := http.Server{
		Handler: corsHandler,
		Addr:    ":8080",
		// запуск таймеров при подключении клиента
		ReadTimeout: consts.ReadTimeout, // макс время, за к-е клиент должен прочитать запрос;
		// если клиент не присылает данные дольше readtimout, то обрыв соединения

		WriteTimeout:      consts.WriteTimeout, // макс время, за к-е происходит отправка клиенту ответа
		ReadHeaderTimeout: consts.ReadHeaderTimeout,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			fmt.Println("Server stopped")
		}
	}()
	fmt.Println("Server started")

	// Graceful shutdown
	sig := <-signalCh
	fmt.Printf("Received signal: %v\n", sig)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		fmt.Printf("Server shutdown failed: %v\n", err)
	}
}
