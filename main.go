// transport-layer-mars

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"transport-layer-mars/internal/consts"
	"transport-layer-mars/internal/handlers"

	// "transport-layer-mars/internal/kafka" // Удаляем импорт Kafka
	"transport-layer-mars/internal/storage"
	"transport-layer-mars/internal/utils"

	"github.com/gorilla/mux"
)

func main() {
	done := make(chan struct{})

	// Periodically scan storage to send assembled messages
	go func() {
		// Увеличим интервал, т.к. ретраи сегментов больше не нужны
		ticker := time.NewTicker(consts.MessageAssemblyCheckPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Передаем функцию для отправки собранного сообщения на прикладной уровень Марса
				storage.ScanStorage(utils.SendReceiveRequest)
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
	r.HandleFunc("/transfer", handlers.HandleTransfer).Methods(http.MethodPost, http.MethodOptions) // Получение сегментов от C-Layer Земли
	r.HandleFunc("/ack", handlers.HandleACK).Methods(http.MethodPost, http.MethodOptions)           // Получение ACK от C-Layer Земли
	http.Handle("/", r)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// Start HTTP server
	srv := http.Server{
		Handler:           r,
		Addr:              consts.MyHost, // Используем константу
		ReadTimeout:       consts.ReadTimeout,
		WriteTimeout:      consts.WriteTimeout,
		ReadHeaderTimeout: consts.ReadHeaderTimeout,
	}
	go func() {
		fmt.Printf("Mars Transport Layer server started on %s\n", srv.Addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Printf("Server error: %v\n", err)
			close(done) // Закрываем канал done при ошибке сервера
		} else {
			fmt.Println("Server stopped gracefully")
		}
	}()

	// Graceful shutdown
	sig := <-signalCh
	fmt.Printf("Received signal: %v\n", sig)
	close(done) // Сигнализируем горутинам о завершении

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		fmt.Printf("Server shutdown failed: %v\n", err)
	} else {
		fmt.Println("Server shutdown complete")
	}
}
