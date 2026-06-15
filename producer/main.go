package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Rushi2398/event-processing-system/producer/handler"
	"github.com/Rushi2398/event-processing-system/producer/service"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if err := godotenv.Load("../.env"); err != nil {
		slog.Info("no .env file found, relying on environment variables")
	}

	brokers := strings.Split(mustEnv("KAFKA_BROKERS"), ",")
	topic := mustEnv("KAFKA_TOPIC")

	producer := service.NewProducer(brokers, topic)
	handler := handler.NewEventHandler(producer)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	// Health endpoints — required for Kubernetes probes.
	r.GET("/healthz/live", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	r.GET("/healthz/ready", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	r.POST("/events", handler.PublishEvent)

	server := &http.Server{
		Addr:         ":8080",
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Graceful shutdown on SIGTERM / SIGINT.
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		sig := <-sigChan
		slog.Info("shutdown signal received", "signal", sig.String())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			slog.Error("server shutdown error", "error", err)
		}

		if err := producer.Close(); err != nil {
			slog.Error("failed to close kafka writer", "error", err)
		}
	}()

	slog.Info("producer started", "addr", ":8080", "topic", topic, "brokers", brokers)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}

	slog.Info("shutdown complete")
	r.Run(":8080")
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required environment variable not set", "key", key)
		os.Exit(1)
	}
	return v
}
