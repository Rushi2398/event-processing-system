package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Rushi2398/event-processing-system/consumer/observability"
	"github.com/Rushi2398/event-processing-system/consumer/service"
	"github.com/Rushi2398/event-processing-system/consumer/worker"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

// config holds all runtime configuration parsed from environment variables.
type config struct {
	brokers           []string
	topic             string
	groupID           string
	redisAddr         string
	postgresURL       string
	workerCount       int
	retryWorkerCount  int
	retryLimit        int
	batchSize         int
	batchFlushMs      int
	observabilityAddr string
}

func main() {
	if err := godotenv.Load("../.env"); err != nil {
		log.Println("No .env file found, relying on environment variables")
	}
	cfg := mustLoadConfig()

	// JSON structured logging — machine-parseable, works with Loki/Datadog/etc.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// Register all Prometheus metrics before any goroutines start recording.
	observability.Register()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	var obsWg sync.WaitGroup
	var batcherWg sync.WaitGroup

	// --- Infrastructure clients ---
	consumer := service.NewConsumer(cfg.brokers, cfg.topic, cfg.groupID)
	redisClient := service.NewRedisClient(cfg.redisAddr)
	pg, err := service.NewPostgres(cfg.postgresURL)
	if err != nil {
		slog.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}

	observability.StartObservabilityServer(ctx, cfg.observabilityAddr, redisClient.Ping, pg.Ping, &obsWg)

	// --- Batcher ---
	batcher := worker.NewBatcher(pg, cfg.batchSize, time.Duration(cfg.batchFlushMs)*time.Millisecond)
	batcher.Start(&batcherWg)

	// --- Graceful shutdown ---
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal %s, initiating shutdown...", sig)
		cancel()
	}()

	// --- Queue depth poller ---
	// Updates Prometheus gauges for retry_queue and dlq every 15 seconds.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if depth, err := redisClient.RetryQueueDepth(ctx); err == nil {
					observability.RetryQueueDepth.Set(float64(depth))
				}
				if depth, err := redisClient.DLQDepth(ctx); err == nil {
					observability.DLQDepth.Set(float64(depth))
				}
			}
		}
	}()

	// --- Retry subsystem ---
	// The scheduler moves due events from the sorted set into retry_queue.
	// The retry worker pool drains retry_queue via BRPop.
	worker.StartRetryScheduler(ctx, redisClient, &wg)
	worker.StartRetryWorkers(ctx, redisClient, batcher, &wg, cfg.retryLimit, cfg.retryWorkerCount)

	jobs := make(chan kafka.Message, cfg.workerCount*2)

	for i := 0; i < cfg.workerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			slog.Info("worker started", "worker_id", id)
			for msg := range jobs {
				slog.Info("[worker] processing message", "worker_id", id, "msg_offset", msg.Offset)

				if err := worker.ProcessEvent(ctx, msg.Value, redisClient, batcher); err != nil {
					slog.Error("[worker] processing failed", "worker_id", id, "error", err)

					if hErr := worker.HandleRetry(ctx, msg.Value, redisClient, cfg.retryLimit); hErr != nil {
						slog.Error("[worker] CRITICAL: failed to schedule retry", "worker_id", id, "error", hErr)
					}
					// Do NOT commit the offset — Kafka will redeliver if the
					// consumer restarts before the retry is processed. This is
					// acceptable because ProcessEvent is idempotent via Redis lock.
					continue
				}

				// commit ONLY after success
				if err := consumer.CommitMessage(ctx, msg); err != nil {
					slog.Error("commit failed", "worker_id", id, "error", err)
				}
			}
		}(i)
	}

	slog.Info("consumer started",
		"workers", cfg.workerCount,
		"retry_workers", cfg.retryWorkerCount,
		"retry_limit", cfg.retryLimit,
		"batch_size", cfg.batchSize,
		"topic", cfg.topic,
	)

	// --- Main consume loop ---
	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled — stopping consumer loop...")
			goto shutdown
		default:
			msg, err := consumer.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					goto shutdown
				}
				slog.Error("error fetching message", "error", err)
				continue
			}
			jobs <- msg
		}
	}
shutdown:
	slog.Info("draining job queue and waiting for workers to finish...")
	close(jobs)
	wg.Wait()

	// Stop the batcher only after all workers have finished their last Submit call. Calling Stop() while a Submit is in-flight would panic.
	slog.Info("stopping batcher and flushing remaining events...")
	batcher.Stop()
	batcherWg.Wait()

	slog.Info("shutting down observability server...")
	obsWg.Wait()

	slog.Info("closing resources...")
	consumer.Close()
	pg.Close()

	slog.Info("shutdown complete")
}

// mustLoadConfig reads and validates all required environment variables.
// It calls log.Fatal if any required variable is missing or invalid,
// preventing silent misconfigurations (e.g. 0 workers, 0 retries).
func mustLoadConfig() config {
	cfg := config{
		brokers:     strings.Split(mustEnv("KAFKA_BROKERS"), ","),
		topic:       mustEnv("KAFKA_TOPIC"),
		groupID:     mustEnv("KAFKA_GROUP_ID"),
		redisAddr:   mustEnv("REDIS_ADDR"),
		postgresURL: mustEnv("POSTGRES_URL"),
	}

	cfg.workerCount = mustEnvInt("WORKER_COUNT", 1, 1000)
	cfg.retryLimit = mustEnvInt("RETRY_LIMIT", 1, 20)
	cfg.batchSize = mustEnvIntDefault("BATCH_SIZE", 100, 1, 1000)
	cfg.batchFlushMs = mustEnvIntDefault("BATCH_FLUSH_MS", 500, 10, 10000)

	cfg.observabilityAddr = envDefault("OBSERVABILITY_ADDR", ":9090")

	// RETRY_WORKER_COUNT defaults to 25% of main worker count if not set.
	if v := os.Getenv("RETRY_WORKER_COUNT"); v != "" {
		cfg.retryWorkerCount = mustEnvInt("RETRY_WORKER_COUNT", 1, 100)
	} else {
		cfg.retryWorkerCount = cfg.workerCount / 4
		if cfg.retryWorkerCount < 1 {
			cfg.retryWorkerCount = 1
		}
	}

	slog.Info("config loaded",
		"workers", cfg.workerCount,
		"retry_workers", cfg.retryWorkerCount,
		"retry_limit", cfg.retryLimit,
		"batch_size", cfg.batchSize,
		"batch_flush_ms", cfg.batchFlushMs,
		"observability_addr", cfg.observabilityAddr,
		"topic", cfg.topic,
	)

	return cfg
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required environment variable %s is not set", key)
	}
	return v
}

func mustEnvInt(key string, min, max int) int {
	raw := os.Getenv(key)
	if raw == "" {
		log.Fatalf("required environment variable %s is not set", key)
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		log.Fatalf("environment variable %s must be an integer, got: %q", key, raw)
	}
	if v < min || v > max {
		log.Fatalf("environment variable %s must be between %d and %d, got: %d", key, min, max, v)
	}
	return v
}

// mustEnvIntDefault returns the parsed integer value of key, or defaultVal if the variable is not set. Fatals if the variable is set but invalid.
func mustEnvIntDefault(key string, defaultVal, min, max int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		log.Fatalf("environment variable %s must be an integer, got: %q", key, raw)
	}
	if v < min || v > max {
		log.Fatalf("environment variable %s must be between %d and %d, got: %d", key, min, max, v)
	}
	return v
}

func envDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
