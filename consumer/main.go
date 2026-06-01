package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Rushi2398/event-processing-system/consumer/service"
	"github.com/Rushi2398/event-processing-system/consumer/worker"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

// config holds all runtime configuration parsed from environment variables.
type config struct {
	brokers          []string
	topic            string
	groupID          string
	redisAddr        string
	postgresURL      string
	workerCount      int
	retryWorkerCount int
	retryLimit       int
	batchSize        int
	batchFlushMs     int
}

func main() {
	if err := godotenv.Load("../.env"); err != nil {
		log.Println("No .env file found, relying on environment variables")
	}
	cfg := mustLoadConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	var batcherWg sync.WaitGroup

	// --- Infrastructure clients ---
	consumer := service.NewConsumer(cfg.brokers, cfg.topic, cfg.groupID)
	redisClient := service.NewRedisClient(cfg.redisAddr)
	pg, err := service.NewPostgres(cfg.postgresURL)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}

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
			log.Printf("[worker-%d] started", id)
			for msg := range jobs {
				log.Printf("[worker-%d] processing message offset=%d", id, msg.Offset)

				if err := worker.ProcessEvent(ctx, msg.Value, redisClient, batcher); err != nil {
					log.Printf("[worker-%d] processing failed: %v", id, err)

					if hErr := worker.HandleRetry(ctx, msg.Value, redisClient, cfg.retryLimit); hErr != nil {
						log.Printf("[worker-%d] CRITICAL: failed to schedule retry: %v", id, hErr)
					}
					// Do NOT commit the offset — Kafka will redeliver if the
					// consumer restarts before the retry is processed. This is
					// acceptable because ProcessEvent is idempotent via Redis lock.
					continue
				}

				// commit ONLY after success
				if err := consumer.CommitMessage(ctx, msg); err != nil {
					log.Printf("[worker-%d] commit failed: %v", id, err)
				}
			}
		}(i)
	}

	log.Println("Consumer started — waiting for messages...")

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
				log.Printf("error fetching message: %v", err)
				continue
			}
			jobs <- msg
		}
	}
shutdown:
	log.Println("draining job queue and waiting for workers to finish...")
	close(jobs)
	wg.Wait()

	// Stop the batcher only after all workers have finished their last Submit call. Calling Stop() while a Submit is in-flight would panic.
	log.Println("stopping batcher and flushing remaining events...")
	batcher.Stop()
	batcherWg.Wait()

	log.Println("closing resources...")
	consumer.Close()
	pg.Close()

	log.Println("shutdown complete")
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

	// RETRY_WORKER_COUNT defaults to 25% of main worker count if not set.
	if v := os.Getenv("RETRY_WORKER_COUNT"); v != "" {
		cfg.retryWorkerCount = mustEnvInt("RETRY_WORKER_COUNT", 1, 100)
	} else {
		cfg.retryWorkerCount = cfg.workerCount / 4
		if cfg.retryWorkerCount < 1 {
			cfg.retryWorkerCount = 1
		}
	}

	log.Printf("config: workers=%d retry_workers=%d retry_limit=%d batch_size=%d batch_flush_ms=%d topic=%s",
		cfg.workerCount, cfg.retryWorkerCount, cfg.retryLimit, cfg.batchSize, cfg.batchFlushMs, cfg.topic)

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
