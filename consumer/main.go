package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/Rushi2398/event-processing-system/consumer/service"
	"github.com/Rushi2398/event-processing-system/consumer/worker"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func main() {
	err := godotenv.Load("../.env")
	if err != nil {
		log.Println("No .env file found")
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	topic := os.Getenv("KAFKA_TOPIC")
	groupID := os.Getenv("KAFKA_GROUP_ID")

	consumer := service.NewConsumer(brokers, topic, groupID)
	redisClient := service.NewRedisClient(os.Getenv("REDIS_ADDR"))
	pg, err := service.NewPostgres(os.Getenv("POSTGRES_URL"))
	if err != nil {
		log.Fatal("failed to connect postgres:", err)

	}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutdown signal received")
		cancel()
	}()

	//Worker Pool
	workerCountStr := os.Getenv("WORKER_COUNT")
	workerCount, _ := strconv.Atoi(workerCountStr)

	retryLimitStr := os.Getenv("RETRY_LIMIT")
	retryLimit, _ := strconv.Atoi(retryLimitStr)

	go worker.StartRetryWorker(ctx, redisClient, pg, &wg, retryLimit)
	jobs := make(chan kafka.Message, 100)

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for msg := range jobs {
				log.Printf("Worker %d processing message\n", id)

				err := worker.ProcessEvent(msg.Value, redisClient, pg)

				if err != nil {
					log.Println("processing failed:", err)
					worker.HandleRetry(msg.Value, redisClient, retryLimit)

					continue
				}

				// commit ONLY after success
				if err := consumer.CommitMessage(ctx, msg); err != nil {
					log.Println("commit failed:", err)
				}
			}
		}(i)
	}
	log.Println("Consumer started...")

	//Consume Messages
	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping Consumer...")
			close(jobs)
			goto shutdown

		default:
			msg, err := consumer.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					goto shutdown
				}
				log.Println("error fetching message:", err)
				continue
			}
			jobs <- msg
		}
	}
shutdown:
	log.Println("Waiting for workers to finish...")
	wg.Wait()

	log.Println("Closing resources...")
	consumer.Close()
	pg.Close()

	log.Println("Shutdown complete")
}
