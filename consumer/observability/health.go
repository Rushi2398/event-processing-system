package observability

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartObservabilityServer starts a Gin HTTP server that serves:
//   - GET /metrics         — Prometheus scrape endpoint
//   - GET /healthz/live    — liveness probe (always 200 if process is running)
//   - GET /healthz/ready   — readiness probe (checks Redis + Postgres reachability)
//
// The server shuts down gracefully when ctx is cancelled.

func StartObservabilityServer(ctx context.Context, addr string, redisCheck func(ctx context.Context) error,
	pgCheck func(ctx context.Context) error, wg *sync.WaitGroup) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	// Prometheus metrics scrape endpoint — wrap the stdlib handler for Gin.
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Liveness: is the process up?
	// Kubernetes restarts the pod if this fails.
	r.GET("/healthz/live", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Readiness: are dependencies reachable?
	// Kubernetes stops routing traffic (without restarting) if this fails.
	r.GET("/healthz/ready", func(c *gin.Context) {
		checkCtx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()

		type checkResult struct {
			Status string `json:"status"`
			Error  string `json:"error,omitempty"`
		}

		results := gin.H{}
		ready := true

		if err := redisCheck(checkCtx); err != nil {
			results["redis"] = checkResult{Status: "unhealthy", Error: err.Error()}
			ready = false
		} else {
			results["redis"] = checkResult{Status: "ok"}
		}

		if err := pgCheck(checkCtx); err != nil {
			results["postgres"] = checkResult{Status: "unhealthy", Error: err.Error()}
			ready = false
		} else {
			results["postgres"] = checkResult{Status: "ok"}
		}

		if !ready {
			slog.Warn("readiness check failed", "results", results)
			c.JSON(http.StatusServiceUnavailable, results)
			return
		}

		c.JSON(http.StatusOK, results)
	})

	server := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := server.Shutdown(shutdownCtx); err != nil {
				slog.Error("observability server shutdown error", "error", err)
			}
		}()
		slog.Info("observability server started", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("observability server failed", "error", err)
		}
	}()
}
