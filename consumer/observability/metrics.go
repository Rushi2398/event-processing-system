package observability

import "github.com/prometheus/client_golang/prometheus"

// All metrics are package-level vars so any package can import and record against them without passing a metrics object around.
//   <namespace>_<subsystem>_<name>_<unit>
// Namespace is "eps" (event processing system).

var (
	// EventsProcessedTotal counts every event that exits ProcessEvent, labelled by outcome. Use this to derive throughput and error rate.
	EventsProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "eps_events_processed_total",
			Help: "Total number of events processed, labelled by status (success|failed|duplicate).",
		},
		[]string{"status"},
	)

	// EventProcessingDuration measures end-to-end latency per event from the moment ProcessEvent is called until it returns.
	EventProcessingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "eps_event_processing_duration_seconds",
			Help:    "End-to-end processing latency per event.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		},
		[]string{"status"},
	)

	// BatchSize records how many events were in each flush.
	BatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "eps_batch_size_events",
			Help:    "Number of events per batch flush to PostgreSQL.",
			Buckets: []float64{1, 5, 10, 25, 50, 75, 100, 150, 200, 500},
		},
	)

	// BatchFlushDuration measures how long each PostgreSQL batch INSERT takes.
	BatchFlushDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "eps_batch_flush_duration_seconds",
			Help:    "Time spent executing a batch INSERT into PostgreSQL.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		},
	)

	// RetryQueueDepth is a gauge updated by the queue depth poller in main.
	// A sustained non-zero value means processing can't keep up with failures.
	RetryQueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "eps_retry_queue_depth",
			Help: "Current number of events waiting in the retry queue.",
		},
	)

	// DLQDepth is a gauge updated by the queue depth poller in main.
	// Any non-zero value warrants investigation.
	DLQDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "eps_dlq_depth",
			Help: "Current number of events in the dead-letter queue.",
		},
	)

	// RetryAttemptsTotal counts retry scheduling calls by attempt number.
	// Lets you see whether failures are concentrated at attempt 1 or spreading.
	RetryAttemptsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "eps_retry_attempts_total",
			Help: "Total retry attempts, labelled by attempt number.",
		},
		[]string{"attempt"},
	)

	// DLQEventsTotal counts events that have exhausted retries and been moved to the dead-letter queue. Alert on this being non-zero.
	DLQEventsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "eps_dlq_events_total",
			Help: "Total events moved to the dead-letter queue after exhausting retries.",
		},
	)
)

// Register registers all metrics with the default Prometheus registry.
// Call once at startup before any metrics are recorded.
func Register() {
	prometheus.MustRegister(
		EventsProcessedTotal,
		EventProcessingDuration,
		BatchSize,
		BatchFlushDuration,
		RetryQueueDepth,
		DLQDepth,
		RetryAttemptsTotal,
		DLQEventsTotal,
	)
}
