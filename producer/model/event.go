package model

type Event struct {
	ID        string `json:"id"`
	Key       string `json:"key"`
	Type      string `json:"type"     binding:"required"`
	Timestamp int64  `json:"timestamp"`
	Payload   any    `json:"payload"  binding:"required"`
	Retry     int    `json:"retry"`
}
