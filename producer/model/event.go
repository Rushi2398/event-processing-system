package model

type Event struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Timestamp int64  `json:"timestamp"`
	Payload   any    `json:"payload"`
}
