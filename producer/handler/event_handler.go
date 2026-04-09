package handler

import (
	"net/http"
	"time"

	"github.com/Rushi2398/event-processing-system/producer/model"
	"github.com/Rushi2398/event-processing-system/producer/service"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type EventHandler struct {
	producer *service.Producer
}

func NewEventHandler(p *service.Producer) *EventHandler {
	return &EventHandler{producer: p}
}

func (h *EventHandler) PublishEvent(c *gin.Context) {
	var req model.Event
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	req.ID = uuid.New().String()
	req.Timestamp = time.Now().Unix()

	if err := h.producer.Publish(req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to publish"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "event published",
		"id":      req.ID,
	})
}
