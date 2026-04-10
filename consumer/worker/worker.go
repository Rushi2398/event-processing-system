package worker

import (
	"encoding/json"
	"log"

	"github.com/Rushi2398/event-processing-system/producer/model"
)

func ProcessEvent(msg []byte) {
	var event model.Event

	if err := json.Unmarshal(msg, &event); err != nil {
		log.Println("failed to parse event:", err)
		return
	}

	log.Printf("Processing event: ID=%s Type=%s Key=%s\n", event.ID, event.Type, event.Key)

	// TODO: Add real business logic here
}
