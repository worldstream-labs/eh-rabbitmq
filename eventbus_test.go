package rabbitmq

import (
	"os"
	"testing"
	"time"

	"github.com/looplab/eventhorizon/eventbus"
)

func TestEventBus(t *testing.T) {
	cs := os.Getenv("RABBITMQ_CONNECTION_STRING")
	if cs == "" {
		cs = "amqp://guest:guest@localhost:5672/"
	}

	timeout := time.Second * 30
	bus1, err := NewEventBus(cs, "suf_")
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	bus2, err := NewEventBus(cs, "suf_")
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	eventbus.AcceptanceTest(t, bus1, bus2, timeout)
}
