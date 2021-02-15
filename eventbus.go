// Copyright (c) 2021 - The Event Horizon authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rabbitmq

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"

	eh "github.com/looplab/eventhorizon"
	_ "github.com/looplab/eventhorizon/types/mongodb"
)

// EventBus is a local event bus that delegates handling of published events
// to all matching registered handlers, in order of registration.
type EventBus struct {
	appID    string
	exchange string

	amqpConn    *amqp.Connection
	amqpChannel *amqp.Channel

	registered   map[eh.EventHandlerType]struct{}
	registeredMu sync.RWMutex
	errCh        chan eh.EventBusError
	wg           sync.WaitGroup
}

func connect(server string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(server)
	if err != nil {
		return nil, nil, fmt.Errorf("failed connecting to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("failed opening channel: %w", err)
	}

	return conn, ch, nil
}

// NewEventBus creates an EventBus, with optional GCP connection settings.
func NewEventBus(connectionString string, topicSuffix string) (*EventBus, error) {
	//ctx := context.Background()
	topic := topicSuffix + "_events"

	conn, ch, err := connect(connectionString)
	if err != nil {
		return nil, err
	}

	if err := ch.ExchangeDeclare(
		topic,    // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	); err != nil {
		return nil, err
	}

	return &EventBus{
		amqpConn:    conn,
		amqpChannel: ch,
		exchange:    topic,
		registered:  map[eh.EventHandlerType]struct{}{},
		errCh:       make(chan eh.EventBusError, 100),
	}, nil
}

// HandlerType implements the HandlerType method of the eventhorizon.EventHandler interface.
func (b *EventBus) HandlerType() eh.EventHandlerType {
	return "eventbus"
}

const (
	aggregateTypeHeader = "aggregate_type"
	eventTypeHeader     = "event_type"
)

// HandleEvent implements the HandleEvent method of the eventhorizon.EventHandler interface.
func (b *EventBus) HandleEvent(ctx context.Context, event eh.Event) error {
	e := evt{
		AggregateID:   event.AggregateID().String(),
		AggregateType: event.AggregateType(),
		EventType:     event.EventType(),
		Version:       event.Version(),
		Timestamp:     event.Timestamp(),
		Metadata:      event.Metadata(),
		Context:       eh.MarshalContext(ctx),
	}

	// Marshal event data if there is any.
	if event.Data() != nil {
		var err error
		if e.RawData, err = bson.Marshal(event.Data()); err != nil {
			return fmt.Errorf("could not marshal event data: %w", err)
		}
	}

	// Marshal the event (using BSON for now).
	data, err := bson.Marshal(e)
	if err != nil {
		return fmt.Errorf("could not marshal event: %w", err)
	}

	if err := b.amqpChannel.Publish(
		b.exchange,
		"",
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table{
				aggregateTypeHeader: event.AggregateType().String(),
				eventTypeHeader:     event.EventType().String(),
			},
			ContentType: "application/bson",
			Body:        data,
		}); err != nil {
		return fmt.Errorf("could not publish event: %w", err)
	}

	return nil
}

// AddHandler implements the AddHandler method of the eventhorizon.EventBus interface.
func (b *EventBus) AddHandler(ctx context.Context, m eh.EventMatcher, h eh.EventHandler) error {
	if m == nil {
		return eh.ErrMissingMatcher
	}

	if h == nil {
		return eh.ErrMissingHandler
	}

	// Check handler existence.
	b.registeredMu.Lock()
	defer b.registeredMu.Unlock()

	if _, ok := b.registered[h.HandlerType()]; ok {
		return eh.ErrHandlerAlreadyAdded
	}

	// Get or create the subscription.
	groupID := b.appID + "_" + h.HandlerType().String()

	q, err := b.amqpChannel.QueueDeclare(
		groupID, // name
		false,   // durable
		false,   // delete when unused
		true,    // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare a queue: %w", err)
	}

	err = b.amqpChannel.QueueBind(
		q.Name,     // queue name
		"",         // routing key
		b.exchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind a queue: %w", err)
	}

	msgs, err := b.amqpChannel.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return fmt.Errorf("failed to consume channel: %w", err)
	}

	// Register handler.
	b.registered[h.HandlerType()] = struct{}{}

	// Handle until context is cancelled.
	b.wg.Add(1)
	go b.handle(ctx, m, h, msgs)

	return nil
}

// Errors implements the Errors method of the eventhorizon.EventBus interface.
func (b *EventBus) Errors() <-chan eh.EventBusError {
	return b.errCh
}

// Wait for all channels to close in the event bus group
func (b *EventBus) Wait() {
	b.wg.Wait()

	if err := b.amqpChannel.Close(); err != nil {
		log.Printf("eventhorizon: failed to close RabbitMQ queue: %s", err)
	}

	if err := b.amqpConn.Close(); err != nil {
		log.Printf("eventhorizon: failed to close RabbitMQ connection: %s", err)
	}
}

// Handles all events coming in on the channel.
func (b *EventBus) handle(ctx context.Context, m eh.EventMatcher, h eh.EventHandler, msgs <-chan amqp.Delivery) {
	defer b.wg.Done()
	handler := b.handler(m, h)

	for msg := range msgs {
		handler(ctx, msg)
	}
}

//
func (b *EventBus) handler(m eh.EventMatcher, h eh.EventHandler) func(ctx context.Context, msg amqp.Delivery) {
	return func(ctx context.Context, msg amqp.Delivery) {
		// Decode the raw BSON event data.
		var e evt
		if err := bson.Unmarshal(msg.Body, &e); err != nil {
			err = fmt.Errorf("could not unmarshal event: %w", err)
			select {
			case b.errCh <- eh.EventBusError{Err: err, Ctx: ctx}:
			default:
				log.Printf("eventhorizon: missed error in RabbitMQ event bus: %s", err)
			}
			return
		}

		// Create an event of the correct type and decode from raw BSON.
		if len(e.RawData) > 0 {
			var err error
			if e.data, err = eh.CreateEventData(e.EventType); err != nil {
				err = fmt.Errorf("could not create event data: %w", err)
				select {
				case b.errCh <- eh.EventBusError{Err: err, Ctx: ctx}:
				default:
					log.Printf("eventhorizon: missed error in Kafka event bus: %s", err)
				}
				return
			}
			if err := bson.Unmarshal(e.RawData, e.data); err != nil {
				err = fmt.Errorf("could not unmarshal event data: %w", err)
				select {
				case b.errCh <- eh.EventBusError{Err: err, Ctx: ctx}:
				default:
					log.Printf("eventhorizon: missed error in Kafka event bus: %s", err)
				}
				return
			}
			e.RawData = nil
		}

		ctx = eh.UnmarshalContext(ctx, e.Context)
		aggregateID, err := uuid.Parse(e.AggregateID)
		if err != nil {
			aggregateID = uuid.Nil
		}
		event := eh.NewEvent(
			e.EventType,
			e.data,
			e.Timestamp,
			eh.ForAggregate(
				e.AggregateType,
				aggregateID,
				e.Version,
			),
			eh.WithMetadata(e.Metadata),
		)

		// Ignore non-matching events.
		if !m.Match(event) {
			//	r.CommitMessages(ctx, msg)
			return
		}

		// Handle the event if it did match.
		if err := h.HandleEvent(ctx, event); err != nil {
			err = fmt.Errorf("could not handle event (%s): %w", h.HandlerType(), err)
			select {
			case b.errCh <- eh.EventBusError{Err: err, Ctx: ctx, Event: event}:
			default:
				log.Printf("eventhorizon: missed error in RabbitMQ event bus: %s", err)
			}
			return
		}

		//r.CommitMessages(ctx, msg)
	}
}

// evt is the internal event used on the wire only.
type evt struct {
	EventType     eh.EventType           `bson:"event_type"`
	RawData       bson.Raw               `bson:"data,omitempty"`
	data          eh.EventData           `bson:"-"`
	Timestamp     time.Time              `bson:"timestamp"`
	AggregateType eh.AggregateType       `bson:"aggregate_type"`
	AggregateID   string                 `bson:"_id"`
	Version       int                    `bson:"version"`
	Metadata      map[string]interface{} `bson:"metadata"`
	Context       map[string]interface{} `bson:"context"`
}
