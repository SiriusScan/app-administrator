package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/streadway/amqp"
)

// AdminCommand represents an administrative command
type AdminCommand struct {
	Action    string    `json:"action"`     // "restart", "stop", "start", etc.
	Target    string    `json:"target"`     // Target container name
	RequestID string    `json:"request_id"` // Unique request identifier
	Timestamp time.Time `json:"timestamp"`
}

// AdminResponse represents the response to an admin command
type AdminResponse struct {
	RequestID     string    `json:"request_id"`
	ContainerName string    `json:"container_name"`
	Action        string    `json:"action"`
	Status        string    `json:"status"` // "success", "error"
	Message       string    `json:"message"`
	Timestamp     time.Time `json:"timestamp"`
}

// Administrator handles administrative commands via RabbitMQ
type Administrator struct {
	containerName string
	startTime     time.Time
}

// NewAdministrator creates a new administrator instance
func NewAdministrator(containerName string) (*Administrator, error) {
	return &Administrator{
		containerName: containerName,
		startTime:     time.Now(),
	}, nil
}

// Start begins listening for admin commands
func (a *Administrator) Start(ctx context.Context) error {
	log.Printf("Starting administrator for container: %s", a.containerName)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start consuming admin commands with retry logic
	adminQueue := "admin_commands"

	log.Printf("Administrator listening for commands on queue: %s", adminQueue)

	// Start RabbitMQ connection in a goroutine with retry logic
	go a.connectToRabbitMQ(adminQueue)

	// Wait for shutdown signal
	select {
	case <-ctx.Done():
		log.Printf("Administrator context cancelled, shutting down...")
		return nil
	case <-sigChan:
		log.Printf("Received shutdown signal, stopping administrator...")
		return nil
	}
}

// connectToRabbitMQ connects to RabbitMQ with retry logic
func (a *Administrator) connectToRabbitMQ(queueName string) {
	rabbitMQURL := "amqp://guest:guest@sirius-rabbitmq:5672/"

	for {
		log.Printf("Attempting to connect to RabbitMQ at %s", rabbitMQURL)

		conn, err := amqp.Dial(rabbitMQURL)
		if err != nil {
			log.Printf("Failed to connect to RabbitMQ: %v, retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Successfully connected to RabbitMQ")

		// Start consuming messages
		if err := a.consumeMessages(conn, queueName); err != nil {
			log.Printf("Error consuming messages: %v, reconnecting...", err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		// If we get here, the connection was lost
		log.Printf("RabbitMQ connection lost, reconnecting...")
		conn.Close()
		time.Sleep(5 * time.Second)
	}
}

// consumeMessages consumes messages from the RabbitMQ queue
func (a *Administrator) consumeMessages(conn *amqp.Connection, queueName string) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	// Declare the queue
	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Start consuming
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	log.Printf("Successfully started consuming messages from queue: %s", queueName)

	// Process messages
	for msg := range msgs {
		log.Printf("DEBUG: Received message from RabbitMQ, body length: %d", len(msg.Body))
		a.processMessageString(string(msg.Body))
	}

	return nil
}

// processMessageString processes an incoming admin command message
func (a *Administrator) processMessageString(msg string) {
	log.Printf("DEBUG: Received message: %s", msg)

	// Parse the admin command
	var command AdminCommand
	if err := json.Unmarshal([]byte(msg), &command); err != nil {
		log.Printf("Error parsing admin command: %v, raw message: %s", err, msg)
		return
	}

	log.Printf("DEBUG: Parsed command - Action: %s, Target: %s, RequestID: %s", command.Action, command.Target, command.RequestID)
	log.Printf("DEBUG: This container name: %s", a.containerName)

	// Check if this command is for this container
	if command.Target != a.containerName {
		log.Printf("Command for %s, ignoring (this is %s)", command.Target, a.containerName)
		return
	}

	log.Printf("Processing admin command: %s for container: %s", command.Action, command.Target)

	// Execute the command
	adminResponse := a.executeCommand(command)

	// Log the response
	log.Printf("Command result: %s - %s", adminResponse.Status, adminResponse.Message)
}

// executeCommand executes the specified admin command
func (a *Administrator) executeCommand(command AdminCommand) AdminResponse {
	response := AdminResponse{
		RequestID:     command.RequestID,
		ContainerName: a.containerName,
		Action:        command.Action,
		Timestamp:     time.Now(),
	}

	switch command.Action {
	case "restart":
		// Signal the main process to exit, which will cause Docker to restart the container
		response.Status = "success"
		response.Message = "Container restart initiated - signaling main process to exit"
		log.Printf("Container %s restart initiated - signaling main process to exit", a.containerName)

		// Send SIGTERM to the main process (PID 1) to trigger container restart
		go func() {
			time.Sleep(1 * time.Second) // Give time for response to be logged
			log.Printf("Sending SIGTERM to main process to trigger container restart")
			if err := syscall.Kill(1, syscall.SIGTERM); err != nil {
				log.Printf("Failed to send SIGTERM to main process: %v", err)
				// Fallback: exit the administrator process
				log.Printf("Falling back to exiting administrator process")
				os.Exit(0)
			}
		}()

	case "stop":
		response.Status = "success"
		response.Message = "Container stop initiated"
		log.Printf("Container %s stop initiated", a.containerName)

	case "start":
		response.Status = "success"
		response.Message = "Container start initiated"
		log.Printf("Container %s start initiated", a.containerName)

	default:
		response.Status = "error"
		response.Message = fmt.Sprintf("Unknown action: %s", command.Action)
		log.Printf("Unknown admin action: %s", command.Action)
	}

	return response
}

func main() {
	// Get container name from environment
	containerName := os.Getenv("CONTAINER_NAME")
	if containerName == "" {
		log.Fatal("CONTAINER_NAME environment variable is required")
	}

	// Create administrator instance
	admin, err := NewAdministrator(containerName)
	if err != nil {
		log.Fatalf("Failed to create administrator: %v", err)
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the administrator
	if err := admin.Start(ctx); err != nil {
		log.Fatalf("Administrator failed: %v", err)
	}

	log.Printf("Administrator for %s stopped", containerName)
}
