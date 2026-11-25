package main

import (
	"strings"
	"testing"
	"time"
)

func TestResolveRabbitMQURL(t *testing.T) {
	t.Run("uses RABBITMQ_URL when set", func(t *testing.T) {
		t.Setenv("RABBITMQ_URL", "  amqp://sirius_pro:s3cr3t@rabbitmq:5672/  ")
		got := resolveRabbitMQURL()
		want := "amqp://sirius_pro:s3cr3t@rabbitmq:5672/"
		if got != want {
			t.Fatalf("resolveRabbitMQURL() = %q, want %q", got, want)
		}
	})

	t.Run("falls back to default when unset or blank", func(t *testing.T) {
		t.Setenv("RABBITMQ_URL", "   ")
		if got := resolveRabbitMQURL(); got != defaultRabbitMQURL {
			t.Fatalf("resolveRabbitMQURL() = %q, want %q", got, defaultRabbitMQURL)
		}
	})
}

func TestRedactAMQPURL(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		want     string
		noSecret string
	}{
		{
			name:     "redacts password",
			input:    "amqp://sirius_pro:super-secret-pass@rabbitmq:5672/",
			want:     "amqp://sirius_pro:xxxxx@rabbitmq:5672/",
			noSecret: "super-secret-pass",
		},
		{
			name:     "preserves host without credentials",
			input:    "amqp://rabbitmq:5672/prod",
			want:     "amqp://rabbitmq:5672/prod",
			noSecret: "",
		},
		{
			name:     "malformed url never leaks secret",
			input:    "://sirius_pro:leaked-secret@bad",
			want:     "<redacted-amqp-url>",
			noSecret: "leaked-secret",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactAMQPURL(tc.input)
			if got != tc.want {
				t.Fatalf("redactAMQPURL(%q) = %q, want %q", tc.input, got, tc.want)
			}
			if tc.noSecret != "" && strings.Contains(got, tc.noSecret) {
				t.Fatalf("redacted URL %q leaked secret %q", got, tc.noSecret)
			}
		})
	}
}

func TestNextRetryDelay(t *testing.T) {
	delays := []time.Duration{
		5 * time.Second,
		10 * time.Second,
		20 * time.Second,
		40 * time.Second,
		60 * time.Second,
		60 * time.Second,
	}
	cur := initialRetryDelay
	for i, want := range delays {
		if cur != want {
			t.Fatalf("step %d: got %s, want %s", i, cur, want)
		}
		cur = nextRetryDelay(cur)
	}
}
