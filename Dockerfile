# Multi-stage build for app-administrator
FROM golang:1.24-alpine AS builder

# Install git for go mod download
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o administrator main.go

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN adduser -D -s /bin/sh administrator

# Set working directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/administrator .

# Change ownership
RUN chown administrator:administrator /app/administrator

# Switch to non-root user
USER administrator

# Expose port (if needed for health checks)
EXPOSE 8080

# Run the administrator
CMD ["./administrator"]
