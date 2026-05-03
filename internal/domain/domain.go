package domain

import (
	"crypto/rand"
	"encoding/base64"
	"strings"
	"time"
)

type QueueProvider string

const (
	ProviderBullMQ   QueueProvider = "bullmq"
	ProviderSQS      QueueProvider = "sqs"
	ProviderRabbitMQ QueueProvider = "rabbitmq"
)

type ConnectionMode string

const (
	ConnectionReadOnly ConnectionMode = "read_only"
	ConnectionOperator ConnectionMode = "operator"
)

type MessageStatus string

const (
	StatusWaiting    MessageStatus = "waiting"
	StatusActive     MessageStatus = "active"
	StatusDelayed    MessageStatus = "delayed"
	StatusCompleted  MessageStatus = "completed"
	StatusFailed     MessageStatus = "failed"
	StatusDeadLetter MessageStatus = "dead_letter"
	StatusInFlight   MessageStatus = "in_flight"
	StatusUnknown    MessageStatus = "unknown"
)

type UserRole string

const (
	RoleViewer   UserRole = "viewer"
	RoleOperator UserRole = "operator"
	RoleAdmin    UserRole = "admin"
)

type User struct {
	ID    string   `json:"id"`
	Email string   `json:"email"`
	Role  UserRole `json:"role"`
}

type ProviderInfo struct {
	ID           QueueProvider `json:"id"`
	Name         string        `json:"name"`
	Capabilities []string      `json:"capabilities"`
}

type QueueConnection struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Provider  QueueProvider  `json:"provider"`
	Mode      ConnectionMode `json:"mode"`
	Config    map[string]any `json:"config"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

type QueueStats struct {
	Waiting     *int `json:"waiting,omitempty"`
	Active      *int `json:"active,omitempty"`
	Delayed     *int `json:"delayed,omitempty"`
	Completed   *int `json:"completed,omitempty"`
	Failed      *int `json:"failed,omitempty"`
	DeadLetter  *int `json:"deadLetter,omitempty"`
	InFlight    *int `json:"inFlight,omitempty"`
	ConsumerLag *int `json:"consumerLag,omitempty"`
}

type QueueInfo struct {
	ID           string        `json:"id"`
	Name         string        `json:"name"`
	Provider     QueueProvider `json:"provider"`
	ConnectionID string        `json:"connectionId"`
	Stats        QueueStats    `json:"stats"`
}

type QueueMessage struct {
	ID          string         `json:"id"`
	QueueName   string         `json:"queueName"`
	Provider    QueueProvider  `json:"provider"`
	Status      MessageStatus  `json:"status"`
	Payload     any            `json:"payload"`
	Metadata    map[string]any `json:"metadata"`
	Attempts    *int           `json:"attempts,omitempty"`
	CreatedAt   *time.Time     `json:"createdAt,omitempty"`
	StartedAt   *time.Time     `json:"startedAt,omitempty"`
	CompletedAt *time.Time     `json:"completedAt,omitempty"`
	FailedAt    *time.Time     `json:"failedAt,omitempty"`
	Error       string         `json:"error,omitempty"`
}

type AuditAction string

const (
	AuditRetryMessage  AuditAction = "retry_message"
	AuditDeleteMessage AuditAction = "delete_message"
)

type AuditResult string

const (
	AuditResultSuccess AuditResult = "success"
	AuditResultFailure AuditResult = "failure"
)

type AuditLogEntry struct {
	ID           string        `json:"id"`
	ActorID      string        `json:"actorId"`
	ActorEmail   string        `json:"actorEmail"`
	Action       AuditAction   `json:"action"`
	Result       AuditResult   `json:"result"`
	Provider     QueueProvider `json:"provider"`
	ConnectionID string        `json:"connectionId"`
	QueueName    string        `json:"queueName"`
	MessageID    string        `json:"messageId"`
	Error        string        `json:"error,omitempty"`
	CreatedAt    time.Time     `json:"createdAt"`
}

func NewID(prefix string) string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return prefix + "_" + strings.ReplaceAll(time.Now().UTC().Format(time.RFC3339Nano), ":", "")
	}
	return prefix + "_" + base64.RawURLEncoding.EncodeToString(bytes[:])
}
