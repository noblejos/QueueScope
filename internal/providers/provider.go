package providers

import (
	"context"

	"github.com/gama/queuescope/internal/domain"
)

const (
	CapabilityListQueues        = "listQueues"
	CapabilityGetQueueStats     = "getQueueStats"
	CapabilityListMessages      = "listMessages"
	CapabilityInspectMessage    = "inspectMessage"
	CapabilityRetryMessage      = "retryMessage"
	CapabilityRequeueMessage    = "requeueMessage"
	CapabilityDeleteMessage     = "deleteMessage"
	CapabilityPurgeQueue        = "purgeQueue"
	CapabilityListDeadLetters   = "listDeadLetters"
	CapabilityRedriveDeadLetter = "redriveDeadLetter"
)

type Adapter interface {
	Info() domain.ProviderInfo
	Test(ctx context.Context, connection domain.QueueConnection) error
	ListQueues(ctx context.Context, connection domain.QueueConnection) ([]domain.QueueInfo, error)
	ListMessages(ctx context.Context, connection domain.QueueConnection, queueName string, filter MessageFilter) ([]domain.QueueMessage, error)
	GetMessage(ctx context.Context, connection domain.QueueConnection, queueName string, messageID string) (domain.QueueMessage, error)
	RetryMessage(ctx context.Context, connection domain.QueueConnection, queueName string, messageID string) error
	DeleteMessage(ctx context.Context, connection domain.QueueConnection, queueName string, messageID string) error
}

type MessageFilter struct {
	Status domain.MessageStatus
	Limit  int
	Query  string
}

type Registry struct {
	adapters map[domain.QueueProvider]Adapter
}

func NewRegistry(adapters ...Adapter) *Registry {
	registry := &Registry{adapters: map[domain.QueueProvider]Adapter{}}
	for _, adapter := range adapters {
		registry.adapters[adapter.Info().ID] = adapter
	}
	return registry
}

func (r *Registry) Providers() []domain.ProviderInfo {
	providers := make([]domain.ProviderInfo, 0, len(r.adapters))
	for _, adapter := range r.adapters {
		providers = append(providers, adapter.Info())
	}
	return providers
}

func (r *Registry) Get(provider domain.QueueProvider) (Adapter, bool) {
	adapter, ok := r.adapters[provider]
	return adapter, ok
}
