package sqs

import (
	"context"
	"errors"

	"github.com/gama/queuescope/internal/domain"
	"github.com/gama/queuescope/internal/providers"
)

type Adapter struct{}

func New() *Adapter {
	return &Adapter{}
}

func (a *Adapter) Info() domain.ProviderInfo {
	return domain.ProviderInfo{
		ID:   domain.ProviderSQS,
		Name: "Amazon SQS",
		Capabilities: []string{
			providers.CapabilityListQueues,
			providers.CapabilityGetQueueStats,
			providers.CapabilityListMessages,
			providers.CapabilityInspectMessage,
			providers.CapabilityDeleteMessage,
			providers.CapabilityListDeadLetters,
			providers.CapabilityRedriveDeadLetter,
		},
	}
}

func (a *Adapter) Test(ctx context.Context, connection domain.QueueConnection) error {
	return nil
}

func (a *Adapter) ListQueues(ctx context.Context, connection domain.QueueConnection) ([]domain.QueueInfo, error) {
	return []domain.QueueInfo{}, nil
}

func (a *Adapter) ListMessages(ctx context.Context, connection domain.QueueConnection, queueName string, filter providers.MessageFilter) ([]domain.QueueMessage, error) {
	return []domain.QueueMessage{}, nil
}

func (a *Adapter) GetMessage(ctx context.Context, connection domain.QueueConnection, queueName string, messageID string) (domain.QueueMessage, error) {
	return domain.QueueMessage{}, errors.New("sqs message inspection is not implemented yet")
}

func (a *Adapter) RetryMessage(ctx context.Context, connection domain.QueueConnection, queueName string, messageID string) error {
	return errors.New("sqs retry is not implemented yet")
}

func (a *Adapter) DeleteMessage(ctx context.Context, connection domain.QueueConnection, queueName string, messageID string) error {
	return errors.New("sqs delete is not implemented yet")
}
