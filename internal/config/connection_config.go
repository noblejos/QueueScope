package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/gama/queuescope/internal/domain"
)

func ValidateConnectionConfig(provider domain.QueueProvider, raw map[string]any) (map[string]any, error) {
	switch provider {
	case domain.ProviderBullMQ:
		return validateBullMQConfig(raw)
	case domain.ProviderSQS:
		return validateSQSConfig(raw)
	case domain.ProviderRabbitMQ:
		return validateRabbitMQConfig(raw)
	default:
		return nil, fmt.Errorf("unsupported provider %q", provider)
	}
}

func validateBullMQConfig(raw map[string]any) (map[string]any, error) {
	redisURL, err := requiredString(raw, "redisUrl")
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(redisURL)
	if err != nil || parsed.Scheme != "redis" && parsed.Scheme != "rediss" || parsed.Host == "" {
		return nil, errors.New("bullmq config redisUrl must be a valid redis:// or rediss:// URL")
	}

	prefix := optionalString(raw, "prefix", "bull")
	if strings.TrimSpace(prefix) == "" {
		return nil, errors.New("bullmq config prefix cannot be empty")
	}

	return map[string]any{
		"redisUrl": redisURL,
		"prefix":   prefix,
	}, nil
}

func validateSQSConfig(raw map[string]any) (map[string]any, error) {
	region, err := requiredString(raw, "region")
	if err != nil {
		return nil, err
	}
	queueURL, err := requiredString(raw, "queueUrl")
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(queueURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, errors.New("sqs config queueUrl must be a valid https URL")
	}

	config := map[string]any{
		"region":   region,
		"queueUrl": queueURL,
	}
	if profile := optionalString(raw, "profile", ""); profile != "" {
		config["profile"] = profile
	}
	if endpointURL := optionalString(raw, "endpointUrl", ""); endpointURL != "" {
		config["endpointUrl"] = endpointURL
	}
	return config, nil
}

func validateRabbitMQConfig(raw map[string]any) (map[string]any, error) {
	amqpURL, err := requiredString(raw, "amqpUrl")
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(amqpURL)
	if err != nil || parsed.Scheme != "amqp" && parsed.Scheme != "amqps" || parsed.Host == "" {
		return nil, errors.New("rabbitmq config amqpUrl must be a valid amqp:// or amqps:// URL")
	}

	vhost := optionalString(raw, "vhost", "/")
	if strings.TrimSpace(vhost) == "" {
		return nil, errors.New("rabbitmq config vhost cannot be empty")
	}

	return map[string]any{
		"amqpUrl": amqpURL,
		"vhost":   vhost,
	}, nil
}

func requiredString(raw map[string]any, key string) (string, error) {
	value, ok := raw[key].(string)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("config %s is required", key)
	}
	return strings.TrimSpace(value), nil
}

func optionalString(raw map[string]any, key string, fallback string) string {
	value, ok := raw[key].(string)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}
