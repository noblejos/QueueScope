package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/gama/queuescope/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrConnectionNotFound = errors.New("connection not found")

type ConnectionStore struct {
	db *pgxpool.Pool
}

func NewConnectionStore(db *pgxpool.Pool) *ConnectionStore {
	return &ConnectionStore{db: db}
}

func (s *ConnectionStore) Migrate(ctx context.Context) error {
	_, err := s.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS queue_connections (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			provider TEXT NOT NULL,
			mode TEXT NOT NULL,
			config JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL
		);

		CREATE INDEX IF NOT EXISTS queue_connections_provider_idx
			ON queue_connections (provider);

		CREATE TABLE IF NOT EXISTS audit_log_entries (
			id TEXT PRIMARY KEY,
			actor_id TEXT NOT NULL,
			actor_email TEXT NOT NULL,
			action TEXT NOT NULL,
			result TEXT NOT NULL,
			provider TEXT NOT NULL,
			connection_id TEXT NOT NULL,
			queue_name TEXT NOT NULL,
			message_id TEXT NOT NULL,
			error TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL
		);

		CREATE INDEX IF NOT EXISTS audit_log_entries_created_at_idx
			ON audit_log_entries (created_at DESC);
	`)
	return err
}

func (s *ConnectionStore) List(ctx context.Context) ([]domain.QueueConnection, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, name, provider, mode, config, created_at, updated_at
		FROM queue_connections
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	connections := []domain.QueueConnection{}
	for rows.Next() {
		connection, err := scanConnection(rows)
		if err != nil {
			return nil, err
		}
		connections = append(connections, connection)
	}

	return connections, rows.Err()
}

func (s *ConnectionStore) Create(ctx context.Context, connection domain.QueueConnection) (domain.QueueConnection, error) {
	now := time.Now().UTC()
	connection.ID = domain.NewID("conn")
	connection.CreatedAt = now
	connection.UpdatedAt = now

	configBytes, err := json.Marshal(connection.Config)
	if err != nil {
		return domain.QueueConnection{}, err
	}

	_, err = s.db.Exec(ctx, `
		INSERT INTO queue_connections (id, name, provider, mode, config, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, connection.ID, connection.Name, connection.Provider, connection.Mode, configBytes, connection.CreatedAt, connection.UpdatedAt)
	if err != nil {
		return domain.QueueConnection{}, err
	}

	return connection, nil
}

func (s *ConnectionStore) Get(ctx context.Context, id string) (domain.QueueConnection, error) {
	row := s.db.QueryRow(ctx, `
		SELECT id, name, provider, mode, config, created_at, updated_at
		FROM queue_connections
		WHERE id = $1
	`, id)

	connection, err := scanConnection(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.QueueConnection{}, ErrConnectionNotFound
	}
	if err != nil {
		return domain.QueueConnection{}, err
	}

	return connection, nil
}

func (s *ConnectionStore) Delete(ctx context.Context, id string) error {
	result, err := s.db.Exec(ctx, `
		DELETE FROM queue_connections
		WHERE id = $1
	`, id)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrConnectionNotFound
	}

	return nil
}

func (s *ConnectionStore) CreateAuditEntry(ctx context.Context, entry domain.AuditLogEntry) (domain.AuditLogEntry, error) {
	entry.ID = domain.NewID("audit")
	entry.CreatedAt = time.Now().UTC()

	_, err := s.db.Exec(ctx, `
		INSERT INTO audit_log_entries (
			id,
			actor_id,
			actor_email,
			action,
			result,
			provider,
			connection_id,
			queue_name,
			message_id,
			error,
			created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`,
		entry.ID,
		entry.ActorID,
		entry.ActorEmail,
		entry.Action,
		entry.Result,
		entry.Provider,
		entry.ConnectionID,
		entry.QueueName,
		entry.MessageID,
		entry.Error,
		entry.CreatedAt,
	)
	if err != nil {
		return domain.AuditLogEntry{}, err
	}

	return entry, nil
}

func (s *ConnectionStore) ListAuditEntries(ctx context.Context, limit int) ([]domain.AuditLogEntry, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}

	rows, err := s.db.Query(ctx, `
		SELECT
			id,
			actor_id,
			actor_email,
			action,
			result,
			provider,
			connection_id,
			queue_name,
			message_id,
			error,
			created_at
		FROM audit_log_entries
		ORDER BY created_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := []domain.AuditLogEntry{}
	for rows.Next() {
		var entry domain.AuditLogEntry
		err := rows.Scan(
			&entry.ID,
			&entry.ActorID,
			&entry.ActorEmail,
			&entry.Action,
			&entry.Result,
			&entry.Provider,
			&entry.ConnectionID,
			&entry.QueueName,
			&entry.MessageID,
			&entry.Error,
			&entry.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, rows.Err()
}

type connectionScanner interface {
	Scan(dest ...any) error
}

func scanConnection(scanner connectionScanner) (domain.QueueConnection, error) {
	var connection domain.QueueConnection
	var configBytes []byte

	err := scanner.Scan(
		&connection.ID,
		&connection.Name,
		&connection.Provider,
		&connection.Mode,
		&configBytes,
		&connection.CreatedAt,
		&connection.UpdatedAt,
	)
	if err != nil {
		return domain.QueueConnection{}, err
	}

	if err := json.Unmarshal(configBytes, &connection.Config); err != nil {
		return domain.QueueConnection{}, err
	}
	if connection.Config == nil {
		connection.Config = map[string]any{}
	}

	return connection, nil
}
