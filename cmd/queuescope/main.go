package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gama/queuescope/internal/api"
	"github.com/gama/queuescope/internal/config"
	"github.com/gama/queuescope/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfg := config.Load()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := connectPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	connectionStore := store.NewConnectionStore(db)
	if err := connectionStore.Migrate(ctx); err != nil {
		log.Fatal(err)
	}

	server := api.NewServer(cfg, connectionStore)

	log.Printf("QueueScope backend listening on %s", cfg.Addr)
	if err := server.Routes().Run(cfg.Addr); err != nil {
		log.Fatal(err)
	}
}

func connectPostgres(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	db, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for attempt := 1; attempt <= 20; attempt++ {
		if err := db.Ping(ctx); err == nil {
			return db, nil
		} else {
			lastErr = err
		}

		log.Printf("waiting for Postgres to be ready, attempt %d/20", attempt)
		time.Sleep(750 * time.Millisecond)
	}

	db.Close()
	return nil, fmt.Errorf("could not connect to Postgres: %w", lastErr)
}
