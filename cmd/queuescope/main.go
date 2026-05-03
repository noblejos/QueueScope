package main

import (
	"context"
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

	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		log.Fatal(err)
	}

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
