package config

import "os"

type Config struct {
	Addr            string
	DatabaseURL     string
	SessionSecret   string
	AdminEmail      string
	AdminPassword   string
	AllowedOrigin   string
	ReadOnlyDefault bool
}

func Load() Config {
	return Config{
		Addr:            getEnv("QUEUESCOPE_ADDR", ":8080"),
		DatabaseURL:     getEnv("QUEUESCOPE_DATABASE_URL", "postgres://queuescope:queuescope@localhost:5432/queuescope?sslmode=disable"),
		SessionSecret:   getEnv("QUEUESCOPE_SESSION_SECRET", "dev-session-secret-change-me"),
		AdminEmail:      getEnv("QUEUESCOPE_ADMIN_EMAIL", "admin@queuescope.local"),
		AdminPassword:   getEnv("QUEUESCOPE_ADMIN_PASSWORD", "queuescope"),
		AllowedOrigin:   getEnv("QUEUESCOPE_ALLOWED_ORIGIN", "http://localhost:5173"),
		ReadOnlyDefault: getEnv("QUEUESCOPE_READ_ONLY_DEFAULT", "true") != "false",
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
