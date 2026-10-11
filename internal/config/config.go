package config

import (
	"os"
	"strings"
)

type Config struct {
	InstanceID     string
	HTTPAddr       string
	GRPCAddr       string
	MetricsAddr    string
	DatabaseURL    string
	DatabaseRead   string
	RedisAddr      string
	KafkaBrokers   []string
	MinIOEndpoint  string
	MinIOAccessKey string
	MinIOSecretKey string
	MinIOBucket    string
	MinIOPublicURL string
	JWTSecret      string
	OTELEndpoint   string
	WorkerShards   []string
}

func Load() Config {
	return Config{
		InstanceID:     getEnv("INSTANCE_ID", "api-1"),
		HTTPAddr:       getEnv("HTTP_ADDR", ":8080"),
		GRPCAddr:       getEnv("GRPC_ADDR", ":50051"),
		MetricsAddr:    getEnv("METRICS_ADDR", ":9090"),
		DatabaseURL:    getEnv("DATABASE_URL", "postgres://ke_user:ke_password@localhost:5432/ke_events?sslmode=disable"),
		DatabaseRead:   getEnv("DATABASE_READ_URL", "postgres://ke_user:ke_password@localhost:5432/ke_events?sslmode=disable"),
		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:   strings.Split(getEnv("KAFKA_BROKERS", "localhost:19092"), ","),
		MinIOEndpoint:  getEnv("MINIO_ENDPOINT", "localhost:9000"),
		MinIOAccessKey: getEnv("MINIO_ACCESS_KEY", "minioadmin"),
		MinIOSecretKey: getEnv("MINIO_SECRET_KEY", "minioadmin_secret"),
		MinIOBucket:    getEnv("MINIO_BUCKET", "ke-event-media"),
		MinIOPublicURL: getEnv("MINIO_PUBLIC_URL", "http://localhost:9000"),
		JWTSecret:      getEnv("JWT_SECRET", "dev-secret-change-me"),
		OTELEndpoint:   getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		WorkerShards:   strings.Split(getEnv("WORKER_SHARDS", "worker-1"), ","),
	}
}

func getEnv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
