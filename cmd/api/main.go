package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	eventsv1 "github.com/cladder/ke-events/gen/events/v1"
	"github.com/cladder/ke-events/internal/api"
	"github.com/cladder/ke-events/internal/broker"
	"github.com/cladder/ke-events/internal/cache"
	"github.com/cladder/ke-events/internal/config"
	"github.com/cladder/ke-events/internal/db"
	"github.com/cladder/ke-events/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	cfg := config.Load()
	log := telemetry.NewLogger("api-" + cfg.InstanceID)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	shutdownTrace := telemetry.InitTracer(ctx, "ke-events-api", cfg.OTELEndpoint)
	defer func() { _ = shutdownTrace(context.Background()) }()

	store, err := db.New(ctx, cfg.DatabaseURL, cfg.DatabaseRead)
	if err != nil {
		log.Error("failed to connect postgres", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	rdb := cache.New(cfg.RedisAddr)
	defer rdb.Close()

	brk, err := broker.New(cfg.KafkaBrokers)
	if err != nil {
		log.Warn("kafka unavailable at startup", "err", err)
	} else {
		defer brk.Close()
	}

	httpSrv := &api.HTTPServer{
		Store:      store,
		Cache:      rdb,
		Broker:     brk,
		JWTSecret:  cfg.JWTSecret,
		InstanceID: cfg.InstanceID,
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpSrv.Router(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Start gRPC server
	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Error("grpc listen failed", "err", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer()
	eventsv1.RegisterEventAggregationServiceServer(grpcSrv, &api.GRPCServer{
		Store:  store,
		Cache:  rdb,
		Broker: brk,
	})
	reflection.Register(grpcSrv)

	go func() {
		log.Info("gRPC server listening", "addr", cfg.GRPCAddr)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			log.Error("grpc serve error", "err", err)
		}
	}()

	go func() {
		log.Info("HTTP API server listening", "addr", cfg.HTTPAddr, "instance", cfg.InstanceID)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http serve error", "err", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down API servers")
	shCtx, shCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shCancel()
	_ = srv.Shutdown(shCtx)
	grpcSrv.GracefulStop()
}
