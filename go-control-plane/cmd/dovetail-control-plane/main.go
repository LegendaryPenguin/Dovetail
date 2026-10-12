// Command dovetail-control-plane runs the scheduler and its HTTP API.
package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	dovetail "github.com/LegendaryPenguin/dovetail"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	cycle := flag.Duration("cycle", 250*time.Millisecond, "scheduling interval")
	reconcile := flag.Duration("reconcile", 5*time.Second, "reconciliation interval")
	heartbeatTimeout := flag.Duration("heartbeat-timeout", 15*time.Second,
		"how long a worker may go quiet before its assignments are reclaimed")
	reservationTimeout := flag.Duration("reservation-timeout", 5*time.Second,
		"how long an unconfirmed assignment holds capacity (must be shorter than heartbeat-timeout)")
	packingBias := flag.Float64("packing-bias", 0.7, "0 spreads, 1 packs")
	maxAttempts := flag.Int("max-attempts", 3, "per-task retry bound")
	spreadGangs := flag.Bool("spread-gangs", false,
		"place gang members in distinct zones, trading locality for failure independence")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := dovetail.Config{
		HeartbeatTimeout:       *heartbeatTimeout,
		ReservationTimeout:     *reservationTimeout,
		MaxAttempts:            *maxAttempts,
		PackingBias:            *packingBias,
		SpreadGangsAcrossZones: *spreadGangs,
	}
	sched, err := dovetail.NewScheduler(cfg)
	if err != nil {
		// A bad configuration is a refusal to start. In particular a
		// reservation timeout longer than the heartbeat timeout makes the
		// reservation path dead code that still looks live, which is worse
		// than not starting.
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	stop := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		logger.Info("shutting down")
		close(stop)
	}()

	srv := dovetail.NewServer(sched, logger)
	if err := srv.Run(*addr, *cycle, *reconcile, stop); err != nil {
		logger.Error("serve", "error", err)
		os.Exit(1)
	}
}
