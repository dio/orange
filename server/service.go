package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/dio/orange/api/orange/config/admin/v1/adminv1connect"
	"github.com/dio/orange/api/orange/config/v1/configv1connect"
	"github.com/dio/orange/snapshot"
)

// Service bundles the snapshot manager with auth/lane hooks and exposes
// Connect handler pairs for mux embedding.
type Service struct {
	snapshot        *SnapshotService
	admin           *AdminService
	shutdownTimeout time.Duration
}

// ServiceOptions configures a Service. Missing auth and lane hooks fail closed.
type ServiceOptions struct {
	Manager         *snapshot.Manager
	Auth            Authenticator
	Lanes           LaneResolver
	ShutdownTimeout time.Duration
}

// NewService creates a Service from the provided options.
func NewService(opts ServiceOptions) *Service {
	shutdownTimeout := opts.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = 5 * time.Second
	}
	return &Service{
		snapshot:        NewSnapshotService(opts.Manager, opts.Auth, opts.Lanes),
		admin:           NewAdminService(opts.Manager, opts.Auth, opts.Lanes),
		shutdownTimeout: shutdownTimeout,
	}
}

// SnapshotServiceHandler returns the Connect path prefix and http.Handler for
// orange.config.v1.SnapshotService. Mount it with mux.Handle(path, handler).
func (s *Service) SnapshotServiceHandler() (string, http.Handler) {
	return configv1connect.NewSnapshotServiceHandler(s.snapshot)
}

// ConfigAdminServiceHandler returns the Connect path prefix and http.Handler
// for orange.config.admin.v1.ConfigAdminService. Mount it with
// mux.Handle(path, handler).
func (s *Service) ConfigAdminServiceHandler() (string, http.Handler) {
	return adminv1connect.NewConfigAdminServiceHandler(s.admin)
}

// Serve mounts both Connect handlers and serves HTTP on lis until ctx is
// cancelled. It performs a graceful shutdown and returns after in-flight
// requests complete or the configured drain timeout expires.
func (s *Service) Serve(ctx context.Context, lis net.Listener) error {
	mux := http.NewServeMux()
	snapPath, snapHandler := s.SnapshotServiceHandler()
	mux.Handle(snapPath, snapHandler)
	adminPath, adminHandler := s.ConfigAdminServiceHandler()
	mux.Handle(adminPath, adminHandler)

	srv := &http.Server{Handler: mux}

	serveErr := make(chan error, 1)
	go func() {
		err := srv.Serve(lis)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveErr <- err
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.shutdownTimeout)
		defer cancel()
		shutdownErr := srv.Shutdown(shutCtx)
		if shutdownErr != nil {
			_ = srv.Close()
		}
		err := <-serveErr
		if shutdownErr != nil {
			return shutdownErr
		}
		return err
	}
}

// ListenAndServe binds to addr and calls Serve. addr is passed to net.Listen
// so "127.0.0.1:0" is valid and lets the OS pick a free port.
func (s *Service) ListenAndServe(ctx context.Context, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ctx, lis)
}
