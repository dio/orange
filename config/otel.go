package config

import (
	"context"

	"github.com/dio/orange/internal/otelx"
)

// ConfigureOpenTelemetryFromEnv installs the same process-global OTel SDK that
// Orange auto-installs when standard OTEL trace exporter environment variables
// are present. Embedders that want startup error handling can call this before
// constructing Orange servers or producers.
func ConfigureOpenTelemetryFromEnv(ctx context.Context) error {
	return otelx.ConfigureFromEnv(ctx)
}

// ShutdownOpenTelemetry flushes and stops the OTel SDK installed by Orange.
// It is a no-op if Orange did not install an SDK.
func ShutdownOpenTelemetry(ctx context.Context) error {
	return otelx.Shutdown(ctx)
}
