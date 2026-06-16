package otelx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var (
	sdkMu       sync.Mutex
	sdkStarted  bool
	sdkShutdown func(context.Context) error
)

// AutoConfigureFromEnv installs a process-global OTel SDK when standard OTEL
// trace or metric exporter environment variables are present. It reports configuration
// failures through the global OTel error handler because Orange constructors do
// not return setup errors.
func AutoConfigureFromEnv() {
	if err := ConfigureFromEnv(context.Background()); err != nil {
		RecordSetupError(err)
	}
}

// RecordSetupError reports automatic instrumentation setup failures through
// the global OTel error handler.
func RecordSetupError(err error) {
	if err != nil {
		otel.Handle(err)
	}
}

// ConfigureFromEnv installs a process-global OTel SDK from standard OTEL trace
// and metric exporter environment variables. If no exporter is configured, it
// is a no-op so library use stays inert by default.
func ConfigureFromEnv(ctx context.Context) error {
	tracesEnabled := traceExportEnabledFromEnv()
	metricsEnabled := metricExportEnabledFromEnv()
	if !tracesEnabled && !metricsEnabled {
		return nil
	}

	sdkMu.Lock()
	defer sdkMu.Unlock()

	if sdkStarted {
		return nil
	}

	tracesEnabled = tracesEnabled && defaultTracerProvider(otel.GetTracerProvider())
	metricsEnabled = metricsEnabled && defaultMeterProvider(otel.GetMeterProvider())
	if !tracesEnabled && !metricsEnabled {
		return nil
	}

	res, err := resource.New(ctx, resource.WithFromEnv(), resource.WithTelemetrySDK())
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return fmt.Errorf("create otel resource: %w", err)
	}

	var traceExp *otlptrace.Exporter
	var metricExp sdkmetric.Exporter
	var exportShutdowns []func(context.Context) error
	if tracesEnabled {
		exp, err := traceExporter(ctx)
		if err != nil {
			return err
		}
		traceExp = exp
		exportShutdowns = append(exportShutdowns, exp.Shutdown)
	}
	if metricsEnabled {
		exp, err := metricExporter(ctx)
		if err != nil {
			return joinSetupError(ctx, err, exportShutdowns)
		}
		metricExp = exp
	}

	var shutdowns []func(context.Context) error
	if traceExp != nil {
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(traceExp),
			sdktrace.WithResource(res),
		)
		otel.SetTracerProvider(tp)
		shutdowns = append(shutdowns, tp.Shutdown)
	}
	if metricExp != nil {
		mp := sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
			sdkmetric.WithResource(res),
		)
		otel.SetMeterProvider(mp)
		shutdowns = append(shutdowns, mp.Shutdown)
	}
	if traceExp != nil && defaultTextMapPropagator(otel.GetTextMapPropagator()) {
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
	}

	sdkStarted = true
	sdkShutdown = func(ctx context.Context) error {
		errs := make([]error, 0, len(shutdowns))
		for _, shutdown := range shutdowns {
			errs = append(errs, shutdown(ctx))
		}
		return errors.Join(errs...)
	}
	return nil
}

func defaultTracerProvider(tp trace.TracerProvider) bool {
	return providerTypeName(tp) == "tracerProvider"
}

func defaultMeterProvider(mp metric.MeterProvider) bool {
	return providerTypeName(mp) == "meterProvider"
}

func defaultTextMapPropagator(p propagation.TextMapPropagator) bool {
	return providerTypeName(p) == "textMapPropagator" && len(p.Fields()) == 0
}

func providerTypeName(provider any) string {
	if provider == nil {
		return ""
	}
	typ := reflect.TypeOf(provider)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.PkgPath() != "go.opentelemetry.io/otel/internal/global" {
		return ""
	}
	return typ.Name()
}

func joinSetupError(ctx context.Context, err error, shutdowns []func(context.Context) error) error {
	errs := make([]error, 0, len(shutdowns)+1)
	errs = append(errs, err)
	for _, shutdown := range shutdowns {
		if shutdownErr := shutdown(ctx); shutdownErr != nil {
			errs = append(errs, fmt.Errorf("shutdown partial otel setup: %w", shutdownErr))
		}
	}
	return errors.Join(errs...)
}

// Shutdown flushes and stops the SDK installed by ConfigureFromEnv. It is a
// no-op when Orange did not install an SDK.
func Shutdown(ctx context.Context) error {
	sdkMu.Lock()
	shutdown := sdkShutdown
	sdkStarted = false
	sdkShutdown = nil
	sdkMu.Unlock()

	if shutdown == nil {
		return nil
	}
	return shutdown(ctx)
}

func traceExporter(ctx context.Context) (*otlptrace.Exporter, error) {
	switch traceProtocolFromEnv() {
	case "grpc":
		return otlptracegrpc.New(ctx)
	case "", "http/protobuf":
		return otlptracehttp.New(ctx)
	default:
		return nil, fmt.Errorf("unsupported OTLP trace protocol %q", traceProtocolFromEnv())
	}
}

func metricExporter(ctx context.Context) (sdkmetric.Exporter, error) {
	switch metricProtocolFromEnv() {
	case "grpc":
		return otlpmetricgrpc.New(ctx)
	case "", "http/protobuf":
		return otlpmetrichttp.New(ctx)
	default:
		return nil, fmt.Errorf("unsupported OTLP metric protocol %q", metricProtocolFromEnv())
	}
}

func traceExportEnabledFromEnv() bool {
	return signalExportEnabledFromEnv("TRACES", []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_PROTOCOL",
		"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
	})
}

func metricExportEnabledFromEnv() bool {
	return signalExportEnabledFromEnv("METRICS", []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_EXPORTER_OTLP_PROTOCOL",
		"OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
	})
}

func signalExportEnabledFromEnv(signal string, fallbackKeys []string) bool {
	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") {
		return false
	}

	exporter := strings.TrimSpace(os.Getenv("OTEL_" + signal + "_EXPORTER"))
	if exporter != "" {
		for _, name := range strings.Split(exporter, ",") {
			switch strings.ToLower(strings.TrimSpace(name)) {
			case "none":
				return false
			case "otlp":
				return true
			}
		}
		return false
	}

	for _, key := range fallbackKeys {
		if strings.TrimSpace(os.Getenv(key)) != "" {
			return true
		}
	}
	return false
}

func traceProtocolFromEnv() string {
	if protocol := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")); protocol != "" {
		return strings.ToLower(protocol)
	}
	return strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")))
}

func metricProtocolFromEnv() string {
	if protocol := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")); protocol != "" {
		return strings.ToLower(protocol)
	}
	return strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")))
}
