package otelx

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceExportEnabledFromEnv(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want bool
	}{
		{
			name: "disabled without exporter env",
			want: false,
		},
		{
			name: "enabled by traces exporter",
			env: map[string]string{
				"OTEL_TRACES_EXPORTER": "otlp",
			},
			want: true,
		},
		{
			name: "disabled by traces exporter none",
			env: map[string]string{
				"OTEL_TRACES_EXPORTER": "none",
			},
			want: false,
		},
		{
			name: "disabled by sdk disabled",
			env: map[string]string{
				"OTEL_SDK_DISABLED":    "true",
				"OTEL_TRACES_EXPORTER": "otlp",
			},
			want: false,
		},
		{
			name: "enabled by generic otlp endpoint",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318",
			},
			want: true,
		},
		{
			name: "enabled by trace protocol",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "grpc",
			},
			want: true,
		},
		{
			name: "unsupported exporter stays disabled",
			env: map[string]string{
				"OTEL_TRACES_EXPORTER": "console",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearOTelEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			if got := traceExportEnabledFromEnv(); got != tt.want {
				t.Fatalf("traceExportEnabledFromEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetricExportEnabledFromEnv(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want bool
	}{
		{
			name: "disabled without exporter env",
			want: false,
		},
		{
			name: "enabled by metrics exporter",
			env: map[string]string{
				"OTEL_METRICS_EXPORTER": "otlp",
			},
			want: true,
		},
		{
			name: "disabled by metrics exporter none",
			env: map[string]string{
				"OTEL_METRICS_EXPORTER": "none",
			},
			want: false,
		},
		{
			name: "disabled by sdk disabled",
			env: map[string]string{
				"OTEL_SDK_DISABLED":     "true",
				"OTEL_METRICS_EXPORTER": "otlp",
			},
			want: false,
		},
		{
			name: "enabled by generic otlp endpoint",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318",
			},
			want: true,
		},
		{
			name: "enabled by metric protocol",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_METRICS_PROTOCOL": "grpc",
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearOTelEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			if got := metricExportEnabledFromEnv(); got != tt.want {
				t.Fatalf("metricExportEnabledFromEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTraceProtocolFromEnv(t *testing.T) {
	clearOTelEnv(t)
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc")

	if got := traceProtocolFromEnv(); got != "grpc" {
		t.Fatalf("traceProtocolFromEnv() = %q, want %q", got, "grpc")
	}
}

func TestMetricProtocolFromEnv(t *testing.T) {
	clearOTelEnv(t)
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "grpc")

	if got := metricProtocolFromEnv(); got != "grpc" {
		t.Fatalf("metricProtocolFromEnv() = %q, want %q", got, "grpc")
	}
}

func TestDefaultProviderDetection(t *testing.T) {
	require.True(t, defaultTracerProvider(otel.GetTracerProvider()))
	require.True(t, defaultMeterProvider(otel.GetMeterProvider()))
	require.True(t, defaultTextMapPropagator(otel.GetTextMapPropagator()))
	require.False(t, defaultTracerProvider(trace.NewNoopTracerProvider()))
	require.False(t, defaultMeterProvider(noop.NewMeterProvider()))
	require.False(t, defaultTextMapPropagator(staticPropagator{}))
}

func TestConfigureFromEnvDoesNotInstallGlobalsOnLaterSetupError(t *testing.T) {
	clearOTelEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_METRICS_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "not-a-protocol")

	oldTracerProvider := otel.GetTracerProvider()
	oldMeterProvider := otel.GetMeterProvider()
	oldPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(oldTracerProvider)
		otel.SetMeterProvider(oldMeterProvider)
		otel.SetTextMapPropagator(oldPropagator)
		resetSDKForTest()
	})

	err := ConfigureFromEnv(context.Background())
	require.ErrorContains(t, err, `unsupported OTLP metric protocol "not-a-protocol"`)
	require.Same(t, oldTracerProvider, otel.GetTracerProvider())
	require.Same(t, oldMeterProvider, otel.GetMeterProvider())
	require.Same(t, oldPropagator, otel.GetTextMapPropagator())
	require.False(t, sdkStarted)
	require.Nil(t, sdkShutdown)
}

func TestConfigureFromEnvPreservesExistingGlobalProviders(t *testing.T) {
	clearOTelEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_METRICS_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")

	oldTracerProvider := otel.GetTracerProvider()
	oldMeterProvider := otel.GetMeterProvider()
	t.Cleanup(func() {
		otel.SetTracerProvider(oldTracerProvider)
		otel.SetMeterProvider(oldMeterProvider)
		resetSDKForTest()
	})

	tracerProvider := trace.NewNoopTracerProvider()
	meterProvider := noop.NewMeterProvider()
	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)

	require.NoError(t, ConfigureFromEnv(context.Background()))
	require.Equal(t, tracerProvider, otel.GetTracerProvider())
	require.Equal(t, meterProvider, otel.GetMeterProvider())
}

func TestConfigureFromEnvPreservesExistingPropagatorForMetricsOnlySetup(t *testing.T) {
	clearOTelEnv(t)
	t.Setenv("OTEL_METRICS_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")

	oldTracerProvider := otel.GetTracerProvider()
	oldMeterProvider := otel.GetMeterProvider()
	oldPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		_ = Shutdown(context.Background())
		otel.SetTracerProvider(oldTracerProvider)
		otel.SetMeterProvider(oldMeterProvider)
		otel.SetTextMapPropagator(oldPropagator)
		resetSDKForTest()
	})

	propagator := staticPropagator{fields: []string{"x-custom-trace"}}
	otel.SetTextMapPropagator(propagator)

	require.NoError(t, ConfigureFromEnv(context.Background()))
	require.Equal(t, propagator, otel.GetTextMapPropagator())
}

func TestConfigureFromEnvPreservesExistingPropagatorForTraceSetup(t *testing.T) {
	clearOTelEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")

	oldTracerProvider := otel.GetTracerProvider()
	oldMeterProvider := otel.GetMeterProvider()
	oldPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		_ = Shutdown(context.Background())
		otel.SetTracerProvider(oldTracerProvider)
		otel.SetMeterProvider(oldMeterProvider)
		otel.SetTextMapPropagator(oldPropagator)
		resetSDKForTest()
	})

	propagator := staticPropagator{fields: []string{"x-custom-trace"}}
	otel.SetTextMapPropagator(propagator)

	require.NoError(t, ConfigureFromEnv(context.Background()))
	require.Equal(t, propagator, otel.GetTextMapPropagator())
}

type staticPropagator struct {
	fields []string
}

func (p staticPropagator) Inject(context.Context, propagation.TextMapCarrier) {}

func (p staticPropagator) Extract(ctx context.Context, _ propagation.TextMapCarrier) context.Context {
	return ctx
}

func (p staticPropagator) Fields() []string {
	return p.fields
}

func clearOTelEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"OTEL_SDK_DISABLED",
		"OTEL_TRACES_EXPORTER",
		"OTEL_METRICS_EXPORTER",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_EXPORTER_OTLP_PROTOCOL",
		"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
		"OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
	} {
		t.Setenv(key, "")
	}
}

func resetSDKForTest() {
	sdkMu.Lock()
	defer sdkMu.Unlock()

	sdkStarted = false
	sdkShutdown = nil
}
