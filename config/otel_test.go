package config

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestConfigOperationMetricsExportNamesAttributesAndResult(t *testing.T) {
	ctx := context.Background()
	oldMeterProvider := otel.GetMeterProvider()
	oldMetrics := orangeMetrics
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(meterProvider)
	orangeMetrics = newOrangeMetrics()
	t.Cleanup(func() {
		orangeMetrics = oldMetrics
		otel.SetMeterProvider(oldMeterProvider)
		require.NoError(t, meterProvider.Shutdown(context.Background()))
	})

	store := NewMemoryStore()
	_, _, err := store.FetchMappedSplitMap(ctx, "lane-a", 7, []byte{1, 2, 3})
	require.Error(t, err)

	var got metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &got))

	ops := requireMetric[metricdata.Sum[int64]](t, got, "orange.config.operations")
	requireDataPointAttrs(t, ops.DataPoints, map[attribute.Key]string{
		"orange.operation": "store.fetch_mapped_split_map",
		"orange.result":    "not_found",
		"orange.store":     "memory",
	})

	duration := requireMetric[metricdata.Histogram[float64]](t, got, "orange.config.operation.duration")
	requireHistogramPointAttrs(t, duration.DataPoints, map[attribute.Key]string{
		"orange.operation": "store.fetch_mapped_split_map",
		"orange.result":    "not_found",
		"orange.store":     "memory",
	})
}

func TestMemoryStoreFetchMappedSplitMapExportsSpanResultAndError(t *testing.T) {
	oldTracerProvider := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(tracerProvider)
	t.Cleanup(func() {
		require.NoError(t, tracerProvider.Shutdown(context.Background()))
		otel.SetTracerProvider(oldTracerProvider)
	})

	store := NewMemoryStore()
	_, _, err := store.FetchMappedSplitMap(context.Background(), "lane-a", 0, nil)
	require.Error(t, err)

	span := requireConfigEndedSpan(t, recorder, "orange.config.MemoryStore.FetchMappedSplitMap")
	require.Equal(t, codes.Error, span.Status().Code)
	require.Equal(t, "not_found", configSpanAttribute(t, span.Attributes(), "orange.result").AsString())
	require.Equal(t, "memory", configSpanAttribute(t, span.Attributes(), "orange.store").AsString())
	require.Equal(t, "lane-a", configSpanAttribute(t, span.Attributes(), "orange.lane").AsString())
	require.NotEmpty(t, span.Events())
	require.Equal(t, "exception", span.Events()[0].Name)
}

func requireMetric[T any](t *testing.T, rm metricdata.ResourceMetrics, name string) T {
	t.Helper()

	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == name {
				data, ok := metric.Data.(T)
				require.Truef(t, ok, "metric %q has data type %T", name, metric.Data)
				return data
			}
		}
	}
	t.Fatalf("metric %q was not exported", name)
	var zero T
	return zero
}

func requireDataPointAttrs[N int64 | float64](t *testing.T, points []metricdata.DataPoint[N], want map[attribute.Key]string) {
	t.Helper()

	for _, point := range points {
		if metricPointHasAttrs(point.Attributes, want) {
			return
		}
	}
	t.Fatalf("metric point with attributes %v was not exported", want)
}

func requireHistogramPointAttrs[N int64 | float64](t *testing.T, points []metricdata.HistogramDataPoint[N], want map[attribute.Key]string) {
	t.Helper()

	for _, point := range points {
		if metricPointHasAttrs(point.Attributes, want) {
			return
		}
	}
	t.Fatalf("metric point with attributes %v was not exported", want)
}

func metricPointHasAttrs(set attribute.Set, want map[attribute.Key]string) bool {
	for key, value := range want {
		got, ok := set.Value(key)
		if !ok || got.AsString() != value {
			return false
		}
	}
	return true
}

func requireConfigEndedSpan(t *testing.T, recorder *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()

	for _, span := range recorder.Ended() {
		if span.Name() == name {
			return span
		}
	}
	t.Fatalf("span %q was not exported", name)
	return nil
}

func configSpanAttribute(t *testing.T, attrs []attribute.KeyValue, key attribute.Key) attribute.Value {
	t.Helper()

	for _, attr := range attrs {
		if attr.Key == key {
			return attr.Value
		}
	}
	t.Fatalf("span attribute %q was not exported", key)
	return attribute.Value{}
}
