package mappedsplit

import (
	"context"
	"testing"

	"github.com/dio/cherry"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestBuilderDuplicateComponentRecordsSpanError(t *testing.T) {
	oldTracerProvider := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(tracerProvider)
	t.Cleanup(func() {
		require.NoError(t, tracerProvider.Shutdown(context.Background()))
		otel.SetTracerProvider(oldTracerProvider)
	})

	spec := cherry.MappedSplitSpec{LLMUserKeyPartitions: 1, MCPUserProfilePartitions: 1}
	req := buildRequest(spec, "gen1", 1, testInput("orange://alice/openai"), -1)
	req.Components = append(req.Components, req.Components[0])

	_, err := NewBuilder(BuildOptions{Producer: "test"}).Build(context.Background(), req)
	require.ErrorContains(t, err, "duplicate mapped split component")

	span := requireEndedSpan(t, recorder, "orange.mappedsplit.Builder.Build")
	require.Equal(t, codes.Error, span.Status().Code)
	require.Equal(t, int64(len(req.Components)), spanAttribute(t, span.Attributes(), "orange.component_count").AsInt64())
	require.NotEmpty(t, span.Events())
	require.Equal(t, "exception", span.Events()[0].Name)
}

func requireEndedSpan(t *testing.T, recorder *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()

	for _, span := range recorder.Ended() {
		if span.Name() == name {
			return span
		}
	}
	t.Fatalf("span %q was not exported", name)
	return nil
}

func spanAttribute(t *testing.T, attrs []attribute.KeyValue, key attribute.Key) attribute.Value {
	t.Helper()

	for _, attr := range attrs {
		if attr.Key == key {
			return attr.Value
		}
	}
	t.Fatalf("span attribute %q was not exported", key)
	return attribute.Value{}
}
