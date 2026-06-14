package config

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/dio/orange/internal/otelx"
)

var orangeMetrics = newOrangeMetrics()

type orangeMetricSet struct {
	operations metric.Int64Counter
	duration   metric.Float64Histogram
}

func newOrangeMetrics() orangeMetricSet {
	meter := otelx.Meter("config")
	operations, err := meter.Int64Counter(
		"orange.config.operations",
		metric.WithDescription("Orange config operation count."),
	)
	if err != nil {
		otel.Handle(err)
	}
	duration, err := meter.Float64Histogram(
		"orange.config.operation.duration",
		metric.WithDescription("Orange config operation duration."),
		metric.WithUnit("s"),
	)
	if err != nil {
		otel.Handle(err)
	}
	return orangeMetricSet{operations: operations, duration: duration}
}

func recordConfigOperation(ctx context.Context, operation string, result string, start time.Time, attrs ...attribute.KeyValue) {
	allAttrs := make([]attribute.KeyValue, 0, len(attrs)+2)
	allAttrs = append(allAttrs,
		attribute.String("orange.operation", operation),
		attribute.String("orange.result", result),
	)
	allAttrs = append(allAttrs, attrs...)
	opt := metric.WithAttributes(allAttrs...)
	if orangeMetrics.operations != nil {
		orangeMetrics.operations.Add(ctx, 1, opt)
	}
	if orangeMetrics.duration != nil {
		orangeMetrics.duration.Record(ctx, time.Since(start).Seconds(), opt)
	}
}

func startConfigOperationSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return configTracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

func finishConfigOperationSpan(span trace.Span, result string, err error) {
	if span == nil {
		return
	}
	span.SetAttributes(attribute.String("orange.result", result))
	if err != nil {
		otelx.RecordError(span, err)
	}
	span.End()
}

func captureSpanError(dst *error, err error) {
	if err != nil {
		*dst = err
	}
}

func metricResult(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

func storeErrorResult(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, ErrBuildLeaseHeld):
		return "lease_held"
	case errors.Is(err, ErrBuildLeaseLost):
		return "lease_lost"
	default:
		return "error"
	}
}
