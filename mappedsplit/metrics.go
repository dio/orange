package mappedsplit

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/dio/orange/internal/otelx"
)

var mappedSplitMetrics = newMappedSplitMetrics()

type mappedSplitMetricSet struct {
	operations metric.Int64Counter
	duration   metric.Float64Histogram
}

func newMappedSplitMetrics() mappedSplitMetricSet {
	meter := otelx.Meter("mappedsplit")
	operations, err := meter.Int64Counter(
		"orange.mappedsplit.operations",
		metric.WithDescription("Orange mapped-split operation count."),
	)
	if err != nil {
		otel.Handle(err)
	}
	duration, err := meter.Float64Histogram(
		"orange.mappedsplit.operation.duration",
		metric.WithDescription("Orange mapped-split operation duration."),
		metric.WithUnit("s"),
	)
	if err != nil {
		otel.Handle(err)
	}
	return mappedSplitMetricSet{operations: operations, duration: duration}
}

func recordMappedSplitOperation(ctx context.Context, operation string, result string, start time.Time) {
	opt := metric.WithAttributes(
		attribute.String("orange.operation", operation),
		attribute.String("orange.result", result),
	)
	if mappedSplitMetrics.operations != nil {
		mappedSplitMetrics.operations.Add(ctx, 1, opt)
	}
	if mappedSplitMetrics.duration != nil {
		mappedSplitMetrics.duration.Record(ctx, time.Since(start).Seconds(), opt)
	}
}
