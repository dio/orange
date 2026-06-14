package producer

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/dio/orange/internal/otelx"
)

var producerMetrics = newProducerMetrics()

type producerMetricSet struct {
	operations metric.Int64Counter
	duration   metric.Float64Histogram
}

func newProducerMetrics() producerMetricSet {
	meter := otelx.Meter("producer")
	operations, err := meter.Int64Counter(
		"orange.producer.operations",
		metric.WithDescription("Orange producer operation count."),
	)
	if err != nil {
		otel.Handle(err)
	}
	duration, err := meter.Float64Histogram(
		"orange.producer.operation.duration",
		metric.WithDescription("Orange producer operation duration."),
		metric.WithUnit("s"),
	)
	if err != nil {
		otel.Handle(err)
	}
	return producerMetricSet{operations: operations, duration: duration}
}

func recordProducerOperation(ctx context.Context, operation string, result string, start time.Time) {
	opt := metric.WithAttributes(
		attribute.String("orange.operation", operation),
		attribute.String("orange.result", result),
	)
	if producerMetrics.operations != nil {
		producerMetrics.operations.Add(ctx, 1, opt)
	}
	if producerMetrics.duration != nil {
		producerMetrics.duration.Record(ctx, time.Since(start).Seconds(), opt)
	}
}
