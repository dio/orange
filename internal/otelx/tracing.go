package otelx

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/dio/orange"

// Tracer returns an Orange package tracer.
func Tracer(pkg string) trace.Tracer {
	return otel.Tracer(instrumentationName + "/" + pkg)
}

// Meter returns an Orange package meter.
func Meter(pkg string) metric.Meter {
	return otel.Meter(instrumentationName + "/" + pkg)
}

// RecordError records err on span and marks the operation failed.
func RecordError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, "operation failed")
}
