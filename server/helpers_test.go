package server_test

import (
	"context"
	"time"

	cherry "github.com/dio/cherry"
	"github.com/dio/orange/producer"
	"github.com/dio/orange/snapshot"
)

func testBuilder() *producer.Builder {
	return producer.NewBuilder(producer.Options{
		Producer: "test",
		Clock:    func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
	})
}

func successCallback(_ string) snapshot.MutationCallback {
	return func(_ context.Context, _ snapshot.MutationRequest) (producer.BuildResult, error) {
		return producer.BuildResult{
			SourceRevision: "rev-1",
			Scopes:         []string{"ws-1"},
			Input: cherry.Input{
				Providers: []cherry.Provider{{
					ID: "openai", Kind: "openai",
					Endpoint:  "https://api.openai.com",
					SecretRef: "env://OPENAI_API_KEY",
				}},
				Models: []cherry.Model{{ID: "gpt-4o-mini", Provider: "openai", Name: "gpt-4o-mini"}},
				Scopes: []cherry.Scope{{
					ID: "ws-1",
					Principals: []cherry.Principal{{
						Slug:  "slug:1",
						Route: cherry.RoutePlan{Provider: "openai", Model: "gpt-4o-mini"},
						Rate:  cherry.RatePolicy{USDPerDayCents: 500, RPM: 30, OnExceed: "reject"},
					}},
				}},
			},
		}, nil
	}
}
