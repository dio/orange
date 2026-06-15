package config

import (
	"context"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"

	inspectv1 "github.com/dio/orange/api/orange/inspect/v1"
	"github.com/dio/orange/api/orange/inspect/v1/inspectv1connect"
	"github.com/dio/orange/internal/otelx"
)

type (
	StatusRequest  = inspectv1.StatusRequest
	StatusResponse = inspectv1.StatusResponse

	GenerationRequest  = inspectv1.GenerationRequest
	GenerationResponse = inspectv1.GenerationResponse

	ScopesRequest  = inspectv1.ScopesRequest
	ScopesResponse = inspectv1.ScopesResponse

	ProvidersRequest  = inspectv1.ProvidersRequest
	ProvidersResponse = inspectv1.ProvidersResponse

	ModelsRequest  = inspectv1.ModelsRequest
	ModelsResponse = inspectv1.ModelsResponse

	ResolveLLMRequest  = inspectv1.ResolveLLMRequest
	ResolveLLMResponse = inspectv1.ResolveLLMResponse

	ResolveMCPRequest  = inspectv1.ResolveMCPRequest
	ResolveMCPResponse = inspectv1.ResolveMCPResponse

	MatchViewRequest  = inspectv1.MatchViewRequest
	MatchViewResponse = inspectv1.MatchViewResponse

	PickViewRequest  = inspectv1.PickViewRequest
	PickViewResponse = inspectv1.PickViewResponse

	AdaptViewRequest  = inspectv1.AdaptViewRequest
	AdaptViewResponse = inspectv1.AdaptViewResponse

	GenerationInfo = inspectv1.GenerationInfo
	ComponentInfo  = inspectv1.ComponentInfo
	ScopeInfo      = inspectv1.ScopeInfo
	ProviderInfo   = inspectv1.ProviderInfo
	ModelInfo      = inspectv1.ModelInfo
	LLMRoute       = inspectv1.LLMRoute
	MCPRoute       = inspectv1.MCPRoute
	MCPTool        = inspectv1.MCPTool
)

const InspectionProtocolVersion uint32 = 1

// InspectionService is the embedder-facing implementation interface for
// orange.inspect.v1.InspectionService. Implementations should answer each call
// from one immutable active data-plane generation captured at method entry.
type InspectionService interface {
	Status(context.Context, *StatusRequest) (*StatusResponse, error)
	Generation(context.Context, *GenerationRequest) (*GenerationResponse, error)
	Scopes(context.Context, *ScopesRequest) (*ScopesResponse, error)
	Providers(context.Context, *ProvidersRequest) (*ProvidersResponse, error)
	Models(context.Context, *ModelsRequest) (*ModelsResponse, error)
	ResolveLLM(context.Context, *ResolveLLMRequest) (*ResolveLLMResponse, error)
	ResolveMCP(context.Context, *ResolveMCPRequest) (*ResolveMCPResponse, error)
	MatchView(context.Context, *MatchViewRequest) (*MatchViewResponse, error)
	PickView(context.Context, *PickViewRequest) (*PickViewResponse, error)
	AdaptView(context.Context, *AdaptViewRequest) (*AdaptViewResponse, error)
}

// InspectorOptions configures an attachable inspection handler.
type InspectorOptions struct {
	Service        InspectionService
	HandlerOptions []connect.HandlerOption
}

// Inspector adapts an embedder-owned InspectionService to the generated Connect
// inspection service.
type Inspector struct {
	service        InspectionService
	handlerOptions []connect.HandlerOption
}

// NewInspector creates an inspection facade. The service is required.
func NewInspector(opts InspectorOptions) (*Inspector, error) {
	otelx.AutoConfigureFromEnv()

	if opts.Service == nil {
		return nil, fmt.Errorf("inspection service is required")
	}
	return &Inspector{
		service:        opts.Service,
		handlerOptions: append([]connect.HandlerOption(nil), opts.HandlerOptions...),
	}, nil
}

// Handler returns the Connect InspectionService mount path and handler.
func (i *Inspector) Handler() (string, http.Handler) {
	otelx.AutoConfigureFromEnv()

	handlerOptions := append([]connect.HandlerOption(nil), i.handlerOptions...)
	if interceptor, err := otelconnect.NewInterceptor(); err == nil {
		handlerOptions = append(handlerOptions, connect.WithInterceptors(interceptor))
	} else {
		otelx.RecordSetupError(err)
	}
	return inspectv1connect.NewInspectionServiceHandler(inspectionConnectAdapter{service: i.service}, handlerOptions...)
}

// Mount attaches the InspectionService handler to mux.
func (i *Inspector) Mount(mux *http.ServeMux) string {
	path, handler := i.Handler()
	mux.Handle(path, handler)
	return path
}

type inspectionConnectAdapter struct {
	service InspectionService
}

func (a inspectionConnectAdapter) Status(
	ctx context.Context,
	req *connect.Request[inspectv1.StatusRequest],
) (*connect.Response[inspectv1.StatusResponse], error) {
	return connectResponse(a.service.Status(ctx, req.Msg))
}

func (a inspectionConnectAdapter) Generation(
	ctx context.Context,
	req *connect.Request[inspectv1.GenerationRequest],
) (*connect.Response[inspectv1.GenerationResponse], error) {
	return connectResponse(a.service.Generation(ctx, req.Msg))
}

func (a inspectionConnectAdapter) Scopes(
	ctx context.Context,
	req *connect.Request[inspectv1.ScopesRequest],
) (*connect.Response[inspectv1.ScopesResponse], error) {
	return connectResponse(a.service.Scopes(ctx, req.Msg))
}

func (a inspectionConnectAdapter) Providers(
	ctx context.Context,
	req *connect.Request[inspectv1.ProvidersRequest],
) (*connect.Response[inspectv1.ProvidersResponse], error) {
	return connectResponse(a.service.Providers(ctx, req.Msg))
}

func (a inspectionConnectAdapter) Models(
	ctx context.Context,
	req *connect.Request[inspectv1.ModelsRequest],
) (*connect.Response[inspectv1.ModelsResponse], error) {
	return connectResponse(a.service.Models(ctx, req.Msg))
}

func (a inspectionConnectAdapter) ResolveLLM(
	ctx context.Context,
	req *connect.Request[inspectv1.ResolveLLMRequest],
) (*connect.Response[inspectv1.ResolveLLMResponse], error) {
	return connectResponse(a.service.ResolveLLM(ctx, req.Msg))
}

func (a inspectionConnectAdapter) ResolveMCP(
	ctx context.Context,
	req *connect.Request[inspectv1.ResolveMCPRequest],
) (*connect.Response[inspectv1.ResolveMCPResponse], error) {
	return connectResponse(a.service.ResolveMCP(ctx, req.Msg))
}

func (a inspectionConnectAdapter) MatchView(
	ctx context.Context,
	req *connect.Request[inspectv1.MatchViewRequest],
) (*connect.Response[inspectv1.MatchViewResponse], error) {
	return connectResponse(a.service.MatchView(ctx, req.Msg))
}

func (a inspectionConnectAdapter) PickView(
	ctx context.Context,
	req *connect.Request[inspectv1.PickViewRequest],
) (*connect.Response[inspectv1.PickViewResponse], error) {
	return connectResponse(a.service.PickView(ctx, req.Msg))
}

func (a inspectionConnectAdapter) AdaptView(
	ctx context.Context,
	req *connect.Request[inspectv1.AdaptViewRequest],
) (*connect.Response[inspectv1.AdaptViewResponse], error) {
	return connectResponse(a.service.AdaptView(ctx, req.Msg))
}

func connectResponse[T any](msg *T, err error) (*connect.Response[T], error) {
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("inspection service returned nil response"))
	}
	return connect.NewResponse(msg), nil
}
