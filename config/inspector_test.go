package config_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dio/orange/api/orange/inspect/v1/inspectv1connect"
	"github.com/dio/orange/config"
)

func TestNewInspectorRequiresService(t *testing.T) {
	inspector, err := config.NewInspector(config.InspectorOptions{})

	require.Error(t, err)
	assert.Nil(t, inspector)
	assert.Contains(t, err.Error(), "inspection service is required")
}

func TestInspectorMountServesGeneratedConnectEndpoint(t *testing.T) {
	service := &fakeInspectionService{
		status: &config.StatusResponse{
			ProtocolVersion: config.InspectionProtocolVersion,
			Capabilities:    []string{"status", "generation"},
			HasGeneration:   true,
			Generation: &config.GenerationInfo{
				GenerationId:   "gen-1",
				Revision:       "rev-1",
				SourceRevision: "source-1",
			},
		},
	}
	inspector, err := config.NewInspector(config.InspectorOptions{Service: service})
	require.NoError(t, err)

	mux := http.NewServeMux()
	path := inspector.Mount(mux)
	assert.Equal(t, "/orange.inspect.v1.InspectionService/", path)

	server := httptest.NewServer(mux)
	defer server.Close()

	client := inspectv1connect.NewInspectionServiceClient(server.Client(), server.URL)
	resp, err := client.Status(context.Background(), connect.NewRequest(&config.StatusRequest{
		ClientProtocolVersion: config.InspectionProtocolVersion,
	}))

	require.NoError(t, err)
	require.NotNil(t, resp.Msg)
	assert.Equal(t, uint32(1), resp.Msg.ProtocolVersion)
	assert.True(t, resp.Msg.HasGeneration)
	assert.Equal(t, "gen-1", resp.Msg.Generation.GetGenerationId())
	assert.Equal(t, uint32(1), service.statusClientProtocol)
}

func TestInspectorPropagatesServiceConnectError(t *testing.T) {
	service := &fakeInspectionService{
		err: connect.NewError(connect.CodeFailedPrecondition, errors.New("no active generation")),
	}
	inspector, err := config.NewInspector(config.InspectorOptions{Service: service})
	require.NoError(t, err)

	mux := http.NewServeMux()
	inspector.Mount(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	client := inspectv1connect.NewInspectionServiceClient(server.Client(), server.URL)
	resp, err := client.Generation(context.Background(), connect.NewRequest(&config.GenerationRequest{}))

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "no active generation")
}

func TestInspectorRejectsNilServiceResponse(t *testing.T) {
	service := &fakeInspectionService{}
	inspector, err := config.NewInspector(config.InspectorOptions{Service: service})
	require.NoError(t, err)

	mux := http.NewServeMux()
	inspector.Mount(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	client := inspectv1connect.NewInspectionServiceClient(server.Client(), server.URL)
	resp, err := client.Status(context.Background(), connect.NewRequest(&config.StatusRequest{}))

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "nil response")
}

type fakeInspectionService struct {
	status               *config.StatusResponse
	err                  error
	statusClientProtocol uint32
}

func (s *fakeInspectionService) Status(_ context.Context, req *config.StatusRequest) (*config.StatusResponse, error) {
	s.statusClientProtocol = req.GetClientProtocolVersion()
	if s.err != nil {
		return nil, s.err
	}
	return s.status, nil
}

func (s *fakeInspectionService) Generation(context.Context, *config.GenerationRequest) (*config.GenerationResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &config.GenerationResponse{ProtocolVersion: config.InspectionProtocolVersion}, nil
}

func (s *fakeInspectionService) Scopes(context.Context, *config.ScopesRequest) (*config.ScopesResponse, error) {
	return &config.ScopesResponse{}, s.err
}

func (s *fakeInspectionService) Providers(context.Context, *config.ProvidersRequest) (*config.ProvidersResponse, error) {
	return &config.ProvidersResponse{}, s.err
}

func (s *fakeInspectionService) Models(context.Context, *config.ModelsRequest) (*config.ModelsResponse, error) {
	return &config.ModelsResponse{}, s.err
}

func (s *fakeInspectionService) ResolveLLM(context.Context, *config.ResolveLLMRequest) (*config.ResolveLLMResponse, error) {
	return &config.ResolveLLMResponse{}, s.err
}

func (s *fakeInspectionService) ResolveMCP(context.Context, *config.ResolveMCPRequest) (*config.ResolveMCPResponse, error) {
	return &config.ResolveMCPResponse{}, s.err
}

func (s *fakeInspectionService) MatchView(context.Context, *config.MatchViewRequest) (*config.MatchViewResponse, error) {
	return &config.MatchViewResponse{}, s.err
}

func (s *fakeInspectionService) PickView(context.Context, *config.PickViewRequest) (*config.PickViewResponse, error) {
	return &config.PickViewResponse{}, s.err
}

func (s *fakeInspectionService) AdaptView(context.Context, *config.AdaptViewRequest) (*config.AdaptViewResponse, error) {
	return &config.AdaptViewResponse{}, s.err
}
