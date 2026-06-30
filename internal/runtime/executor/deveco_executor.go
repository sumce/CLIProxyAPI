package executor

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/auth/deveco"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/sjson"
)

// modelAPIURL is the Huawei API endpoint for fetching available models.
const modelAPIURL = deveco.DevEcoBaseURL + "/codeGenie/modelConfig"

// DevecoExecutor implements ProviderExecutor for Huawei DevEco Code's MaaS API.
type DevecoExecutor struct {
	provider   string
	cfg        *config.Config
	devecoAuth *deveco.DevecoAuth
}

// NewDevecoExecutor creates a DevEco executor.
func NewDevecoExecutor(cfg *config.Config) *DevecoExecutor {
	return &DevecoExecutor{
		provider:   deveco.DevecoProviderID,
		cfg:        cfg,
		devecoAuth: deveco.NewDevecoAuth(cfg),
	}
}

// Identifier implements coreauth.ProviderExecutor.
func (e *DevecoExecutor) Identifier() string { return e.provider }

func (e *DevecoExecutor) resolveAuth(auth *coreauth.Auth) string {
	if auth == nil || auth.Attributes == nil {
		return ""
	}
	baseURL := strings.TrimSpace(auth.Attributes["base_url"])
	if baseURL == "" {
		baseURL = deveco.DevEcoBaseURL + "/sse/codeGenie/maas/v2"
	}
	return baseURL
}

// Execute handles non-streaming DevEco API requests.
func (e *DevecoExecutor) Execute(ctx context.Context, auth *coreauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ cliproxyexecutor.Response, err error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	reporter := helps.NewExecutorUsageReporter(ctx, e, baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	to := sdktranslator.FromString("openai")

	originalPayload := opts.OriginalRequest
	if len(originalPayload) == 0 {
		originalPayload = req.Payload
	}
	translated := sdktranslator.TranslateRequest(from, to, baseModel, req.Payload, opts.Stream)

	translated, err = thinking.ApplyThinking(translated, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	requestPath := helps.PayloadRequestPath(opts)
	// Avoid duplicate translation when originalPayload == req.Payload (common case)
	originalTranslated := translated
	if len(opts.OriginalRequest) > 0 {
		originalTranslated = sdktranslator.TranslateRequest(from, to, baseModel, originalPayload, opts.Stream)
	}
	translated = helps.ApplyPayloadConfigWithRequest(e.cfg, baseModel, to.String(), from.String(), "", translated, originalTranslated, requestedModel, requestPath, opts.Headers)
	reporter.SetTranslatedReasoningEffort(translated, to.String())

	// DevEco non-streaming: /no-stream/chat/completions
	baseURL := e.resolveAuth(auth)
	url := strings.TrimSuffix(baseURL, "/") + "/no-stream/chat/completions"
	body := translated
	if !opts.Stream {
		if b, setErr := sjson.SetBytes(body, "stream", false); setErr != nil {
			log.Warnf("deveco: failed to set stream=false: %v", setErr)
		} else {
			body = b
		}
	}

	var httpReq *http.Request
	httpReq, err = http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}
	e.injectHeaders(httpReq, auth)
	httpReq.Header.Set("Content-Type", "application/json")
	util.ApplyCustomHeadersFromAttrs(httpReq, auth.Attributes)

	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL: url, Method: http.MethodPost, Headers: httpReq.Header.Clone(),
		Body: body, Provider: e.Identifier(), AuthID: auth.ID,
	})

	httpClient := helps.NewProxyAwareHTTPClient(ctx, e.cfg, auth, 0)
	httpClient = reporter.TrackHTTPClient(httpClient)
	var httpResp *http.Response
	httpResp, err = httpClient.Do(httpReq)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return cliproxyexecutor.Response{}, err
	}
	defer httpResp.Body.Close()

	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		var b []byte
		b, _ = io.ReadAll(httpResp.Body)
		helps.AppendAPIResponseChunk(ctx, e.cfg, b)
		return cliproxyexecutor.Response{}, statusErr{code: httpResp.StatusCode, msg: string(b)}
	}

	var respBody []byte
	respBody, err = io.ReadAll(httpResp.Body)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return cliproxyexecutor.Response{}, err
	}
	helps.AppendAPIResponseChunk(ctx, e.cfg, respBody)
	reporter.Publish(ctx, helps.ParseOpenAIUsage(respBody))
	reporter.EnsurePublished(ctx)

	var param any
	out := sdktranslator.TranslateNonStream(ctx, to, responseFormat, req.Model, opts.OriginalRequest, translated, respBody, &param)
	return cliproxyexecutor.Response{Payload: out, Headers: httpResp.Header.Clone()}, nil
}

// ExecuteStream handles streaming DevEco API requests.
func (e *DevecoExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	reporter := helps.NewExecutorUsageReporter(ctx, e, baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	to := sdktranslator.FromString("openai")

	originalPayload := opts.OriginalRequest
	if len(originalPayload) == 0 {
		originalPayload = req.Payload
	}
	translated := sdktranslator.TranslateRequest(from, to, baseModel, req.Payload, true)

	translated, err = thinking.ApplyThinking(translated, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return nil, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	requestPath := helps.PayloadRequestPath(opts)
	// Avoid duplicate translation when originalPayload == req.Payload (common case)
	originalTranslated := translated
	if len(opts.OriginalRequest) > 0 {
		originalTranslated = sdktranslator.TranslateRequest(from, to, baseModel, originalPayload, true)
	}
	translated = helps.ApplyPayloadConfigWithRequest(e.cfg, baseModel, to.String(), from.String(), "", translated, originalTranslated, requestedModel, requestPath, opts.Headers)
	if t, setErr := sjson.SetBytes(translated, "stream_options.include_usage", true); setErr != nil {
		log.Warnf("deveco: failed to set stream_options.include_usage: %v", setErr)
	} else {
		translated = t
	}
	reporter.SetTranslatedReasoningEffort(translated, to.String())

	// DevEco streaming: standard /chat/completions
	baseURL := e.resolveAuth(auth)
	url := strings.TrimSuffix(baseURL, "/") + "/chat/completions"

	var httpReq *http.Request
	httpReq, err = http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(translated))
	if err != nil {
		return nil, err
	}
	e.injectHeaders(httpReq, auth)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	httpReq.Header.Set("Cache-Control", "no-cache")
	util.ApplyCustomHeadersFromAttrs(httpReq, auth.Attributes)

	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL: url, Method: http.MethodPost, Headers: httpReq.Header.Clone(),
		Body: translated, Provider: e.Identifier(), AuthID: auth.ID,
	})

	httpClient := helps.NewProxyAwareHTTPClient(ctx, e.cfg, auth, 0)
	httpClient = reporter.TrackHTTPClient(httpClient)
	var httpResp *http.Response
	httpResp, err = httpClient.Do(httpReq)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, err
	}
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		var b []byte
		b, _ = io.ReadAll(httpResp.Body)
		httpResp.Body.Close()
		return nil, statusErr{code: httpResp.StatusCode, msg: string(b)}
	}

	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(out)
		defer httpResp.Body.Close()

		scanner := bufio.NewScanner(httpResp.Body)
		scanner.Buffer(nil, 52_428_800)
		var param any
		for scanner.Scan() {
			line := scanner.Bytes()
			helps.AppendAPIResponseChunk(ctx, e.cfg, line)
			if detail, ok := helps.ParseOpenAIStreamUsage(line); ok {
				reporter.Publish(ctx, detail)
			}
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) == 0 {
				continue
			}
			if !bytes.HasPrefix(trimmed, []byte("data:")) {
				if bytes.HasPrefix(trimmed, []byte(":")) || bytes.HasPrefix(trimmed, []byte("event:")) ||
					bytes.HasPrefix(trimmed, []byte("id:")) || bytes.HasPrefix(trimmed, []byte("retry:")) {
					continue
				}
				continue
			}
			chunks := sdktranslator.TranslateStream(ctx, to, responseFormat, req.Model, opts.OriginalRequest, translated, bytes.Clone(trimmed), &param)
			for i := range chunks {
				select {
				case out <- cliproxyexecutor.StreamChunk{Payload: chunks[i]}:
				case <-ctx.Done():
					return
				}
			}
		}
		doneChunks := sdktranslator.TranslateStream(ctx, to, responseFormat, req.Model, opts.OriginalRequest, translated, []byte("data: [DONE]"), &param)
		for i := range doneChunks {
			select {
			case out <- cliproxyexecutor.StreamChunk{Payload: doneChunks[i]}:
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			log.Errorf("deveco: stream read error for auth %s: %v", auth.ID, err)
			select {
			case out <- cliproxyexecutor.StreamChunk{Payload: []byte(fmt.Sprintf(`data: {"error":"stream read error: %v"}`, err))}:
			case <-ctx.Done():
			}
			return
		}
		reporter.EnsurePublished(ctx)
	}()
	return &cliproxyexecutor.StreamResult{Headers: httpResp.Header.Clone(), Chunks: out}, nil
}

// CountTokens delegates to the OpenAI-compatible token counter.
func (e *DevecoExecutor) CountTokens(ctx context.Context, auth *coreauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ cliproxyexecutor.Response, err error) {
	inner := NewOpenAICompatExecutor(deveco.DevecoProviderID, e.cfg)
	return inner.CountTokens(ctx, auth, req, opts)
}

// HttpRequest injects DevEco credentials into the request and executes it.
func (e *DevecoExecutor) HttpRequest(ctx context.Context, auth *coreauth.Auth, req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("deveco: request is nil")
	}
	e.injectHeaders(req, auth)
	httpClient := helps.NewProxyAwareHTTPClient(ctx, e.cfg, auth, 0)
	return httpClient.Do(req)
}

// Refresh handles DevEco token refresh using the stored JWT.
func (e *DevecoExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	if auth == nil {
		return nil, fmt.Errorf("deveco refresh: auth is nil")
	}
	if refreshed, handled, err := helps.RefreshAuthViaHome(ctx, e.cfg, auth); handled {
		return refreshed, err
	}

	jwtRaw, ok := auth.Metadata["jwt_token"]
	if !ok {
		log.Warnf("deveco refresh: no jwt_token for auth %s", auth.ID)
		return auth, nil
	}
	jwtToken, _ := jwtRaw.(string)
	if jwtToken == "" {
		return auth, nil
	}

	result, err := e.devecoAuth.RefreshToken(ctx, jwtToken)
	if err != nil {
		log.Errorf("deveco refresh: failed for auth %s: %v", auth.ID, err)
		return auth, err
	}

	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	auth.Metadata["access_token"] = result.AccessToken
	auth.Metadata["refresh_token"] = result.RefreshToken
	auth.Metadata["expires_at"] = float64(result.ExpiresAt)

	now := time.Now()
	auth.NextRefreshAfter = now.Add(25 * time.Minute)
	auth.LastRefreshedAt = now
	log.Infof("deveco: refreshed token for auth %s (user: %s)", auth.ID, result.UserName)
	return auth, nil
}

func (e *DevecoExecutor) injectHeaders(req *http.Request, auth *coreauth.Auth) {
	if req == nil || auth == nil {
		return
	}
	if token := extractAccessToken(auth); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if req.Header.Get("lang") == "" {
		req.Header.Set("lang", "en")
	}
	if req.Header.Get("Chat-Id") == "" {
		req.Header.Set("Chat-Id", newChatID())
	}
}

func extractAccessToken(auth *coreauth.Auth) string {
	if auth == nil || auth.Metadata == nil {
		return ""
	}
	token, _ := auth.Metadata["access_token"].(string)
	return token
}

func newChatID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// FetchAndRegisterDevecoModels fetches the latest model list from the Huawei DevEco API
// and registers them in the global model registry. This enables dynamic model discovery
// (e.g., GLM-5.1 when it becomes available through the user's account).
// The httpClient should be proxy-aware; use nil to create a default client.
func FetchAndRegisterDevecoModels(ctx context.Context, accessToken string, installationVersion string, httpClient *http.Client) ([]*registry.ModelInfo, error) {
	url := fmt.Sprintf("%s?localVersion=0&pluginVersion=CLI.%s", modelAPIURL, installationVersion)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("deveco fetch models: create req: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")

	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("deveco fetch models: http: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("deveco fetch models: read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("deveco fetch models: HTTP %d", resp.StatusCode)
	}

	// Parse the Huawei model config API response
	var apiResp struct {
		Code int `json:"code"`
		Body struct {
			Version     int `json:"version"`
			InnerModels []struct {
				Protocol        string `json:"protocol"`
				GroupName       string `json:"group_name"`
				GroupNameCN     string `json:"group_name_cn,omitempty"`
				ModelConfigs    []struct {
					ID              int      `json:"id"`
					ModelID         string   `json:"model_id"`
					ThinkingMode    string   `json:"thinking_mode"`
					InputModalities []string `json:"input_modalities,omitempty"`
					ContextWindow   int      `json:"context_window,omitempty"`
					Output          int      `json:"output,omitempty"`
					ToolCallMode    string   `json:"tool_call_mode"`
				} `json:"model_configs"`
			} `json:"inner_models"`
		} `json:"body"`
	}
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("deveco fetch models: parse: %w", err)
	}
	if apiResp.Code != 200 {
		return nil, fmt.Errorf("deveco fetch models: API code %d", apiResp.Code)
	}

	models := make([]*registry.ModelInfo, 0)
	for _, group := range apiResp.Body.InnerModels {
		for _, mc := range group.ModelConfigs {
			ctxLen := mc.ContextWindow
			if ctxLen <= 0 {
				ctxLen = 202752
			}
			maxOut := mc.Output
			if maxOut <= 0 {
				maxOut = 131072
			}
			models = append(models, &registry.ModelInfo{
				ID:                 mc.ModelID,
				Object:             "model",
				OwnedBy:            "deveco",
				Type:               "deveco",
				DisplayName:        mc.ModelID,
				ContextLength:      ctxLen,
				MaxCompletionTokens: maxOut,
			})
		}
	}
	return models, nil
}

// FetchDevecoModelsDynamic fetches models from the Huawei DevEco API and returns them.
// Falls back to default models if the API call fails or returns empty.
func FetchDevecoModelsDynamic(ctx context.Context, cfg *config.Config, auth *coreauth.Auth) []*registry.ModelInfo {
	accessToken := extractAccessToken(auth)
	if accessToken == "" {
		log.Debug("deveco: no access token for dynamic model fetch, using defaults")
		return registry.GetDevecoModels()
	}

	httpClient := helps.NewProxyAwareHTTPClient(ctx, cfg, auth, 10*time.Second)
	models, err := FetchAndRegisterDevecoModels(ctx, accessToken, "1.0.0", httpClient)
	if err != nil {
		log.Debugf("deveco: dynamic model fetch failed (%v), using defaults", err)
		return registry.GetDevecoModels()
	}

	if len(models) == 0 {
		return registry.GetDevecoModels()
	}
	log.Infof("deveco: fetched %d models from API", len(models))
	return models
}
