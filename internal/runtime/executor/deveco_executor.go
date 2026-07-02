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
	"sync"
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
	"github.com/tidwall/gjson"
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

// aggregatedDelta accumulates content and reasoning_content from streaming
// SSE delta chunks into a single complete chat-completions response.
type aggregatedDelta struct {
	ID               string
	Model            string
	Created          int64
	Content          strings.Builder
	ReasoningContent strings.Builder
	Usage            json.RawMessage
}

func (a *aggregatedDelta) ingest(line []byte) {
	payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return
	}
	if id := gjson.GetBytes(payload, "id").String(); id != "" {
		a.ID = id
	}
	if model := gjson.GetBytes(payload, "model").String(); model != "" {
		a.Model = model
	}
	if created := gjson.GetBytes(payload, "created").Int(); created > 0 {
		a.Created = created
	}
	if delta := gjson.GetBytes(payload, "choices.0.delta"); delta.Exists() {
		if content := delta.Get("content").String(); content != "" {
			a.Content.WriteString(content)
		}
		if rc := delta.Get("reasoning_content").String(); rc != "" {
			a.ReasoningContent.WriteString(rc)
		}
	}
	if usage := gjson.GetBytes(payload, "usage"); usage.Exists() && usage.IsObject() {
		a.Usage = json.RawMessage(usage.Raw)
	}
}

func (a *aggregatedDelta) toCompleteResponse() []byte {
	out := []byte(`{"id":"","object":"chat.completion","created":0,"model":"","choices":[{"index":0,"message":{"role":"assistant","content":""}}]}`)
	if a.ID != "" {
		out, _ = sjson.SetBytes(out, "id", a.ID)
	}
	if a.Model != "" {
		out, _ = sjson.SetBytes(out, "model", a.Model)
	}
	if a.Created > 0 {
		out, _ = sjson.SetBytes(out, "created", a.Created)
	}
	if a.Content.Len() > 0 {
		out, _ = sjson.SetBytes(out, "choices.0.message.content", a.Content.String())
	}
	if a.ReasoningContent.Len() > 0 {
		out, _ = sjson.SetBytes(out, "choices.0.message.reasoning_content", a.ReasoningContent.String())
	}
	if len(a.Usage) > 0 {
		out, _ = sjson.SetRawBytes(out, "usage", a.Usage)
	}
	return out
}

// Execute handles non-streaming DevEco API requests.
// Internally uses the streaming endpoint (/chat/completions) for consistent latency,
// collects all SSE chunks, and assembles the final response. This avoids the
// unpredictable latency of Huawei's /no-stream/chat/completions endpoint (2s-90s).
//
// Note: deveco-code (official IDE) rewrites the URL to /no-stream/chat/completions
// and sets stream=false for non-streaming requests. CLIProxyAPI intentionally uses
// the streaming path + aggregation instead, which produces the same result with
// more predictable latency.
//
// Unlike the streaming path, this method aggregates all streaming delta chunks
// into a single complete chat-completions response before calling TranslateNonStream.
// This is necessary because TranslateNonStream expects complete message format
// (choices[0].message.reasoning_content), not streaming delta format (choices[0].delta.reasoning_content).
func (e *DevecoExecutor) Execute(ctx context.Context, auth *coreauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ cliproxyexecutor.Response, err error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	reporter := helps.NewExecutorUsageReporter(ctx, e, baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	// Prepare request body (same as ExecuteStream)
	translated, _, errPrepare := e.prepareStreamRequest(ctx, auth, req, opts)
	if errPrepare != nil {
		err = errPrepare
		return cliproxyexecutor.Response{}, err
	}

	to := sdktranslator.FromString("openai")
	reporter.SetTranslatedReasoningEffort(translated, to.String())
	baseURL := e.resolveAuth(auth)
	url := strings.TrimSuffix(baseURL, "/") + "/chat/completions"

	var httpReq *http.Request
	httpReq, err = http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(translated))
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}
	e.injectHeaders(httpReq, auth, opts.Headers)
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
		return cliproxyexecutor.Response{}, err
	}
	defer httpResp.Body.Close()
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		var b []byte
		b, _ = io.ReadAll(httpResp.Body)
		return cliproxyexecutor.Response{}, statusErr{code: httpResp.StatusCode, msg: string(b)}
	}

	// Read SSE stream and aggregate all delta chunks into a complete response.
	scanner := bufio.NewScanner(httpResp.Body)
	scanner.Buffer(nil, 52_428_800)
	var delta aggregatedDelta
	for scanner.Scan() {
		line := scanner.Bytes()
		helps.AppendAPIResponseChunk(ctx, e.cfg, line)
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			continue
		}
		if !bytes.HasPrefix(trimmed, []byte("data:")) {
			continue
		}
		delta.ingest(trimmed)
		if detail, ok := helps.ParseOpenAIStreamUsage(line); ok {
			reporter.Publish(ctx, detail)
		}
	}
	var scanErr error
	if errScan := scanner.Err(); errScan != nil {
		log.Errorf("deveco: stream read error for auth %s: %v", auth.ID, errScan)
		scanErr = errScan
	}
	reporter.EnsurePublished(ctx)
	if scanErr != nil {
		return cliproxyexecutor.Response{}, fmt.Errorf("deveco: stream read error: %w", scanErr)
	}

	// Translate the complete aggregated response once.
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	var param any
	out := sdktranslator.TranslateNonStream(ctx, to, responseFormat, req.Model, opts.OriginalRequest, translated, delta.toCompleteResponse(), &param)

	return cliproxyexecutor.Response{Payload: out, Headers: httpResp.Header.Clone()}, nil
}

// prepareStreamRequest performs the common request preparation steps for streaming.
// Returns the translated request body, the requested model, and any error.
func (e *DevecoExecutor) prepareStreamRequest(ctx context.Context, auth *coreauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (translated []byte, requestedModel string, err error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	from := opts.SourceFormat
	to := sdktranslator.FromString("openai")

	originalPayload := opts.OriginalRequest
	if len(originalPayload) == 0 {
		originalPayload = req.Payload
	}
	translated = sdktranslator.TranslateRequest(from, to, baseModel, req.Payload, true)

	translated, err = thinking.ApplyThinking(translated, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return nil, "", err
	}

	// Huawei's DevEco MaaS API requires BOTH reasoning_effort AND thinking.type
	// to actually return reasoning_content. Sending only reasoning_effort yields
	// zero reasoning lines. When a non-empty/non-"none" effort is present, mirror
	// it with thinking.type=enabled so GLM models emit their reasoning stream.
	if effort := gjson.GetBytes(translated, "reasoning_effort"); effort.Exists() {
		if v := effort.String(); v != "" && v != "none" {
			if t, setErr := sjson.SetBytes(translated, "thinking.type", "enabled"); setErr == nil {
				translated = t
			}
		}
	}

	requestedModel = helps.PayloadRequestedModel(opts, req.Model)
	requestPath := helps.PayloadRequestPath(opts)
	// Avoid duplicate translation when originalPayload == req.Payload (common case)
	originalTranslated := translated
	if len(opts.OriginalRequest) > 0 {
		originalTranslated = sdktranslator.TranslateRequest(from, to, baseModel, originalPayload, true)
	}
	translated = helps.ApplyPayloadConfigWithRequest(e.cfg, baseModel, to.String(), from.String(), "", translated, originalTranslated, requestedModel, requestPath, opts.Headers)
	// Ensure the body model ID matches the resolved base model (strip suffixes like :thinking).
	if m, setErr := sjson.SetBytes(translated, "model", baseModel); setErr == nil {
		translated = m
	}
	if t, setErr := sjson.SetBytes(translated, "stream_options.include_usage", true); setErr != nil {
		log.Warnf("deveco: failed to set stream_options.include_usage: %v", setErr)
	} else {
		translated = t
	}
	return translated, requestedModel, nil
}

// ExecuteStream handles streaming DevEco API requests.
func (e *DevecoExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	reporter := helps.NewExecutorUsageReporter(ctx, e, baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	to := sdktranslator.FromString("openai")

	translated, _, err := e.prepareStreamRequest(ctx, auth, req, opts)
	if err != nil {
		return nil, err
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
	e.injectHeaders(httpReq, auth, opts.Headers)
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
	e.injectHeaders(req, auth, nil)
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

func (e *DevecoExecutor) injectHeaders(req *http.Request, auth *coreauth.Auth, clientHeaders http.Header) {
	if req == nil || auth == nil {
		return
	}
	if token := extractAccessToken(auth); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if req.Header.Get("lang") == "" {
		req.Header.Set("lang", "en")
	}

	// Propagate DevEco session affinity from incoming client headers.
	// The client (e.g. DevEco Code IDE plugin) may send x-deveco-session
	// or x-session-affinity to maintain conversation context.
	if req.Header.Get("Session-Id") == "" {
		if clientHeaders != nil {
			if sessionID := clientHeaders.Get("x-deveco-session"); sessionID != "" {
				req.Header.Set("Session-Id", sessionID)
			} else if sessionID := clientHeaders.Get("x-session-affinity"); sessionID != "" {
				req.Header.Set("Session-Id", sessionID)
			}
		}
	}

	// Forward x-deveco-* headers from client to upstream for request tracking.
	if clientHeaders != nil {
		for _, h := range []string{"x-deveco-session", "x-deveco-request", "x-deveco-client", "x-deveco-project"} {
			if v := clientHeaders.Get(h); v != "" && req.Header.Get(h) == "" {
				req.Header.Set(h, v)
			}
		}
	}

	// Set or forward User-Agent.
	if req.Header.Get("User-Agent") == "" {
		if clientHeaders != nil {
			if ua := clientHeaders.Get("User-Agent"); ua != "" {
				req.Header.Set("User-Agent", ua)
			}
		}
		if req.Header.Get("User-Agent") == "" {
			req.Header.Set("User-Agent", "CLIProxyAPI")
		}
	}

	// Stable Chat-Id per session (matching deveco-code behavior).
	sessionID := req.Header.Get("Session-Id")
	if sessionID != "" {
		if cached, ok := devecoSessionChatID.Load(sessionID); ok {
			if id, ok := cached.(string); ok && id != "" {
				req.Header.Set("Chat-Id", id)
				return
			}
		}
	}
	if req.Header.Get("Chat-Id") == "" {
		chatID := newChatID()
		req.Header.Set("Chat-Id", chatID)
		if sessionID != "" {
			devecoSessionChatID.Store(sessionID, chatID)
		}
	}
}

func extractAccessToken(auth *coreauth.Auth) string {
	if auth == nil || auth.Metadata == nil {
		return ""
	}
	token, _ := auth.Metadata["access_token"].(string)
	return token
}

// devecoSessionChatID caches stable Chat-Id values by Session-Id, matching
// deveco-code's sessionChatIdMap pattern for conversation continuity.
var devecoSessionChatID sync.Map

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

	// Parse the Huawei model config API response.
	// inner_models and outer_models share the same shape; both are processed so
	// that models served from either bucket are registered (outer_models is
	// currently empty but Huawei may populate it).
	type devecoModelConfig struct {
		ID              int      `json:"id"`
		ModelID         string   `json:"model_id"`
		ThinkingMode    string   `json:"thinking_mode"`
		InputModalities []string `json:"input_modalities,omitempty"`
		ContextWindow   int      `json:"context_window,omitempty"`
		Output          int      `json:"output,omitempty"`
		ToolCallMode    string   `json:"tool_call_mode"`
	}
	type devecoModelGroup struct {
		Protocol     string              `json:"protocol"`
		GroupName    string              `json:"group_name"`
		GroupNameCN  string              `json:"group_name_cn,omitempty"`
		ModelConfigs []devecoModelConfig `json:"model_configs"`
	}
	var apiResp struct {
		Code int `json:"code"`
		Body struct {
			Version     int                `json:"version"`
			InnerModels []devecoModelGroup `json:"inner_models"`
			OuterModels []devecoModelGroup `json:"outer_models"`
		} `json:"body"`
	}
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("deveco fetch models: parse: %w", err)
	}
	if apiResp.Code != 200 {
		return nil, fmt.Errorf("deveco fetch models: API code %d", apiResp.Code)
	}

	models := make([]*registry.ModelInfo, 0)
	groups := append(append([]devecoModelGroup{}, apiResp.Body.InnerModels...), apiResp.Body.OuterModels...)
	seen := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		for _, mc := range group.ModelConfigs {
			if mc.ModelID == "" {
				continue
			}
			// Deduplicate in case a model appears in both buckets.
			if _, dup := seen[mc.ModelID]; dup {
				continue
			}
			seen[mc.ModelID] = struct{}{}
			ctxLen := mc.ContextWindow
			if ctxLen <= 0 {
				ctxLen = 202752
			}
			maxOut := mc.Output
			if maxOut <= 0 {
				maxOut = 131072
			}
			thinkingSupport := parseDevecoThinkingMode(mc.ThinkingMode)
			// If the model is known to support reasoning (GLM-5.1, GLM-5), always enable
			// thinking regardless of API thinking_mode value.
			if thinkingSupport == nil {
				switch mc.ModelID {
				case "GLM-5.1", "GLM-5", "glm-5.1", "glm-5":
					thinkingSupport = &registry.ThinkingSupport{
						Min:            0,
						Max:            8192,
						ZeroAllowed:    true,
						DynamicAllowed: true,
						Levels:         []string{"low", "medium", "high"},
					}
				}
			}
			models = append(models, &registry.ModelInfo{
				ID:                  mc.ModelID,
				Object:              "model",
				OwnedBy:             "deveco",
				Type:                "deveco",
				DisplayName:         mc.ModelID,
				ContextLength:       ctxLen,
				MaxCompletionTokens: maxOut,
				Thinking:            thinkingSupport,
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

// parseDevecoThinkingMode maps Huawei's thinking_mode string to ThinkingSupport.
// Values observed from Huawei API: "on" / "deep" (full reasoning),
// "configurable" (optional), empty/absent (no thinking support).
func parseDevecoThinkingMode(mode string) *registry.ThinkingSupport {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "on", "deep", "deep_think", "reasoning", "configurable":
		return &registry.ThinkingSupport{
			Min:            0,
			Max:            8192,
			ZeroAllowed:    true,
			DynamicAllowed: true,
			Levels:         []string{"low", "medium", "high"},
		}
	default:
		return nil
	}
}
