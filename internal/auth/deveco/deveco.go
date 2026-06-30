package deveco

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/browser"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

const (
	DevEcoBaseURL      = "https://cn.devecostudio.huawei.com"
	authURL            = "console/DevEcoIDE/apply"
	tempTokenCheckURL  = "authrouter/auth/api/temptoken/check"
	jwtTokenCheckURL   = "authrouter/auth/api/jwToken/check"
	successRedirectURL = "console/DevEcoCode/loginSuccess"
	failedRedirectURL  = "console/DevEcoCode/loginFailed"
	appID              = "1008"
	DefaultCallbackPort = 10101
	accessTokenTTL     = 30 * time.Minute
	loginTimeout       = 10 * time.Minute
	DevecoProviderID   = "deveco"
)

var devecoRefreshGroup singleflight.Group

// DevecoAuth handles the DevEco OAuth authentication flow.
type DevecoAuth struct {
	httpClient *http.Client
	cfg        *config.Config
}

// NewDevecoAuth creates a new DevEco auth service.
func NewDevecoAuth(cfg *config.Config) *DevecoAuth {
	httpClient := &http.Client{Timeout: 30 * time.Second}
	if cfg != nil {
		httpClient = util.SetProxy(&cfg.SDKConfig, httpClient)
	}
	return &DevecoAuth{httpClient: httpClient, cfg: cfg}
}

// LoginResult holds the result of a DevEco OAuth login.
type LoginResult struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	JWTToken     string `json:"jwt_token"`
	UserID       string `json:"user_id"`
	UserName     string `json:"user_name"`
	ExpiresAt    int64  `json:"expires_at"`
}

// JWTUserInfo holds user info extracted from JWT validation.
type JWTUserInfo struct {
	AccessToken  string
	RefreshToken string
	UserID       string
	UserName     string
	IsRealName   bool
}

// JWTClaims represents parsed claims from a DevEco JWT token.
type JWTClaims struct {
	UserID   string `json:"userId"`
	UserName string `json:"userName"`
	Exp      int64  `json:"exp"`
	Iat      int64  `json:"iat"`
}

// Login performs the full DevEco OAuth login flow:
// 1. Start local callback server
// 2. Open browser for Huawei account login
// 3. Receive tempToken via callback
// 4. Exchange tempToken for JWT
// 5. Exchange JWT for accessToken
func (d *DevecoAuth) Login(ctx context.Context, callbackPort int) (*LoginResult, error) {
	clientSecret, err := generateClientSecret()
	if err != nil {
		return nil, fmt.Errorf("deveco auth: generate secret: %w", err)
	}

	port, err := findAvailablePort(callbackPort)
	if err != nil {
		return nil, fmt.Errorf("deveco auth: find port: %w", err)
	}

	cbServer := NewCallbackServer(port, clientSecret, DevEcoBaseURL, successRedirectURL, failedRedirectURL)
	if err := cbServer.Start(); err != nil {
		return nil, fmt.Errorf("deveco auth: start server: %w", err)
	}
	defer func() {
		if stopErr := cbServer.Stop(); stopErr != nil {
			log.Errorf("deveco auth: stop callback server: %v", stopErr)
		}
	}()

	loginURL := fmt.Sprintf("%s/%s?port=%d&appid=%s&code=%s",
		DevEcoBaseURL, authURL, port, appID, clientSecret)
	log.Infof("Opening browser for DevEco login: %s", loginURL)
	if browser.IsAvailable() {
		if err := browser.OpenURL(loginURL); err != nil {
			log.Warnf("deveco auth: failed to open browser: %v", err)
		}
	} else {
		log.Warn("deveco auth: no browser available, user must open URL manually")
	}

	callback, err := cbServer.WaitForCallback(ctx, loginTimeout)
	if err != nil {
		return nil, fmt.Errorf("deveco auth: wait callback: %w", err)
	}

	jwtToken, err := d.exchangeTempToken(ctx, callback.TempToken)
	if err != nil {
		return nil, fmt.Errorf("deveco auth: exchange temp token: %w", err)
	}

	userInfo, err := d.checkJWT(ctx, jwtToken)
	if err != nil {
		return nil, fmt.Errorf("deveco auth: check JWT: %w", err)
	}

	return &LoginResult{
		AccessToken:  userInfo.AccessToken,
		RefreshToken: userInfo.RefreshToken,
		JWTToken:     jwtToken,
		UserID:       userInfo.UserID,
		UserName:     userInfo.UserName,
		ExpiresAt:    time.Now().Add(accessTokenTTL).Unix(),
	}, nil
}

// RefreshToken refreshes the DevEco access token using the JWT.
func (d *DevecoAuth) RefreshToken(ctx context.Context, jwtToken string) (*LoginResult, error) {
	key := "deveco-refresh:" + jwtToken
	result, err, _ := devecoRefreshGroup.Do(key, func() (interface{}, error) {
		return d.refreshTokenCall(ctx, jwtToken)
	})
	if err != nil {
		return nil, err
	}
	return result.(*LoginResult), nil
}

func (d *DevecoAuth) refreshTokenCall(ctx context.Context, jwtToken string) (*LoginResult, error) {
	reqURL := fmt.Sprintf("%s/%s", DevEcoBaseURL, jwtTokenCheckURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("deveco refresh: create req: %w", err)
	}
	req.Header.Set("refresh", "true")
	req.Header.Set("jwtToken", jwtToken)
	req.Header.Set("User-Agent", "CLIProxyAPI/1.0")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("deveco refresh: http: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("deveco refresh: read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		// Truncate error body to avoid leaking credentials in logs
		errBody := string(body)
		if len(errBody) > 256 {
			errBody = errBody[:256] + "..."
		}
		return nil, fmt.Errorf("deveco refresh: HTTP %d: %s", resp.StatusCode, errBody)
	}

	var result struct {
		Status   bool `json:"status"`
		UserInfo *struct {
			AccessToken  string `json:"accessToken"`
			RefreshToken string `json:"refreshToken"`
		} `json:"userInfo"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("deveco refresh: parse: %w", err)
	}
	if !result.Status || result.UserInfo == nil || result.UserInfo.AccessToken == "" {
		return nil, fmt.Errorf("deveco refresh: invalid response")
	}

	claims, err := parseJWT(jwtToken)
	if err != nil {
		return nil, fmt.Errorf("deveco refresh: parse JWT: %w", err)
	}
	return &LoginResult{
		AccessToken:  result.UserInfo.AccessToken,
		RefreshToken: result.UserInfo.RefreshToken,
		JWTToken:     jwtToken,
		UserID:       claims.UserID,
		UserName:     claims.UserName,
		ExpiresAt:    time.Now().Add(accessTokenTTL).Unix(),
	}, nil
}

func (d *DevecoAuth) exchangeTempToken(ctx context.Context, tempToken string) (string, error) {
	reqURL := fmt.Sprintf("%s/%s?tempToken=%s&site=CN&version=1.0.0&appid=%s",
		DevEcoBaseURL, tempTokenCheckURL, url.QueryEscape(tempToken), appID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("exchange temp token: %w", err)
	}
	req.Header.Set("User-Agent", "CLIProxyAPI/1.0")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("exchange temp token: http: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("exchange temp token: read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		errBody := string(body)
		if len(errBody) > 256 {
			errBody = errBody[:256] + "..."
		}
		return "", fmt.Errorf("exchange temp token: HTTP %d: %s", resp.StatusCode, errBody)
	}

	jwtToken := strings.TrimSpace(string(body))
	if parts := strings.Split(jwtToken, "."); len(parts) != 3 {
		return "", fmt.Errorf("exchange temp token: invalid JWT (%d parts)", len(parts))
	}
	return jwtToken, nil
}

func (d *DevecoAuth) checkJWT(ctx context.Context, jwtToken string) (*JWTUserInfo, error) {
	reqURL := fmt.Sprintf("%s/%s", DevEcoBaseURL, jwtTokenCheckURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("check JWT: %w", err)
	}
	req.Header.Set("refresh", "false")
	req.Header.Set("jwtToken", jwtToken)
	req.Header.Set("User-Agent", "CLIProxyAPI/1.0")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("check JWT: http: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("check JWT: read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		errBody := string(body)
		if len(errBody) > 256 {
			errBody = errBody[:256] + "..."
		}
		return nil, fmt.Errorf("check JWT: HTTP %d: %s", resp.StatusCode, errBody)
	}

	var r struct {
		Status   bool `json:"status"`
		UserInfo *struct {
			AccessToken  string      `json:"accessToken"`
			RefreshToken string      `json:"refreshToken"`
			RealName     interface{} `json:"realName"`
		} `json:"userInfo"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("check JWT: parse: %w", err)
	}
	if !r.Status || r.UserInfo == nil || r.UserInfo.AccessToken == "" {
		return nil, fmt.Errorf("check JWT: invalid response")
	}

	claims, err := parseJWT(jwtToken)
	if err != nil {
		return nil, fmt.Errorf("check JWT: parse JWT: %w", err)
	}
	isRealName := false
	switch v := r.UserInfo.RealName.(type) {
	case bool:
		isRealName = v
	case string:
		isRealName = v == "true"
	}
	return &JWTUserInfo{
		AccessToken:  r.UserInfo.AccessToken,
		RefreshToken: r.UserInfo.RefreshToken,
		UserID:       claims.UserID,
		UserName:     claims.UserName,
		IsRealName:   isRealName,
	}, nil
}

// parseJWT decodes the JWT payload without signature verification.
// Returns an error if the token structure or payload is invalid.
func parseJWT(token string) (*JWTClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("deveco parse JWT: expected 3 parts, got %d", len(parts))
	}
	// base64url → base64
	payload := parts[1]
	replacer := strings.NewReplacer("-", "+", "_", "/")
	padded := replacer.Replace(payload)
	switch len(padded) % 4 {
	case 2:
		padded += "=="
	case 3:
		padded += "="
	}
	decoded, err := base64.StdEncoding.DecodeString(padded)
	if err != nil {
		return nil, fmt.Errorf("deveco parse JWT: base64 decode: %w", err)
	}
	var claims JWTClaims
	if err := json.Unmarshal(decoded, &claims); err != nil {
		return nil, fmt.Errorf("deveco parse JWT: json decode: %w", err)
	}
	return &claims, nil
}

func generateClientSecret() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func findAvailablePort(preferred int) (int, error) {
	ports := []int{preferred, 34567, 34568, 34569, 34570}
	for _, port := range ports {
		if portAvailable(port) {
			return port, nil
		}
	}
	return 0, fmt.Errorf("no available ports in %v", ports)
}

func portAvailable(port int) bool {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return false
	}
	ln.Close()
	return true
}
