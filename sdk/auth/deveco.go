package auth

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/auth/deveco"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/browser"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// devecoRefreshLead is the duration before token expiry when refresh should occur.
var devecoRefreshLead = 5 * time.Minute

// DevecoAuthenticator implements the OAuth browser login for Huawei DevEco Code.
type DevecoAuthenticator struct {
	CallbackPort int
}

// NewDevecoAuthenticator constructs a new DevEco authenticator.
func NewDevecoAuthenticator() *DevecoAuthenticator {
	return &DevecoAuthenticator{CallbackPort: deveco.DefaultCallbackPort}
}

// Provider returns the provider key for deveco.
func (a *DevecoAuthenticator) Provider() string {
	return deveco.DevecoProviderID
}

// RefreshLead returns the duration before token expiry when refresh should occur.
func (a *DevecoAuthenticator) RefreshLead() *time.Duration {
	return &devecoRefreshLead
}

// Login initiates the DevEco browser OAuth login flow.
func (a *DevecoAuthenticator) Login(ctx context.Context, cfg *config.Config, opts *LoginOptions) (*coreauth.Auth, error) {
	if cfg == nil {
		return nil, fmt.Errorf("cliproxy auth: configuration is required")
	}
	if opts == nil {
		opts = &LoginOptions{}
	}

	callbackPort := a.CallbackPort
	if opts.CallbackPort > 0 {
		callbackPort = opts.CallbackPort
	}
	if len(cfg.Deveco) > 0 && cfg.Deveco[0].CallbackPort > 0 {
		callbackPort = cfg.Deveco[0].CallbackPort
	}

	authSvc := deveco.NewDevecoAuth(cfg)

	fmt.Println("Starting DevEco Code authentication...")
	fmt.Println("A browser window will open for you to log in with your Huawei account.")

	if !opts.NoBrowser && !browser.IsAvailable() {
		log.Warn("No browser available; a URL will be displayed for manual login")
	}

	result, err := authSvc.Login(ctx, callbackPort)
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "cancelled") {
			return nil, fmt.Errorf("deveco: login cancelled by user")
		}
		if strings.Contains(errMsg, "unsupported region") {
			return nil, fmt.Errorf("deveco: only China site accounts are supported")
		}
		return nil, fmt.Errorf("deveco: login failed: %w", err)
	}

	fmt.Printf("DevEco Code authentication successful! Logged in as %s\n", result.UserName)

	fileName := deveco.CredentialFileName(result.UserName, result.UserID)
	now := time.Now()
	expiresAt := now.Add(30 * time.Minute)
	if result.ExpiresAt > 0 {
		expiresAt = time.Unix(result.ExpiresAt, 0)
	}

	tokenStorage := &deveco.DevecoTokenStorage{
		AccessToken:  result.AccessToken,
		RefreshToken: result.RefreshToken,
		JWTToken:     result.JWTToken,
		TokenType:    "Bearer",
		Expired:      expiresAt.UTC().Format(time.RFC3339),
		Email:        result.UserName,
		UserID:       result.UserID,
		BaseURL:      deveco.DevEcoBaseURL,
	}

	return &coreauth.Auth{
		ID:       fileName,
		Provider: a.Provider(),
		FileName: fileName,
		Label:    fmt.Sprintf("DevEco - %s", result.UserName),
		Storage:  tokenStorage,
		Metadata: map[string]any{
			"access_token":  result.AccessToken,
			"refresh_token": result.RefreshToken,
			"jwt_token":     result.JWTToken,
			"expires_at":    float64(expiresAt.Unix()),
			"user_id":       result.UserID,
			"user_name":     result.UserName,
			"type":          "deveco",
		},
	}, nil
}
