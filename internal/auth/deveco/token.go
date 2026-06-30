package deveco

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/misc"
)

// DevecoTokenStorage stores DevEco OAuth credentials on disk.
type DevecoTokenStorage struct {
	Type         string         `json:"type"`
	AuthKind     string         `json:"auth_kind,omitempty"`
	AccessToken  string         `json:"access_token"`
	RefreshToken string         `json:"refresh_token"`
	JWTToken     string         `json:"jwt_token,omitempty"`
	TokenType    string         `json:"token_type,omitempty"`
	Expired      string         `json:"expired,omitempty"`
	Email        string         `json:"email,omitempty"`
	UserID       string         `json:"user_id,omitempty"`
	BaseURL      string         `json:"base_url,omitempty"`
	Metadata     map[string]any `json:"-"`
}

// SetMetadata allows external callers to inject metadata before saving.
func (ts *DevecoTokenStorage) SetMetadata(meta map[string]any) {
	ts.Metadata = meta
}

// SaveTokenToFile writes DevEco credentials to a JSON auth file.
func (ts *DevecoTokenStorage) SaveTokenToFile(authFilePath string) error {
	misc.LogSavingCredentials(authFilePath)
	ts.Type = "deveco"
	ts.AuthKind = "oauth"
	if err := os.MkdirAll(filepath.Dir(authFilePath), 0o700); err != nil {
		return fmt.Errorf("deveco token storage: create dir: %w", err)
	}
	file, err := os.Create(authFilePath)
	if err != nil {
		return fmt.Errorf("deveco token storage: create file: %w", err)
	}
	defer file.Close()

	data, err := misc.MergeMetadata(ts, ts.Metadata)
	if err != nil {
		return fmt.Errorf("deveco token storage: merge metadata: %w", err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("deveco token storage: encode: %w", err)
	}
	return nil
}

// CredentialFileName returns the auth filename for DevEco credentials.
func CredentialFileName(email, userID string) string {
	email = sanitizeSegment(email)
	if email != "" {
		return fmt.Sprintf("deveco-%s.json", email)
	}
	userID = sanitizeSegment(userID)
	if userID != "" {
		return fmt.Sprintf("deveco-%s.json", userID)
	}
	return fmt.Sprintf("deveco-%d.json", time.Now().UnixMilli())
}

func sanitizeSegment(s string) string {
	if s == "" {
		return ""
	}
	result := make([]byte, 0, len(s))
	for i := range len(s) {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			result = append(result, c)
		}
	}
	return string(result)
}
