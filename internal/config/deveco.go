package config

import (
	"strings"

	log "github.com/sirupsen/logrus"
)

// DevecoConfig defines DevEco Code provider credentials with optional routing and behavior overrides.
type DevecoConfig struct {
	// Enabled toggles this DevEco credential entry.
	Enabled bool `yaml:"enabled" json:"enabled"`

	// Prefix optionally namespaces model aliases for this credential (e.g., "huawei/glm-5").
	Prefix string `yaml:"prefix,omitempty" json:"prefix,omitempty"`

	// CallbackPort is the localhost port used for OAuth callback server.
	// Default: 10101.
	CallbackPort int `yaml:"callback-port,omitempty" json:"callback-port,omitempty"`

	// DisableCooling disables auth/model cooldown scheduling for this credential when true.
	DisableCooling bool `yaml:"disable-cooling,omitempty" json:"disable-cooling,omitempty"`
}

// SanitizeDevecoKeys normalizes all DevEco config entries.
func (cfg *Config) SanitizeDevecoKeys() {
	for i := range cfg.Deveco {
		entry := &cfg.Deveco[i]
		entry.Prefix = strings.TrimSpace(entry.Prefix)
		if entry.CallbackPort <= 0 {
			entry.CallbackPort = 10101
		}
		if entry.Prefix != "" && !strings.HasSuffix(entry.Prefix, "/") {
			entry.Prefix += "/"
		}
		if i > 0 && entry.Prefix == "" {
			log.Warnf("deveco[%d]: multiple credentials should use unique prefixes for correct routing", i)
		}
	}
}

// HasEnabledDeveco returns true if there is at least one enabled DevEco config entry.
func (cfg *Config) HasEnabledDeveco() bool {
	for i := range cfg.Deveco {
		if cfg.Deveco[i].Enabled {
			return true
		}
	}
	return false
}
