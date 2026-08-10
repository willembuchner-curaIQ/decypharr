package config

import (
	"strconv"
	"strings"
)

// Hearsay configures participation in the Hearsay network: sharing
// debrid cache and usenet completeness observations that fall out of
// work decypharr already does. On by default; observations flow only
// from real work (repair probes, add outcomes, import checks) — never
// from probing. See https://github.com/sirrobot01/hearsay.
type Hearsay struct {
	Disabled   bool     `json:"disabled,omitempty"`    // turn off all participation
	NoPublish  bool     `json:"no_publish,omitempty"`  // consume hints without contributing
	Port       int      `json:"port,omitempty"`        // swarm listen port; 0 = random
	GossipPort int      `json:"gossip_port,omitempty"` // discovery listen port; 0 = random
	Interval   string   `json:"interval,omitempty"`    // publish and fetch cycle (default 30m)
	Follow     []string `json:"follow,omitempty"`      // pinned publisher keys ("ed25519:<hex>")
}

func (h Hearsay) IsZero() bool {
	return !h.Disabled && !h.NoPublish && h.Port == 0 && h.GossipPort == 0 &&
		h.Interval == "" && len(h.Follow) == 0
}

func (c *Config) applyHearsayEnvVars() {
	if v := getEnv("HEARSAY__DISABLED"); v != "" {
		c.Hearsay.Disabled = parseBool(v)
	}
	if v := getEnv("HEARSAY__NO_PUBLISH"); v != "" {
		c.Hearsay.NoPublish = parseBool(v)
	}
	if v := getEnv("HEARSAY__PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			c.Hearsay.Port = p
		}
	}
	if v := getEnv("HEARSAY__GOSSIP_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			c.Hearsay.GossipPort = p
		}
	}
	if v := getEnv("HEARSAY__INTERVAL"); v != "" {
		c.Hearsay.Interval = v
	}
	if v := getEnv("HEARSAY__FOLLOW"); v != "" {
		c.Hearsay.Follow = nil
		for _, key := range strings.Split(v, ",") {
			if key = strings.TrimSpace(key); key != "" {
				c.Hearsay.Follow = append(c.Hearsay.Follow, key)
			}
		}
	}
}
