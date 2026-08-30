package arr

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"net/url"
	"path"
	"path/filepath"
	"strings"
)

const arrInstanceFingerprintVersion = "v1"

// InstanceFingerprint identifies the configured Arr endpoint without storing credentials.
func (a *Arr) InstanceFingerprint() string {
	if a == nil || a.Type == "" {
		return ""
	}
	host, err := canonicalArrHost(a.Host)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256([]byte(arrInstanceFingerprintVersion + "\x00" + string(a.Type) + "\x00" + host))
	return arrInstanceFingerprintVersion + ":" + hex.EncodeToString(digest[:])
}

func canonicalArrHost(rawHost string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawHost))
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("Arr host must be an absolute URL")
	}

	parsed.Scheme = strings.ToLower(parsed.Scheme)
	hostname := strings.ToLower(parsed.Hostname())
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || hostname == "" {
		return "", errors.New("Arr host must use HTTP or HTTPS")
	}
	port := parsed.Port()
	if (parsed.Scheme == "http" && port == "80") || (parsed.Scheme == "https" && port == "443") {
		port = ""
	}
	if port != "" {
		parsed.Host = net.JoinHostPort(hostname, port)
	} else if strings.Contains(hostname, ":") {
		parsed.Host = "[" + hostname + "]"
	} else {
		parsed.Host = hostname
	}

	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	parsed.RawPath = ""
	parsed.Path = path.Clean(parsed.Path)
	if parsed.Path == "." || parsed.Path == "/" {
		parsed.Path = ""
	}
	return parsed.String(), nil
}

func validateMutationInstance(instance *Arr, bindings []Binding) error {
	fingerprint := instance.InstanceFingerprint()
	if fingerprint == "" {
		return errors.New("Arr instance identity is unavailable")
	}
	for _, binding := range bindings {
		if binding.ArrInstanceFingerprint != fingerprint {
			return errors.New("Arr instance changed since binding was indexed")
		}
	}
	return nil
}

func sameLibraryPath(left, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	return left != "" && right != "" && filepath.Clean(left) == filepath.Clean(right)
}
