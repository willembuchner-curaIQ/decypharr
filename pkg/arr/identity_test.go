package arr

import "testing"

func TestArrInstanceFingerprintCanonicalizesHost(t *testing.T) {
	first := (&Arr{Type: Sonarr, Host: "HTTP://Example.COM:80/sonarr/", Token: "first"}).InstanceFingerprint()
	second := (&Arr{Type: Sonarr, Host: "http://example.com/sonarr", Token: "second"}).InstanceFingerprint()
	if first == "" || first != second {
		t.Fatalf("equivalent Arr hosts produced fingerprints %q and %q", first, second)
	}

	differentPath := (&Arr{Type: Sonarr, Host: "http://example.com/other"}).InstanceFingerprint()
	differentType := (&Arr{Type: Radarr, Host: "http://example.com/sonarr"}).InstanceFingerprint()
	if first == differentPath || first == differentType {
		t.Fatal("different Arr instances produced the same fingerprint")
	}
}
