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

func TestBindingRequiresCurrentIdentityToAuthorizeMutation(t *testing.T) {
	binding := Binding{
		ArrType:                Radarr,
		ArrInstanceFingerprint: "v1:test",
		ArrFileID:              42,
		LibraryPath:            "/library/movie.mkv",
		Confidence:             BindingConfidenceExactPath,
	}
	if !binding.AuthorizesMutation() {
		t.Fatal("current binding did not authorize mutation")
	}

	binding.ArrInstanceFingerprint = ""
	if binding.AuthorizesMutation() {
		t.Fatal("legacy binding without an instance fingerprint authorized mutation")
	}
	binding.ArrInstanceFingerprint = "v1:test"
	binding.LibraryPath = ""
	if binding.AuthorizesMutation() {
		t.Fatal("binding without a library path authorized mutation")
	}
}
