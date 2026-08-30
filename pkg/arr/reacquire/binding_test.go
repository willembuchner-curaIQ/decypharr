package reacquire

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/arr"
)

func TestBindingRequiresCurrentIdentityToAuthorizeMutation(t *testing.T) {
	binding := Binding{
		ArrType:                arr.Radarr,
		ArrInstanceFingerprint: "v1:test",
		ArrFileID:              42,
		LibraryPath:            "/library/movie.mkv",
		Confidence:             ConfidenceExactPath,
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
