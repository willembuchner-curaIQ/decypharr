package request

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	json "github.com/bytedance/sonic"
)

// errAny marks a case that wants any error, not a specific one.
var errAny = errors.New("any error")

type record struct {
	ID   int    `json:"id"`
	Path string `json:"path"`
	Size int64  `json:"size"`
}

func TestDecodeJSON(t *testing.T) {
	for _, test := range []struct {
		name    string
		body    string
		chunked bool
		want    int
		wantErr error
	}{
		{name: "sized", body: `[{"id":1},{"id":2}]`, want: 2},
		{name: "chunked", body: `[{"id":1},{"id":2}]`, chunked: true, want: 2},
		// An empty body reports io.EOF, the same as a streaming decoder, so
		// call sites that treated it as an error still do.
		{name: "empty body reports EOF", body: "", wantErr: io.EOF},
		{name: "malformed", body: `[{"id":`, wantErr: errAny},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if !test.chunked {
					w.Header().Set("Content-Length", fmt.Sprint(len(test.body)))
				}
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()

			resp, err := http.Get(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer DrainAndCloseResponse(resp)

			var out []record
			err = DecodeJSON(resp, &out)
			if test.wantErr != nil {
				if err == nil {
					t.Fatal("want an error")
				}
				if test.wantErr != errAny && !errors.Is(err, test.wantErr) {
					t.Fatalf("err = %v, want %v", err, test.wantErr)
				}
				if len(out) != 0 {
					t.Fatalf("out = %#v, want untouched", out)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(out) != test.want {
				t.Fatalf("out = %#v", out)
			}
		})
	}
}

func TestDecodeJSONIgnoresNilInputs(t *testing.T) {
	if err := DecodeJSON(nil, &[]record{}); err != nil {
		t.Fatal(err)
	}
	resp := &http.Response{Body: io.NopCloser(strings.NewReader(`[{"id":1}]`))}
	if err := DecodeJSON(resp, nil); err != nil {
		t.Fatal(err)
	}
}

// body serves a payload in bounded reads, the way an HTTP body arrives off the
// wire. Buffer growth is driven by the read size, not the payload size.
type body struct {
	data  []byte
	off   int
	chunk int
}

func (b *body) Read(p []byte) (int, error) {
	if b.off >= len(b.data) {
		return 0, io.EOF
	}
	n := min(min(len(p), b.chunk), len(b.data)-b.off)
	copy(p, b.data[b.off:b.off+n])
	b.off += n
	return n, nil
}

func (b *body) Close() error { return nil }

func payload(tb testing.TB, records int) []byte {
	rows := make([]record, records)
	for i := range rows {
		rows[i] = record{
			ID:   i + 1,
			Path: fmt.Sprintf("/mnt/decypharr/tv/A Show (2019)/Season %02d/A Show (2019) - S%02dE%02d - Title [Bluray-1080p][x264]-GROUP.mkv", i%20+1, i%20+1, i%24+1),
			Size: 3_500_000_000,
		}
	}
	data, err := json.Marshal(rows)
	if err != nil {
		tb.Fatal(err)
	}
	return data
}

// BenchmarkDecodeJSON guards the reason DecodeJSON exists. Streaming a large
// response through sonic's decoder grows its scratch buffer by a fraction of
// its length on every read, so allocation is quadratic in body size: at 40k
// records the streaming path allocates over 4GB against ~100MB here. Compare
// allocated bytes across sizes — a regression shows up as superlinear growth.
func BenchmarkDecodeJSON(b *testing.B) {
	for _, records := range []int{500, 5000, 20000} {
		data := payload(b, records)
		b.Run(fmt.Sprintf("records=%d/bytes=%d", records, len(data)), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				resp := &http.Response{
					ContentLength: int64(len(data)),
					Body:          &body{data: data, chunk: 16 << 10},
				}
				var out []record
				if err := DecodeJSON(resp, &out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
