package request

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	json "github.com/bytedance/sonic"
)

// growHintCap bounds how much Content-Length is trusted to preallocate. A body
// larger than this still decodes; the buffer just grows on its own.
const growHintCap = 64 << 20

// DecodeJSON reads a response body in full, then unmarshals it into out.
//
// Decoding straight off resp.Body with a streaming decoder looks cheaper, but
// sonic buffers the whole document internally anyway, and grows that scratch
// buffer by a fraction of its length on every read off the wire. The copies are
// quadratic in body size: a 9MB Sonarr library response allocates over 4GB.
// One Content-Length sized read keeps it linear.
//
// An empty body returns io.EOF and leaves out untouched, the same as a
// streaming decoder, so callers keep whatever handling they already had for it.
func DecodeJSON(resp *http.Response, out any) error {
	if resp == nil || resp.Body == nil || out == nil {
		return nil
	}
	var buf bytes.Buffer
	if n := resp.ContentLength; n > 0 {
		// +1 so the final empty read that reports EOF does not force a grow.
		buf.Grow(int(min(n, growHintCap)) + 1)
	}
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	if buf.Len() == 0 {
		return io.EOF
	}
	return json.Unmarshal(buf.Bytes(), out)
}
