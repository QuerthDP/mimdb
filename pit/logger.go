package pit

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

// FormatResponse returns a human-readable representation of an HTTP response.
// It safely reads and restores the response body so callers can still read it afterwards.
func FormatResponse(resp *http.Response) string {
	if resp == nil {
		return "<nil response>"
	}

	var bodyBytes []byte
	if resp.Body != nil {
		b, err := io.ReadAll(resp.Body)
		if err == nil {
			bodyBytes = b
		} else {
			bodyBytes = []byte(fmt.Sprintf("<error reading body: %v>", err))
		}
		// restore body so it can be read again by callers
		resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}

	return fmt.Sprintf("Received response:\nHTTP %s\nHeaders: %v\nBody:\n%s", resp.Status, resp.Header, string(bodyBytes))
}

// LogResponse prints the formatted response to stdout. Useful in tests/debugging.
func LogResponse(resp *http.Response) {
	fmt.Print(FormatResponse(resp))
}
