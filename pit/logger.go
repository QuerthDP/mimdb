package pit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// FormatRequest returns a human-readable representation of a request body as JSON.
// It accepts any struct that can be marshaled to JSON.
func FormatRequest(method, endpoint string, body interface{}) string {
	var bodyJSON string
	if body != nil {
		jsonBytes, err := json.MarshalIndent(body, "", "  ")
		if err != nil {
			bodyJSON = fmt.Sprintf("<error marshaling: %v>", err)
		} else {
			bodyJSON = string(jsonBytes)
		}
	} else {
		bodyJSON = "<no body>"
	}

	return fmt.Sprintf("Sending request:\n%s %s\nBody:\n%s", method, endpoint, bodyJSON)
}

// FormatJSON returns a formatted JSON representation of any struct.
// Useful for logging request payloads.
func FormatJSON(v interface{}) string {
	if v == nil {
		return "<nil>"
	}
	jsonBytes, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("<error marshaling: %v>", err)
	}
	return string(jsonBytes)
}

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

// LogRequest prints the formatted request to stdout. Useful in tests/debugging.
func LogRequest(method, endpoint string, body interface{}) {
	fmt.Print(FormatRequest(method, endpoint, body))
}

// LogResponse prints the formatted response to stdout. Useful in tests/debugging.
func LogResponse(resp *http.Response) {
	fmt.Print(FormatResponse(resp))
}
