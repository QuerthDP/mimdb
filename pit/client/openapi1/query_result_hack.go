package openapi1

import (
	"encoding/json"
)

// QueryResult is a wrapper type that accepts both array and single object responses.
// Some servers (v1.0.1) incorrectly return a single object instead of an array.
// This hack treats a single object as a one-element array.
type QueryResult []QueryResultInner

// UnmarshalJSON implements custom unmarshaling that accepts both:
// - Array format (correct): [{"rowCount": 0, "columns": [...]}]
// - Single object format (broken): {"rowCount": 0, "columns": [...]}
func (qr *QueryResult) UnmarshalJSON(data []byte) error {
	// First, try to unmarshal as array (correct format per interface spec)
	var arr []QueryResultInner
	if err := json.Unmarshal(data, &arr); err == nil {
		*qr = arr
		return nil
	}

	// If that fails, try as single object (broken server format)
	// Treat it as a one-element array
	var single QueryResultInner
	if err := json.Unmarshal(data, &single); err == nil {
		*qr = []QueryResultInner{single}
		return nil
	}

	// Both failed - return the array unmarshal error for better diagnostics
	return json.Unmarshal(data, &arr)
}

// MarshalJSON always marshals as an array (correct format)
func (qr QueryResult) MarshalJSON() ([]byte, error) {
	return json.Marshal([]QueryResultInner(qr))
}
