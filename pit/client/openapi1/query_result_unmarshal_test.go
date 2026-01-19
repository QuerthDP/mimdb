package openapi1

import (
	"encoding/json"
	"testing"
)

func TestQueryResultInnerColumnsInner_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantErr   bool
		wantInt64 bool
		wantStr   bool
		skip      string // reason to skip (empty = don't skip)
	}{
		// ===== Cases that SHOULD work =====
		{
			name:      "int64 array with values",
			input:     `[1, 2, 3]`,
			wantErr:   false,
			wantInt64: true,
			wantStr:   false,
		},
		{
			name:      "string array with values",
			input:     `["a", "b", "c"]`,
			wantErr:   false,
			wantInt64: false,
			wantStr:   true,
		},
		{
			name:      "single int64 value",
			input:     `[42]`,
			wantErr:   false,
			wantInt64: true,
			wantStr:   false,
		},
		{
			name:      "single string value",
			input:     `["hello"]`,
			wantErr:   false,
			wantInt64: false,
			wantStr:   true,
		},
		{
			name:      "int64 with zero",
			input:     `[0]`,
			wantErr:   false,
			wantInt64: true,
			wantStr:   false,
		},
		{
			name:      "string with empty string",
			input:     `[""]`,
			wantErr:   false,
			wantInt64: false,
			wantStr:   true,
		},
		{
			name:      "negative int64",
			input:     `[-1, -100, -999]`,
			wantErr:   false,
			wantInt64: true,
			wantStr:   false,
		},

		// ===== Edge cases with empty arrays/objects =====
		{
			name:      "empty array should parse as string (default for empty)",
			input:     `[]`,
			wantErr:   false,
			wantInt64: false,
			wantStr:   true, // defaults to string for empty arrays
		},
		{
			name:      "empty object should be treated as empty string column",
			input:     `{}`,
			wantErr:   false,
			wantInt64: false,
			wantStr:   true, // defaults to string for empty objects
		},
		{
			name:    "null is not supported",
			input:   `null`,
			wantErr: true, // nulls are not supported in the interface
		},

		// ===== Cases that SHOULD fail =====
		{
			name:    "nested empty array",
			input:   `[[]]`,
			wantErr: true,
		},
		{
			name:    "object with properties",
			input:   `{"columns": []}`,
			wantErr: true,
		},
		{
			name:    "mixed array int and string",
			input:   `[1, "two", 3]`,
			wantErr: true,
		},
		{
			name:    "boolean array",
			input:   `[true, false, true]`,
			wantErr: true,
		},
		{
			name:    "object array",
			input:   `[{"a": 1}, {"b": 2}]`,
			wantErr: true,
		},
		{
			name:      "array with null element (Go accepts null as 0)",
			input:     `[1, null, 3]`,
			wantErr:   false,
			wantInt64: true,
			wantStr:   false,
		},
		{
			name:    "array with empty object element",
			input:   `[1, {}, 3]`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			var col QueryResultInnerColumnsInner
			err := json.Unmarshal([]byte(tt.input), &col)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil; col=%+v", col)
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			hasInt64 := col.ArrayOfInt64 != nil
			hasStr := col.ArrayOfString != nil

			if hasInt64 != tt.wantInt64 {
				t.Errorf("ArrayOfInt64: got %v, want %v", hasInt64, tt.wantInt64)
			}
			if hasStr != tt.wantStr {
				t.Errorf("ArrayOfString: got %v, want %v", hasStr, tt.wantStr)
			}
		})
	}
}

func TestQueryResultInner_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		skip    string
	}{
		// ===== Valid cases =====
		{
			name:    "valid result with int64 column",
			input:   `{"rowCount": 3, "columns": [[1, 2, 3]]}`,
			wantErr: false,
		},
		{
			name:    "valid result with string column",
			input:   `{"rowCount": 2, "columns": [["a", "b"]]}`,
			wantErr: false,
		},
		{
			name:    "valid result with multiple columns",
			input:   `{"rowCount": 2, "columns": [[1, 2], ["a", "b"]]}`,
			wantErr: false,
		},
		{
			name:    "empty columns array",
			input:   `{"rowCount": 0, "columns": []}`,
			wantErr: false,
		},
		{
			name:    "missing columns field",
			input:   `{"rowCount": 0}`,
			wantErr: false,
		},
		{
			name:    "empty object (no fields)",
			input:   `{}`,
			wantErr: false,
		},
		{
			name:    "only rowCount",
			input:   `{"rowCount": 5}`,
			wantErr: false,
		},
		{
			name:    "rowCount zero with empty columns",
			input:   `{"rowCount": 0, "columns": []}`,
			wantErr: false,
		},

		// ===== Edge cases with empty arrays/objects =====
		{
			name:    "column as empty object should work",
			input:   `{"rowCount": 0, "columns": [{}]}`,
			wantErr: false,
		},
		{
			name:    "column as null is not supported",
			input:   `{"rowCount": 0, "columns": [null]}`,
			wantErr: true, // nulls are not supported
		},
		{
			name:    "column as empty array should work",
			input:   `{"rowCount": 0, "columns": [[]]}`,
			wantErr: false,
		},
		{
			name:    "multiple empty object columns",
			input:   `{"rowCount": 0, "columns": [{}, {}]}`,
			wantErr: false,
		},
		{
			name:    "mixed valid and empty object columns",
			input:   `{"rowCount": 0, "columns": [[1, 2], {}]}`,
			wantErr: false,
		},
		{
			name:    "columns is null",
			input:   `{"rowCount": 0, "columns": null}`,
			wantErr: false,
		},

		// ===== Cases that SHOULD fail =====
		{
			name:    "boolean column (not in model)",
			input:   `{"rowCount": 2, "columns": [[true, false]]}`,
			wantErr: true,
		},
		{
			name:    "mixed types in column",
			input:   `{"rowCount": 2, "columns": [[1, "two"]]}`,
			wantErr: true,
		},
		{
			name:    "nested object in column",
			input:   `{"rowCount": 1, "columns": [[{"a": 1}]]}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			var result QueryResultInner
			err := json.Unmarshal([]byte(tt.input), &result)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil; result=%+v", result)
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestQueryResult_UnmarshalJSON(t *testing.T) {
	// QueryResult accepts both array and single object (hack for broken servers)
	tests := []struct {
		name    string
		input   string
		wantErr bool
		skip    string
	}{
		// ===== Valid cases =====
		{
			name:    "empty array (no results)",
			input:   `[]`,
			wantErr: false,
		},
		{
			name:    "single result batch with int64",
			input:   `[{"rowCount": 2, "columns": [[1, 2]]}]`,
			wantErr: false,
		},
		{
			name:    "single result batch with string",
			input:   `[{"rowCount": 2, "columns": [["a", "b"]]}]`,
			wantErr: false,
		},
		{
			name:    "single result batch with mixed columns",
			input:   `[{"rowCount": 2, "columns": [[1, 2], ["a", "b"]]}]`,
			wantErr: false,
		},
		{
			name:    "multiple result batches",
			input:   `[{"rowCount": 1, "columns": [[1]]}, {"rowCount": 1, "columns": [[2]]}]`,
			wantErr: false,
		},
		{
			name:    "result with empty columns array",
			input:   `[{"rowCount": 0, "columns": []}]`,
			wantErr: false,
		},
		{
			name:    "result batch is empty object",
			input:   `[{}]`,
			wantErr: false,
		},
		{
			name:    "multiple empty object batches",
			input:   `[{}, {}]`,
			wantErr: false,
		},
		{
			name:    "result with null columns",
			input:   `[{"rowCount": 0, "columns": null}]`,
			wantErr: false,
		},

		// ===== Edge cases with empty arrays/objects =====
		{
			name:    "column as empty object should work",
			input:   `[{"rowCount": 0, "columns": [{}]}]`,
			wantErr: false,
		},
		{
			name:    "column as empty array should work",
			input:   `[{"rowCount": 0, "columns": [[]]}]`,
			wantErr: false,
		},
		{
			name:    "multiple columns with empty objects",
			input:   `[{"rowCount": 0, "columns": [{}, {}, {}]}]`,
			wantErr: false,
		},
		{
			name:    "multiple columns with empty arrays",
			input:   `[{"rowCount": 0, "columns": [[], [], []]}]`,
			wantErr: false,
		},
		{
			name:    "column as null is not supported",
			input:   `[{"rowCount": 0, "columns": [null]}]`,
			wantErr: true, // nulls are not supported
		},
		{
			name:    "valid column followed by empty object",
			input:   `[{"rowCount": 2, "columns": [[1, 2], {}]}]`,
			wantErr: false,
		},
		{
			name:    "valid column followed by empty array",
			input:   `[{"rowCount": 0, "columns": [[1, 2], []]}]`,
			wantErr: false,
		},

		// ===== Broken server format (single object instead of array) =====
		{
			name:    "4 empty cols - single object (broken server v1.0.1)",
			input:   `{"rowCount":0,"columns":[[],[],[],[]]}`,
			wantErr: false,
		},
		{
			name:    "single object with data (broken server format)",
			input:   `{"rowCount": 2, "columns": [[1, 2], ["a", "b"]]}`,
			wantErr: false,
		},

		// ===== Cases that SHOULD fail =====
		{
			name:    "boolean columns",
			input:   `[{"rowCount": 2, "columns": [[true, false]]}]`,
			wantErr: true,
		},
		{
			name:    "mixed types in single column",
			input:   `[{"rowCount": 2, "columns": [[1, "two"]]}]`,
			wantErr: true,
		},
		{
			name:    "object column",
			input:   `[{"rowCount": 1, "columns": [[{"key": "value"}]]}]`,
			wantErr: true,
		},
		{
			name:    "nested arrays",
			input:   `[{"rowCount": 1, "columns": [[[1, 2]]]}]`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}

			var result QueryResult
			err := json.Unmarshal([]byte(tt.input), &result)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil; result=%+v", result)
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
