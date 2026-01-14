// Package parser provides SQL parsing for a subset of SQL
// that maps to the ISBD DBMS OpenAPI interface.
//
// The parser is generated from grammar.peg using pigeon.
// To regenerate, run:
//
//	go generate ./...
//
// Or directly:
//
//	pigeon -o parser_gen.go grammar.peg
package parser

//go:generate pigeon -o parser_gen.go grammar.peg

import (
	openapi "github.com/smogork/ISBD-MIMUW/pit/client"
)

// ParseSQL parses a SQL SELECT query string and returns an openapi.SelectQuery.
// This is the main entry point for the parser.
//
// Example:
//
//	query, err := ParseSQL("SELECT (c1 + c2) / c3 FROM t1 WHERE age > 18")
//	if err != nil {
//	    // handle error
//	}
//	// query is *openapi.SelectQuery ready for API submission
func ParseSQL(input string) (*openapi.SelectQuery, error) {
	result, err := Parse("", []byte(input))
	if err != nil {
		return nil, err
	}
	return result.(*openapi.SelectQuery), nil
}
