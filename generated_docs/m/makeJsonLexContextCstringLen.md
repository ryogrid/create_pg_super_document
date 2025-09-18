# makeJsonLexContextCstringLen

## Location
src/common/jsonapi.c: 326 - 360

## Overview
A function that initializes or creates a JsonLexContext object for parsing JSON from a C string with specified length, providing flexible memory management options.

## Definition
JsonLexContext *makeJsonLexContextCstringLen(JsonLexContext *lex, const char *json, size_t len, int encoding, bool need_escapes)

## Detailed Description
This function provides flexible initialization of JsonLexContext objects with support for both stack-allocated and heap-allocated contexts. If a valid lex pointer is provided, it initializes the existing structure, which is efficient for stack-allocated contexts. If NULL is passed, it allocates a new structure using palloc0. The function sets up all necessary fields for JSON parsing, including input string, length, encoding, and optional escape processing. When need_escapes is true, it creates a StringInfo buffer for storing unescaped string values, which is computationally expensive but necessary for certain parsing scenarios.

## Parameters / Member Variables
- lex: Existing JsonLexContext to initialize, or NULL to allocate a new one
- json: Pointer to the JSON string to parse
- len: Length of the JSON string
- encoding: Character encoding of the input string  
- need_escapes: Whether to enable escape sequence processing for string values

## Dependencies
- Functions called/Symbols referenced:
  - JsonLexContext (structure type)
  - palloc0 (memory allocation)
  - JSONLEX_FREE_STRUCT (flag constant)
  - makeStringInfo (string buffer creation)
  - JSONLEX_FREE_STRVAL (flag constant)
- Called from (representative examples):
  - json_recv
  - jsonb_from_cstring
  - makeJsonLexContext  
  - populate_array_json
  - get_json_object_as_hash
  - json_parse_manifest
  - test_gb18030_json
  - main (in test programs)

## Notes and Other Information
This function is the primary entry point for setting up JSON parsing contexts in PostgreSQL. The flexible memory management allows for optimal performance in different scenarios - stack allocation for temporary parsing and heap allocation for longer-lived contexts. The need_escapes parameter provides an important optimization, as escape processing is expensive and should only be enabled when the unescaped string values are actually needed. The function properly handles memory management flags to ensure correct cleanup behavior.