# json_typeof

## Location
src/backend/utils/adt/json.c: 1726 - 1765

## Overview
A SQL function that determines and returns the type of the outermost JSON value as a text string.

## Definition


## Detailed Description
This function implements the SQL function json_typeof(json) -> text, which analyzes a JSON value and returns its type as a human-readable string. The function works by performing minimal parsing - it only needs to examine the first token of the JSON input to determine the overall type. This is efficient because JSON's syntax allows the type to be determined from the initial character(s). The function supports all standard JSON types: objects, arrays, strings, numbers, booleans, and null values. Since the input JSON has already been validated by json_in() or json_recv() when stored in the database, the function can assume well-formed input and only needs to handle the basic token types.

## Parameters / Member Variables
- Takes one argument through PG_FUNCTION_ARGS macro: a JSON text value
- Returns a Datum containing the type name as text

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - [makeJsonLexContext](../m/makeJsonLexContext.md)
  - json_lex
  - [json_errsave_error](json_errsave_error.md)
  - cstring_to_text
  - PG_RETURN_TEXT_P
  - elog
- Data types referenced:
  - [JsonLexContext](../J/JsonLexContext.md)
  - JsonParseErrorType
  - JSON_SUCCESS
  - JSON_TOKEN_OBJECT_START
  - JSON_TOKEN_ARRAY_START
  - JSON_TOKEN_STRING
  - JSON_TOKEN_NUMBER
  - JSON_TOKEN_TRUE
  - JSON_TOKEN_FALSE
  - JSON_TOKEN_NULL
- Called from (representative examples):
  - Available as SQL function json_typeof()

## Notes and Other Information
- Returns one of six possible type strings: "object", "array", "string", "number", "boolean", or "null"
- Uses single-token lexing for optimal performance rather than full parsing
- Assumes input has already been validated, so it doesn't expect malformed JSON tokens
- Boolean values (both true and false) are unified under the "boolean" type string
- The function follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS and Datum return type
- Error handling includes proper PostgreSQL error reporting for unexpected token types
- Part of PostgreSQL's JSON function library for SQL-level JSON manipulation