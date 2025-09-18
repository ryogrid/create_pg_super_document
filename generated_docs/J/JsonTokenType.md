# JsonTokenType

## Location
src/include/common/jsonapi.h: 34 - 35

## Overview
JsonTokenType is an enumeration that defines the different types of tokens that can be encountered during JSON lexical analysis, providing type identification for each parsed element.

## Definition
```c
typedef enum JsonTokenType
{
    JSON_TOKEN_INVALID,
    JSON_TOKEN_STRING,
    JSON_TOKEN_NUMBER,
    JSON_TOKEN_OBJECT_START,
    JSON_TOKEN_OBJECT_END,
    JSON_TOKEN_ARRAY_START,
    JSON_TOKEN_ARRAY_END,
    JSON_TOKEN_COMMA,
    JSON_TOKEN_COLON,
    JSON_TOKEN_TRUE,
    JSON_TOKEN_FALSE,
    JSON_TOKEN_NULL,
    JSON_TOKEN_END,
} JsonTokenType;
```

## Detailed Description
JsonTokenType serves as the fundamental token classification system for PostgreSQL's JSON lexer. Each enumeration value represents a distinct category of JSON syntax element that the parser can encounter. The enumeration covers all valid JSON tokens including structural elements (braces, brackets, punctuation), literal values (strings, numbers, booleans, null), and special states (invalid tokens, end of input). This token type system enables the parser to make appropriate semantic decisions and route token processing to the correct handling logic.

## Parameters / Member Variables
- `JSON_TOKEN_INVALID`: Represents an invalid or unrecognized token
- `JSON_TOKEN_STRING`: A JSON string literal enclosed in double quotes
- `JSON_TOKEN_NUMBER`: A JSON numeric value (integer or floating-point)
- `JSON_TOKEN_OBJECT_START`: Opening brace "{" that begins a JSON object
- `JSON_TOKEN_OBJECT_END`: Closing brace "}" that ends a JSON object
- `JSON_TOKEN_ARRAY_START`: Opening bracket "[" that begins a JSON array
- `JSON_TOKEN_ARRAY_END`: Closing bracket "]" that ends a JSON array
- `JSON_TOKEN_COMMA`: Comma "," used to separate array elements or object properties
- `JSON_TOKEN_COLON`: Colon ":" used to separate object keys from values
- `JSON_TOKEN_TRUE`: The JSON boolean literal "true"
- `JSON_TOKEN_FALSE`: The JSON boolean literal "false"
- `JSON_TOKEN_NULL`: The JSON null literal "null"
- `JSON_TOKEN_END`: Special token indicating end of input has been reached

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a base enumeration type)
- Called from (representative examples):
  - JsonLexContext (as member variable token_type)
  - json_lex
  - parse_scalar
  - lex_expect
  - json_manifest_scalar
  - various scalar processing functions (get_scalar, each_scalar, etc.)

## Notes and Other Information
This enumeration is fundamental to PostgreSQL's JSON processing pipeline and is used extensively throughout the JSON/JSONB implementation. The token types directly correspond to the JSON specification (RFC 7159/8259) structural and literal elements. The JSON_TOKEN_INVALID type serves as an error indicator, while JSON_TOKEN_END provides a clean way to signal input exhaustion. The enumeration values are used in conditional logic throughout the parser to dispatch appropriate processing for each token type, and they are also passed to semantic action callbacks to inform higher-level processing about the nature of encountered values.