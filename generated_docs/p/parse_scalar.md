# parse_scalar

## Location
[src/common/jsonapi.c:1008-1051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L1008-L1051)

## Overview
A recursive descent parsing function that processes JSON scalar values (string, number, true, false, null) and invokes the appropriate semantic action callback.

## Definition


## Detailed Description
parse_scalar handles the parsing of JSON scalar values within the recursive descent parser framework. It validates that the current token represents a valid scalar type, extracts the token value (either as a de-escaped string or raw lexeme), consumes the token from the input stream, and invokes the scalar semantic action callback if one is provided. For string tokens, it extracts the processed string value, while for other scalar types it captures the raw lexeme text. The function ensures proper token validation before processing and handles cases where no semantic action is needed.

## Parameters / Member Variables
- : JsonLexContext pointer containing the current parsing state and token information
- : JsonSemAction pointer containing the scalar callback function and semantic state

## Dependencies
- Functions called/Symbols referenced:
  - [lex_peek](../l/lex_peek.md) (for token lookahead and validation)
  - report_parse_error (for invalid token error reporting)
  - json_lex (for token consumption)
  - [pstrdup](pstrdup.md) (for string duplication)
  - [palloc](palloc.md)/memcpy (for raw lexeme extraction)
- Called from (representative examples):
  - [pg_parse_json](pg_parse_json.md) (src/common/jsonapi.c:569) - for bare scalar JSON values
  - [parse_object_field](parse_object_field.md) (src/common/jsonapi.c:1098) - for object field values
  - parse_array_element (src/common/jsonapi.c:1215) - for array element values

## Notes and Other Information
The function is declared as static inline for performance optimization within the parser. It distinguishes between string tokens (which provide processed string data) and other scalar tokens (which require raw lexeme extraction). The semantic callback receives both the extracted value and the original token type for proper type handling. If no semantic function is provided, the function simply consumes the token without further processing. Token validation ensures only valid scalar types are accepted, returning parse errors for invalid tokens.