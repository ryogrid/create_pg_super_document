# json_lex

## Location
[src/common/jsonapi.c:1309-1671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L1309-L1671)

## Overview
The core lexical analyzer function that tokenizes JSON input, handling both streaming and incremental parsing while identifying and classifying JSON tokens.

## Definition

```c
JsonParseErrorType
json_lex(JsonLexContext *lex)
```
## Detailed Description
The  function is the central component of PostgreSQL's JSON lexical analysis system. It processes JSON input character by character to identify and classify tokens such as strings, numbers, literals (true/false/null), and structural punctuation (braces, brackets, commas, colons). The function supports both traditional parsing and incremental parsing for streaming JSON data.

Key functionality includes:
1. **Incremental parsing support**: Handles partial tokens across input chunks, maintaining state between calls
2. **Whitespace management**: Skips whitespace and tracks line numbers for error reporting
3. **Token classification**: Identifies all valid JSON token types through character analysis
4. **Partial token reconstruction**: Accumulates incomplete tokens across multiple input chunks
5. **Error detection**: Validates token structure and reports specific error locations

The function uses a state machine approach, examining the current character to determine token type and delegating to specialized lexers for complex tokens (strings, numbers). For incremental parsing, it maintains partial token buffers and handles token completion across input boundaries.

## Parameters / Member Variables
- : Pointer to JsonLexContext containing lexical state including input position, token boundaries, line numbers, and incremental parsing state

## Dependencies
- Functions called/Symbols referenced:
  - [resetStringInfo](../r/resetStringInfo.md) (partial token buffer management)
  - appendStringInfoCharMacro (partial token building)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md) (bulk partial token addition)
  - [json_lex_string](json_lex_string.md) (string token parsing)
  - [json_lex_number](json_lex_number.md) (number token parsing)
  - JSON_ALPHANUMERIC_CHAR (character classification macro)
  - JSON token type constants
  - memcmp (literal token comparison)

- Called from (representative examples):
  - [pg_parse_json](../p/pg_parse_json.md) (main parsing entry point)
  - [pg_parse_json_incremental](../p/pg_parse_json_incremental.md) (incremental parsing)
  - [parse_scalar](../p/parse_scalar.md), parse_object_field, parse_object, parse_array (recursive parser components)
  - [lex_expect](../l/lex_expect.md) (token validation)

## Notes and Other Information
- Supports both complete and incremental JSON parsing modes
- Maintains line number tracking for detailed error reporting
- Handles UTF-8 encoding considerations through input_encoding context
- Implements partial token buffering for streaming scenarios where tokens span input chunks
- Uses recursive self-calls to process completed partial tokens
- Integrates with specialized token parsers for complex token types
- Central to PostgreSQL's JSON processing infrastructure across multiple modules
- Error handling preserves exact token positions for meaningful error messages
- Supports all JSON specification token types including structural punctuation and literals