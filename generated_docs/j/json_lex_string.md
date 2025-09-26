# json_lex_string

## Location
[src/common/jsonapi.c:1672-1678](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L1672-L1678)

## Overview
A static inline function that lexically analyzes and decodes JSON string tokens from the input stream.

## Definition
```c
static inline JsonParseErrorType
json_lex_string(JsonLexContext *lex)
```

## Detailed Description
json_lex_string is a core lexical analysis function in PostgreSQL's JSON parser that handles the parsing and decoding of JSON string tokens. When the parser has already determined that the next token in the input stream is a string (begins with a quote character), this function performs the detailed lexical analysis.

The function handles several important aspects of JSON string processing:
- Decodes escape sequences (\\n, \\t, \\", \\uXXXX, etc.)
- Processes Unicode surrogate pairs properly
- Manages incremental parsing by handling incomplete strings across chunk boundaries
- Updates lexical context state including token terminators
- Fills the strval buffer if string content extraction is requested

The function supports both complete and incremental parsing modes. In incremental mode, if a string spans multiple input chunks, it properly handles the partial token state and can return JSON_INCOMPLETE to request more input.

## Parameters / Member Variables
- `lex`: Pointer to JsonLexContext structure containing:
  - Input buffer and position information
  - String value buffer (strval) for decoded output
  - Token position tracking fields
  - Incremental parsing state information

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md) (parameter type)
  - Various JSON error codes and parsing state management
- Called from (representative examples):
  - [json_lex](json_lex.md) (main lexical analysis function)
  - TD_ENTRY (used in parser table for string token processing)

## Notes and Other Information
- Declared as static inline for performance in the lexical analysis hot path
- Implements proper Unicode handling including UTF-16 surrogate pair processing
- Carefully manages error conditions to ensure token_terminator is properly advanced
- Uses convenience macros for error handling that support incremental parsing
- Critical for string literal processing in both regular JSON parsing and incremental streaming scenarios
- Handles all JSON string escape sequences according to RFC 7159 specification