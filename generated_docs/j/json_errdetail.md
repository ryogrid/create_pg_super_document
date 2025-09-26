# json_errdetail

## Location
[src/common/jsonapi.c:2100-2110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L2100-L2110)

## Overview
A function that constructs detailed, human-readable error messages for JSON parsing errors based on error type and lexical context.

## Definition
```c
char *
json_errdetail(JsonParseErrorType error, JsonLexContext *lex)
```

## Detailed Description
json_errdetail is a comprehensive error message formatting function in PostgreSQL's JSON parser that converts JSON parsing error codes into user-friendly, localized error messages. The function takes a JsonParseErrorType enum value and a JsonLexContext pointer to generate contextual error descriptions.

The function handles a wide variety of JSON parsing errors including:
- Lexical errors (invalid tokens, escaping issues)
- Structural errors (missing colons, brackets, etc.)
- Unicode processing errors (invalid escape sequences, surrogate pairs)
- Parser state errors (unexpected end of input, nesting too deep)
- Semantic action failures

Key features include:
- Uses the existing errormsg StringInfo buffer in the JsonLexContext, or creates one if needed
- Provides localized error messages using the gettext framework (_() macros)
- Includes contextual information like the problematic token when applicable
- Handles both backend and frontend compilation contexts
- Returns a pointer to the error string that should not be freed by the caller

## Parameters / Member Variables
- `error`: JsonParseErrorType enum value indicating the specific type of parsing error
- `lex`: Pointer to JsonLexContext containing parsing state and token information used for error context

## Dependencies
- Functions called/Symbols referenced:
  - JsonParseErrorType (parameter type)
  - JsonLexContext (parameter type)
  - resetStringInfo (StringInfo management)
  - makeStringInfo (StringInfo creation)
  - appendStringInfo (error message formatting)
  - GetDatabaseEncodingName (backend-only encoding information)
- Called from (representative examples):
  - json_errsave_error (backend JSON functions error handling)
  - json_parse_manifest_incremental_chunk (manifest parsing)
  - json_parse_manifest (complete manifest parsing)
  - Various test functions for JSON parser validation

## Notes and Other Information
- Returns a pointer that should not be freed - memory is managed by the JsonLexContext
- Uses a comprehensive switch statement covering all JsonParseErrorType enum values
- Compiler will warn about unhandled enum values due to absence of default case
- Includes conditional compilation for backend vs frontend contexts
- Uses gettext localization for internationalization support
- Provides detailed token context in error messages using json_token_error macro
- Handles special cases like Unicode encoding limitations in different contexts