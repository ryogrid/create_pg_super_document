# do_scalar

## Location
src/test/modules/test_json_parser/test_json_parser_incremental.c: 281 - 299

## Overview
The `do_scalar` function is a JSON parsing callback function that handles scalar values (strings, numbers, booleans, null) in PostgreSQL's incremental JSON parser test module.

## Definition
```c
static JsonParseErrorType
do_scalar(void *state, char *token, JsonTokenType tokentype)
```

## Detailed Description
This function serves as a semantic action callback that processes scalar values encountered during JSON parsing. It differentiates between string tokens and other scalar types (numbers, booleans, null). For string tokens, it performs proper JSON escaping using a helper function and outputs the escaped string. For non-string scalars (numbers, booleans, null), it outputs the token directly without modification. The function uses a state buffer to manage string escaping operations, ensuring that JSON strings are properly formatted with escape sequences in the test output.

## Parameters / Member Variables
- `state`: A void pointer to the parser state, which gets cast to `DoState *` internally. This contains the parsing context including lexer information, element tracking flags, and output buffer.
- `token`: A null-terminated string containing the scalar value to be processed.
- `tokentype`: An enumerated value of type `JsonTokenType` that indicates the specific type of the scalar token (string, number, boolean, null, etc.).

## Dependencies
- Functions called/Symbols referenced:
  - `[DoState](../D/DoState.md)` (struct type for parser state)
  - `[JsonTokenType](../J/JsonTokenType.md)` (enum type for token classification)
  - `JSON_TOKEN_STRING` (enum constant for string tokens)
  - `JSON_SUCCESS` (return value constant)
  - `resetStringInfo` (function to reset StringInfo buffer)
  - `[escape_json](../e/escape_json.md)` (helper function for JSON string escaping)
  - `printf` (standard C library function for output)
- Called from (representative examples):
  - Used as a callback function in JSON parser semantic actions (no direct references found in current analysis)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the test_json_parser_incremental.c file
- Part of the test infrastructure for PostgreSQL's incremental JSON parsing capabilities
- The function always returns `JSON_SUCCESS`, indicating successful processing
- Handles special processing for JSON_TOKEN_STRING by applying proper escape sequences
- Non-string scalars (numbers, booleans, null) are output directly without escaping
- Uses the parser state's StringInfo buffer for efficient string processing during escaping
- The `escape_json` helper function is noted as being "copied from backend code", indicating it follows PostgreSQL's standard JSON escaping rules
- Critical for maintaining valid JSON output format in the test framework
- Works alongside array and object processing functions to handle complete JSON documents