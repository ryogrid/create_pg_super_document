# report_parse_error

## Location
[src/common/jsonapi.c:2056-2099](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L2056-L2099)

## Overview
Reports parsing errors during JSON parsing by determining the appropriate error type based on the current parsing context and lexical state.

## Definition

```c
enum values.
	 */
	Assert(false);
```
## Detailed Description
The  function is a static helper function in PostgreSQL's JSON parsing API that analyzes the current parsing context and lexical state to determine the most appropriate error type to report when a JSON parsing error occurs. It serves as a centralized error classification mechanism within the JSON parser.

The function operates by first checking if the input has ended prematurely (when  is NULL or the token type is ). If so, it returns  to indicate that more input was expected.

For other error conditions, the function uses a switch statement to map the current parsing context ( enum) to the most appropriate error code ( enum). This context-sensitive error reporting provides more meaningful error messages to users by indicating what was expected at each stage of JSON parsing.

The function assumes that  and  have been properly set to identify the current token that caused the parsing error.

## Parameters / Member Variables
- : The current JSON parsing context indicating what type of JSON construct was being parsed when the error occurred
- : Pointer to the JSON lexical context containing information about the current token and parsing state

## Dependencies
- Functions called/Symbols referenced:
  - JsonParseContext (enum values: JSON_PARSE_END, JSON_PARSE_VALUE, JSON_PARSE_STRING, etc.)
  - JsonParseErrorType (enum values: JSON_EXPECTED_MORE, JSON_EXPECTED_END, JSON_EXPECTED_JSON, etc.)
  - JSON_TOKEN_END (from JsonTokenType enum)
  - JSON_SUCCESS (return value for unreachable code path)

- Called from (representative examples):
  - [lex_expect](../l/lex_expect.md) (src/common/jsonapi.c:255)
  - [pg_parse_json_incremental](../p/pg_parse_json_incremental.md) (src/common/jsonapi.c:991)
  - [parse_scalar](../p/parse_scalar.md) (src/common/jsonapi.c:1019)
  - [parse_object_field](../p/parse_object_field.md) (src/common/jsonapi.c:1068)
  - [parse_object](../p/parse_object.md) (src/common/jsonapi.c:1166)

## Notes and Other Information
- The function is declared static, meaning it's only accessible within the jsonapi.c file
- The switch statement intentionally omits a default case so that the compiler will warn about unhandled enum values if new parsing contexts are added
- The final return statement () is included only to silence compiler warnings and should never be reached due to the Assert(false) preceding it
- This function is part of PostgreSQL's table-driven JSON parser implementation
- The error types returned by this function correspond to specific expected JSON constructs (arrays, objects, values, etc.) providing context-aware error reporting
- File location: src/common/jsonapi.c:2056-2099