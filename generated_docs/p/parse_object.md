# parse_object

## Location
src/common/jsonapi.c: 1114 - 1187

## Overview
A recursive descent parsing function that processes JSON object constructs, handling the parsing of curly-brace-enclosed sequences of key-value pairs separated by commas.

## Definition

```c
struct_action ostart = sem->object_start;
```
## Detailed Description
The  function implements JSON object parsing within PostgreSQL's JSON API infrastructure. It processes JSON objects as sequences of object fields (key-value pairs) surrounded by curly braces and separated by commas. The function manages nesting levels, invokes semantic actions for object start/end events, and coordinates with the lexer to consume tokens appropriately.

The parsing follows a standard recursive descent approach:
1. Calls the semantic action for object start if provided
2. Increments the lexical nesting level
3. Expects and consumes the opening brace token
4. Parses object fields in a loop, handling comma separators
5. Expects and consumes the closing brace token  
6. Decrements the lexical nesting level
7. Calls the semantic action for object end if provided

The function includes stack depth checking in non-frontend builds to prevent stack overflow during deeply nested JSON parsing.

## Parameters / Member Variables
- : Pointer to JsonLexContext containing lexical analysis state including current token position and nesting level
- : Pointer to JsonSemAction structure containing semantic action callbacks for object start/end events

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - lex_peek (token lookahead)
  - json_lex (token consumption)
  - parse_object_field (recursive field parsing)
  - lex_expect (expected token validation)
  - report_parse_error (error reporting)
  - JSON token types and parse error constants

- Called from (representative examples):
  - pg_parse_json (main JSON parsing entry point)
  - parse_object_field (recursive object nesting)
  - parse_array_element (objects within arrays)

## Notes and Other Information
- Manages lexical nesting level () to track JSON structure depth
- Supports empty objects (immediate closing brace after opening brace)
- Integrates with semantic action framework allowing customizable object processing
- Stack depth checking prevents stack overflow in deeply nested JSON structures
- Error handling preserves parse context for meaningful error reporting
- Part of PostgreSQL's common JSON parsing infrastructure used across multiple modules