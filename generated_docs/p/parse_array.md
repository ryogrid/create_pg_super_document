# parse_array

## Location
src/common/jsonapi.c: 1232 - 1308

## Overview
A recursive descent parsing function that processes JSON array constructs, handling the parsing of square-bracket-enclosed sequences of elements separated by commas.

## Definition


## Detailed Description
The  function implements JSON array parsing within PostgreSQL's JSON API infrastructure. It processes JSON arrays as sequences of elements surrounded by square brackets and separated by commas. The function manages nesting levels, invokes semantic actions for array start/end events, and coordinates with array element parsing.

The parsing follows a standard recursive descent approach:
1. Calls the semantic action for array start if provided
2. Increments the lexical nesting level
3. Expects and validates the opening bracket token
4. Parses array elements in a loop, handling comma separators between elements
5. Expects and validates the closing bracket token
6. Decrements the lexical nesting level
7. Calls the semantic action for array end if provided

The function includes stack depth checking in non-frontend builds to prevent stack overflow during deeply nested JSON parsing. It properly handles empty arrays by checking for immediate closing bracket after opening bracket.

## Parameters / Member Variables
- : Pointer to JsonLexContext containing lexical analysis state including current token position and nesting level
- : Pointer to JsonSemAction structure containing semantic action callbacks for array start/end events

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - lex_expect (expected token validation)
  - lex_peek (token lookahead)  
  - parse_array_element (recursive element parsing)
  - json_lex (token consumption)
  - JSON token types and parse error constants

- Called from (representative examples):
  - pg_parse_json (main JSON parsing entry point)
  - parse_object_field (arrays as object values)
  - parse_array_element (recursive array nesting)

## Notes and Other Information
- Manages lexical nesting level () to track JSON structure depth
- Supports empty arrays (immediate closing bracket after opening bracket)
- Integrates with semantic action framework allowing customizable array processing
- Stack depth checking prevents stack overflow in deeply nested JSON structures
- Error handling preserves parse context for meaningful error reporting
- Enables recursive parsing through parse_array_element, supporting arbitrarily nested arrays
- Part of PostgreSQL's common JSON parsing infrastructure used across multiple modules