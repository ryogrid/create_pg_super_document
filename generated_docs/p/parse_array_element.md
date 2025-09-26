# parse_array_element

## Location
[src/common/jsonapi.c:1188-1231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L1188-L1231)

## Overview
A recursive descent parsing function that processes individual JSON array elements, dispatching to appropriate parsers based on element type (object, array, or scalar).

## Definition

```c
struct_action astart = sem->array_start;
```
## Detailed Description
The  function handles the parsing of individual elements within JSON arrays. It serves as a dispatcher that examines the current token to determine the element type and delegates to the appropriate specialized parser. The function supports all valid JSON value types as array elements: objects, arrays (enabling nested arrays), and scalar values (strings, numbers, booleans, null).

The parsing flow:
1. Determines if the current element is null by examining the token
2. Calls the semantic action for array element start if provided, passing null status
3. Dispatches to specialized parsers based on token type:
   - Objects: calls parse_object
   - Arrays: calls parse_array (enabling nesting)
   - Scalars: calls parse_scalar  
4. Calls the semantic action for array element end if provided

This design enables the JSON parser to handle arbitrarily nested data structures while maintaining clean separation of concerns between different value type parsers.

## Parameters / Member Variables
- : Pointer to JsonLexContext containing lexical analysis state and current token information
- : Pointer to JsonSemAction structure containing semantic action callbacks for array element start/end events

## Dependencies
- Functions called/Symbols referenced:
  - [lex_peek](../l/lex_peek.md) (token lookahead)
  - [parse_object](parse_object.md) (nested object parsing)
  - [parse_array](parse_array.md) (nested array parsing) 
  - [parse_scalar](parse_scalar.md) (scalar value parsing)
  - JSON token type constants (JSON_TOKEN_NULL, JSON_TOKEN_OBJECT_START, JSON_TOKEN_ARRAY_START)

- Called from (representative examples):
  - [parse_array](parse_array.md) (array element processing)
  - [json_count_array_elements](../j/json_count_array_elements.md) (array element counting)

## Notes and Other Information
- Supports all JSON value types as array elements, enabling full JSON specification compliance
- Handles null detection specifically to provide null status to semantic actions
- Enables recursive parsing of nested structures (arrays within arrays, objects within arrays)
- Integrates with semantic action framework for customizable element processing
- Part of the recursive descent parser architecture that cleanly separates concerns by value type
- Error propagation preserves parse context for meaningful error reporting