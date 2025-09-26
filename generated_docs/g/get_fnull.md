# get_fnull

## Location
[src/common/jsonapi.c:470-482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L470-L482)

## Overview
Retrieves the null flag for the current lexical level from a JSON lexical context structure.

## Definition
```c
static inline bool get_fnull(JsonLexContext *lex)
```

## Detailed Description
The `get_fnull` function is a static inline helper function that returns the boolean null flag associated with the current lexical level in a JSON parsing context. It accesses the `fnull` array at the position corresponding to the current `lex_level` in the JSON lexical contexts parse stack. This function provides read access to null state information during JSON parsing operations.

## Parameters / Member Variables
- `lex`: Pointer to the JsonLexContext structure containing the parsing state

## Dependencies
- Functions called/Symbols referenced:
  - JsonLexContext (structure type)
- Called from (representative examples):
  - pg_parse_json_incremental (multiple locations)

## Notes and Other Information
This is a static inline function, meaning it is only accessible within the jsonapi.c file and will likely be inlined by the compiler for performance. The function serves as a companion to `set_fnull`, providing read access to the null flags stored during JSON parsing. It returns a boolean value indicating whether the field at the current lexical level should be treated as null, which is essential for proper JSON null value handling.