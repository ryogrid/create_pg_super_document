# dec_lex_level

## Location
[src/common/jsonapi.c:419-424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L419-L424)

## Overview
Decrements the lexical nesting level counter in a JSON parsing context to track the depth of nested JSON structures.

## Definition
```c
static inline void dec_lex_level(JsonLexContext *lex)
```

## Detailed Description
The `dec_lex_level` function is a simple inline utility function that decrements the `lex_level` field of a `JsonLexContext` structure by 1. This function is used during JSON parsing to track the nesting depth when exiting nested JSON structures like objects or arrays. It serves as the counterpart to incrementing the lexical level when entering nested structures.

## Parameters / Member Variables
- `lex`: Pointer to a `JsonLexContext` structure containing the parsing state, including the current nesting level

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md) (struct type)
- Called from (representative examples):
  - [pg_parse_json_incremental](../p/pg_parse_json_incremental.md) (at src/common/jsonapi.c:747)
  - [pg_parse_json_incremental](../p/pg_parse_json_incremental.md) (at src/common/jsonapi.c:776)

## Notes and Other Information
- This is a static inline function for performance optimization
- Used internally within the JSON parsing API to maintain proper nesting level tracking
- The function is called when the parser exits nested JSON structures to properly decrement the depth counter