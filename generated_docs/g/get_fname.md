# get_fname

## Location
[src/common/jsonapi.c:458-463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L458-L463)

## Overview
Retrieves the field name for the current lexical level from a JSON lexical context structure.

## Definition
```c
static inline char *get_fname(JsonLexContext *lex)
```

## Detailed Description
The `get_fname` function is a static inline helper function that returns the field name associated with the current lexical level in a JSON parsing context. It accesses the `fnames` array at the position corresponding to the current `lex_level` in the JSON lexical contexts parse stack. This function provides read access to field names during JSON parsing operations.

## Parameters / Member Variables
- `lex`: Pointer to the JsonLexContext structure containing the parsing state

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md) (structure type)
- Called from (representative examples):
  - [pg_parse_json_incremental](../p/pg_parse_json_incremental.md) (multiple locations)

## Notes and Other Information
This is a static inline function, meaning it is only accessible within the jsonapi.c file and will likely be inlined by the compiler for performance. The function serves as a companion to `set_fname`, providing read access to the field names stored during JSON parsing. It returns a char pointer to the field name string, which may be NULL if no field name has been set for the current level.