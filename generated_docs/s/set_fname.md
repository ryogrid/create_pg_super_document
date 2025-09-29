# set_fname

## Location
[src/common/jsonapi.c:452-457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L452-L457)

## Overview
Sets the field name for the current lexical level in a JSON lexical context structure.

## Definition

```c
static inline void
set_fname(JsonLexContext *lex, char *fname)
```
## Detailed Description
The `set_fname` function is a static inline helper function that assigns a field name to the current lexical level within a JSON parsing context. It updates the `fnames` array at the position corresponding to the current `lex_level` in the JSON lexical context's parse stack. This function is part of the JSON parsing infrastructure in PostgreSQL's common library.

## Parameters / Member Variables
- `lex`: Pointer to the JsonLexContext structure containing the parsing state
- `fname`: Character pointer to the field name string to be set

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md) (structure type)
- Called from (representative examples):
  - [pg_parse_json_incremental](../p/pg_parse_json_incremental.md)

## Notes and Other Information
This is a static inline function, meaning it's only accessible within the jsonapi.c file and will likely be inlined by the compiler for performance. The function provides a clean abstraction for managing field names during JSON parsing, helping to track the hierarchical structure of nested JSON objects.

## Simplified Source

```c
static inline void
set_fname(JsonLexContext *lex, char *fname)
{
    // Set field name for current parsing level
    lex->pstack->fnames[lex->lex_level] = fname;
}
```