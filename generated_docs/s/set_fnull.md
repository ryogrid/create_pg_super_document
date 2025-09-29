# set_fnull

## Location
[src/common/jsonapi.c:464-469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L464-L469)

## Overview
Sets the null flag for the current lexical level in a JSON lexical context structure.

## Definition
```c
static inline void set_fnull(JsonLexContext *lex, bool fnull)
```

## Detailed Description
The `set_fnull` function is a static inline helper function that sets a boolean flag indicating whether the field at the current lexical level in a JSON parsing context should be treated as null. It updates the `fnull` array at the position corresponding to the current `lex_level` in the JSON lexical contexts parse stack. This function is used to track null field states during JSON parsing operations.

## Parameters / Member Variables
- `lex`: Pointer to the JsonLexContext structure containing the parsing state
- `fnull`: Boolean value indicating whether the field should be considered null (true) or not (false)

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md) (structure type)
- Called from (representative examples):
  - [pg_parse_json_incremental](../p/pg_parse_json_incremental.md) (multiple locations)

## Notes and Other Information
This is a static inline function, meaning it is only accessible within the jsonapi.c file and will likely be inlined by the compiler for performance. The function works in conjunction with `get_fnull` to manage null state tracking during JSON parsing. This functionality is important for properly handling JSON null values and distinguishing them from other value types during parsing operations.

## Simplified Source

```c
static inline void
set_fnull(JsonLexContext *lex, bool fnull)
{
    // Set null flag for current parsing level
    lex->pstack->fnull[lex->lex_level] = fnull;
}
```