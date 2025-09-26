# set_fnull

## Location
src/common/jsonapi.c: 464 - 469

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
  - JsonLexContext (structure type)
- Called from (representative examples):
  - pg_parse_json_incremental (multiple locations)

## Notes and Other Information
This is a static inline function, meaning it is only accessible within the jsonapi.c file and will likely be inlined by the compiler for performance. The function works in conjunction with `get_fnull` to manage null state tracking during JSON parsing. This functionality is important for properly handling JSON null values and distinguishing them from other value types during parsing operations.