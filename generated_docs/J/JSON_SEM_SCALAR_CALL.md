# JSON_SEM_SCALAR_CALL

## Location
src/common/jsonapi.c: 74 - 83

## Overview
An enumeration value in the JsonParserSem enum that represents a semantic action for invoking scalar value callbacks during JSON parsing.

## Definition
```c
enum JsonParserSem
{
    JSON_SEM_OSTART = 64,
    JSON_SEM_OEND,
    JSON_SEM_ASTART,
    JSON_SEM_AEND,
    JSON_SEM_OFIELD_INIT,
    JSON_SEM_OFIELD_START,
    JSON_SEM_OFIELD_END,
    JSON_SEM_AELEM_START,
    JSON_SEM_AELEM_END,
    JSON_SEM_SCALAR_INIT,
    JSON_SEM_SCALAR_CALL,
};
```

## Detailed Description
JSON_SEM_SCALAR_CALL is a semantic action marker used in PostgreSQL's incremental JSON parser to trigger callback functions for scalar values. When the parser encounters a complete scalar value (string, number, boolean, or null), this semantic action is invoked to call the appropriate user-provided callback function with the parsed scalar value.

This enum value works in conjunction with JSON_SEM_SCALAR_INIT to handle scalar value processing. While JSON_SEM_SCALAR_INIT prepares for scalar parsing, JSON_SEM_SCALAR_CALL actually invokes the callback with the completed scalar value. The parser uses this two-phase approach to properly handle scalar values that may be processed incrementally across multiple parser invocations.

## Parameters / Member Variables
- This is an enum constant with no parameters or member variables

## Dependencies
- Functions called/Symbols referenced:
  - None (enum constant)
- Called from (representative examples):
  - IS_NT (macro usage in parsing logic)
  - pg_parse_json_incremental (main parsing function for semantic action processing)

## Notes and Other Information
- Part of the JsonParserSem enum starting at value 64
- Used in the parser's semantic action processing to trigger scalar value callbacks
- Works with JSON_SEM_SCALAR_INIT as part of a two-phase scalar processing mechanism
- Critical for providing parsed scalar values to user callback functions in the incremental JSON parsing API
- The semantic action system allows the parser to trigger appropriate callbacks at specific points during JSON document processing