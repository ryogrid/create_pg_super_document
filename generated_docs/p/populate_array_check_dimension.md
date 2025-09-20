# populate_array_check_dimension

## Location
[src/backend/utils/adt/jsonfuncs.c:2588-2615](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2588-L2615)

## Overview
Validates and manages dimensions of multi-dimensional arrays during JSON array population, ensuring consistency across sub-arrays.

## Definition

```c
static bool
populate_array_check_dimension(PopulateArrayContext *ctx, int ndim)
```
## Detailed Description
This function is responsible for checking and maintaining dimensional consistency when populating multi-dimensional arrays from JSON data. It tracks the size of each dimension and ensures that all sub-arrays at the same level have matching dimensions. If a dimension hasn't been determined yet (indicated by -1), it assigns the current size. If the dimension has already been set, it validates that the current sub-array matches the expected size. The function also manages dimension counters by resetting the current dimension counter and incrementing the parent dimension counter for nested arrays.

## Parameters / Member Variables
- : PopulateArrayContext pointer containing array dimension tracking information including sizes and dims arrays
- : The dimension level being checked (0-based index into the dimension arrays)

## Dependencies
- Functions called/Symbols referenced:
  - [PopulateArrayContext](../P/PopulateArrayContext.md) (struct type)
  - ereturn (error return macro)
- Called from (representative examples):
  - [populate_array_array_end](populate_array_array_end.md)
  - [populate_array_dim_jsonb](populate_array_dim_jsonb.md)
  - JsObjectFree

## Notes and Other Information
- Returns false if dimensional inconsistency is detected, true otherwise
- Uses PostgreSQL's error reporting system with ERRCODE_INVALID_TEXT_REPRESENTATION
- Critical for maintaining array structure integrity during JSON-to-array conversion
- Handles both dimension assignment for unknown dimensions and validation for known dimensions
- Part of the JSON array population subsystem in PostgreSQL's JSON handling utilities