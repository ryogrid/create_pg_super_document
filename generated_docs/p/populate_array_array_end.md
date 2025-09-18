# populate_array_array_end

## Location
src/backend/utils/adt/jsonfuncs.c: 2666 - 2689

## Overview
JSON parse handler that processes the end of JSON arrays during array population, managing dimensions and validating array structure consistency.

## Definition
```c
static JsonParseErrorType populate_array_array_end(void *_state)
```

## Detailed Description
This function serves as a JSON parsing event handler for array end events during PostgreSQL array population from JSON. It manages the completion of JSON array parsing by handling dimension assignment and validation. When array dimensions have not yet been determined, it assigns dimensions based on the current nesting level plus one. For arrays with established dimensions, it calls populate_array_check_dimension to validate that the completed sub-array matches the expected dimensional structure. This ensures consistency across all sub-arrays at the same dimensional level and maintains the integrity of the multi-dimensional array being constructed.

## Parameters / Member Variables
- `_state`: Void pointer to PopulateArrayState containing the parsing state, lexer context, and population context information

## Dependencies
- Functions called/Symbols referenced:
  - [populate_array_assign_ndims](populate_array_assign_ndims.md) (dimension assignment)
  - [populate_array_check_dimension](populate_array_check_dimension.md) (dimension validation)
  - [PopulateArrayState](../P/PopulateArrayState.md), PopulateArrayContext (data types)
  - JsonParseErrorType (return type)
  - JSON_SEM_ACTION_FAILED, JSON_SUCCESS (return constants)
- Called from (representative examples):
  - [populate_array_json](populate_array_json.md)
  - JsObjectFree

## Notes and Other Information
- Returns JSON_SUCCESS on successful processing, JSON_SEM_ACTION_FAILED on validation errors
- Part of the JSON semantic action handler system for array population
- Critical for maintaining dimensional consistency in multi-dimensional arrays
- Works with populate_array_check_dimension to enforce structural constraints
- Handles both initial dimension discovery and ongoing dimension validation
- Essential component in the JSON-to-PostgreSQL array conversion pipeline