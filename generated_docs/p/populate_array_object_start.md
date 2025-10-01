# populate_array_object_start

## Location
[src/backend/utils/adt/jsonfuncs.c:2643-2665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2643-L2665)

## Overview
JSON parse handler that processes the start of JSON objects during array population, validating dimensional constraints and structure.

## Definition
```c
static JsonParseErrorType populate_array_object_start(void *_state)
```

## Detailed Description
This function serves as a JSON parsing event handler specifically for object start events during array population from JSON. It validates that JSON objects appear at appropriate dimensional levels according to the expected array structure. When dimensions have not yet been determined (ndims <= 0), it assigns the current nesting level as the number of dimensions. If dimensions are already established, it ensures that objects only appear at the deepest level, reporting errors for objects at inappropriate nesting levels. This enforces the constraint that PostgreSQL arrays cannot contain mixed data types at the same level.

## Parameters / Member Variables
- `_state`: Void pointer to PopulateArrayState containing the parsing state, lexer information, and population context

## Dependencies
- Functions called/Symbols referenced:
  - [populate_array_assign_ndims](populate_array_assign_ndims.md) (dimension assignment)
  - [populate_array_report_expected_array](populate_array_report_expected_array.md) (error reporting)
  - SOFT_ERROR_OCCURRED (error checking macro)
  - [PopulateArrayState](../P/PopulateArrayState.md), JsonParseErrorType (data types)
  - JSON_SEM_ACTION_FAILED, JSON_SUCCESS (return constants)
- Called from (representative examples):
  - [populate_array_json](populate_array_json.md)
  - JsObjectFree

## Notes and Other Information
- Returns JSON_SUCCESS on successful validation, JSON_SEM_ACTION_FAILED on error
- Part of the JSON semantic action handler system for array population
- Enforces PostgreSQL array structure constraints during JSON parsing
- Uses soft error handling to allow graceful error recovery in appropriate contexts
- Critical for maintaining type consistency in multi-dimensional array construction
- Works in conjunction with other populate_array_* handlers to build valid PostgreSQL arrays

## Simplified Source

```c
static JsonParseErrorType
populate_array_object_start(void *_state)
{
    PopulateArrayState *state = (PopulateArrayState *) _state;
    int ndim = state->lex->lex_level;

    // Assign dimensions if not yet determined
    if (state->ctx->ndims <= 0) {
        if (!populate_array_assign_ndims(state->ctx, ndim))
            return JSON_SEM_ACTION_FAILED;
    }
    // Validate object appears at correct nesting level
    else if (ndim < state->ctx->ndims) {
        populate_array_report_expected_array(state->ctx, ndim);
        return JSON_SEM_ACTION_FAILED;
    }

    return JSON_SUCCESS;
}
```