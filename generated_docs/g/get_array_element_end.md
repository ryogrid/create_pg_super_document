# get_array_element_end

## Location
[src/backend/utils/adt/jsonfuncs.c:1399-1442](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1399-L1442)

## Overview
A static callback function used during JSON parsing to handle the end of individual array elements, responsible for capturing element values and managing path state when target array elements are completed.

## Definition
```c
static JsonParseErrorType get_array_element_end(void *state, bool isnull)
```

## Detailed Description
The `get_array_element_end` function is a JSON parser callback that processes the completion of individual array elements during path-based extraction operations. It works as the counterpart to `get_array_element_start` and is responsible for finalizing element value capture when the current element matches the extraction path.

The function implements the same path matching logic as its start counterpart but focuses on completion tasks:
- Validates that the current element matches the target path specification
- Resets path validity flags for intermediate path levels
- Captures the complete element text when reaching the end of the extraction path
- Handles null value processing with proper normalization
- Manages result state cleanup after successful capture

The function uses the same boundary calculation approach as other JSON capture functions, determining text length from the start position (set by `get_array_element_start`) to the current token terminator position.

## Parameters / Member Variables
- `state`: A void pointer that is cast to `GetState *`, containing the parsing state including lexical analyzer, path information, array indices, and result tracking
- `isnull`: A boolean indicating whether the current array element is null, used for proper null handling in normalized results

## Dependencies
- Functions called/Symbols referenced:
  - [GetState](../G/GetState.md) (struct type for casting state parameter)
  - JsonParseErrorType (return type)
  - cstring_to_text_with_len (converts C string to PostgreSQL text type)
  - JSON_SUCCESS (success return constant)
- Called from (representative examples):
  - [get_worker](get_worker.md) (JSON extraction worker function)
  - JsObjectFree (JSON object processing)

## Notes and Other Information
- This function mirrors the path matching logic from `get_array_element_start` ("`same tests as in get_array_element_start`" comment)
- Follows the same processing pattern as object field handling ("`same logic as for objects`" comment)
- Properly handles null array elements by setting tresult to NULL when normalization is enabled
- Text capture only occurs when result_start is non-NULL, indicating that capture was initiated
- [Path](../P/Path.md) state management includes resetting pathok flags for intermediate levels to prepare for subsequent parsing
- The function ensures proper cleanup by setting result_start to NULL after successful capture