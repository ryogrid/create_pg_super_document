# get_array_element_start

## Location
src/backend/utils/adt/jsonfuncs.c: 1351 - 1398

## Overview
A static callback function used during JSON parsing to handle the start of individual array elements, managing array index tracking and determining when specific array elements should be captured based on the extraction path.

## Definition
```c
static JsonParseErrorType get_array_element_start(void *state, bool isnull)
```

## Detailed Description
The `get_array_element_start` function is a JSON parser callback that processes the beginning of each individual element within JSON arrays during path-based extraction operations. It maintains array element counters, compares current indices with target path specifications, and determines whether the current element should be captured as a result.

The function implements sophisticated path matching logic:
- Increments array element counters as elements are encountered
- Compares current array positions with target path indices
- Sets path validity flags for nested structures
- Determines when to capture element values based on path completion
- Handles both intermediate path steps and final target elements

When an element matches the extraction path, the function prepares for value capture by setting up result tracking and handling special cases like string normalization.

## Parameters / Member Variables
- `state`: A void pointer that is cast to `GetState *`, containing the parsing state including lexical analyzer, path information, array indices, and result tracking
- `isnull`: A boolean indicating whether the current array element is null (though not directly used in the current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - GetState (struct type for casting state parameter)
  - JsonParseErrorType (return type)
  - JSON_TOKEN_STRING (token type constant for string detection)
  - JSON_SUCCESS (success return constant)
- Called from (representative examples):
  - get_worker (JSON extraction worker function)
  - JsObjectFree (JSON object processing)

## Notes and Other Information
- This function works in conjunction with array start/end callbacks to manage complete array processing
- Array indexing is zero-based and maintained at each lexical level for nested arrays
- Path matching logic supports both intermediate navigation and final element capture
- String normalization is conditionally enabled based on parsing configuration
- The function follows the same logical pattern as object field processing ("`same logic as for objects`" comment)
- Null array elements are supported through the isnull parameter but current implementation focuses on index tracking regardless of null status