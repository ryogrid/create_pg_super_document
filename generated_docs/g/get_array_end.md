# get_array_end

## Location
src/backend/utils/adt/jsonfuncs.c: 1333 - 1350

## Overview
A static callback function used during JSON parsing to handle the end of JSON arrays, responsible for capturing the complete array text when entire arrays need to be returned as results.

## Definition
```c
static JsonParseErrorType get_array_end(void *state)
```

## Detailed Description
The `get_array_end` function is a JSON parser callback that processes the end of JSON arrays during parsing operations. It works as the counterpart to `get_array_start` and is primarily responsible for capturing the complete array text when the parsing operation is designed to return an entire array rather than specific elements within it.

The function handles a specific use case: when the path specification indicates that the entire array should be returned (indicated by `lex_level == 0 && _state->npath == 0`). In this scenario, it calculates the length of the array text from the start position (set by `get_array_start`) to the current end position, then converts this text segment into a PostgreSQL text result.

This function is part of the JSON path extraction system and ensures proper completion of array processing during JSON parsing operations.

## Parameters / Member Variables
- `state`: A void pointer that is cast to `GetState *`, containing the parsing state including lexical analyzer, result tracking, and text boundaries for the array being processed

## Dependencies
- Functions called/Symbols referenced:
  - GetState (struct type for casting state parameter)
  - JsonParseErrorType (return type)
  - cstring_to_text_with_len (converts C string to PostgreSQL text type)
  - JSON_SUCCESS (success return constant)
- Called from (representative examples):
  - get_worker (JSON extraction worker function)
  - JsObjectFree (JSON object processing)

## Notes and Other Information
- This function only performs text capture in the special case where the entire array is the target result
- The result text includes the complete array syntax including brackets and all contained elements
- Text boundaries are determined using lexical analyzer token positions
- The function works in conjunction with `get_array_start` which sets the initial result_start position
- Returns JSON_SUCCESS in all cases, indicating successful completion of array processing