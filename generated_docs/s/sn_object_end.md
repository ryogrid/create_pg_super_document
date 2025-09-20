# sn_object_end

## Location
[src/backend/utils/adt/jsonfuncs.c:4398-4407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4398-L4407)

## Overview
This function handles the end of JSON objects during null-stripping operations, appending a closing brace to the output string buffer.

## Definition

```c
static JsonParseErrorType
sn_object_end(void *state)
```
## Detailed Description
The `sn_object_end` function is a callback handler used by the JSON null-stripping functionality (`json_strip_nulls`). It serves as the counterpart to `sn_object_start` and processes the end of JSON objects during parsing.

This function is part of the semantic action system designed to reproduce input JSON while selectively removing null object fields. When a JSON object ends (indicated by a '}' character), this function appends the closing brace to the output string buffer, properly terminating the JSON object structure.

The function works in coordination with other semantic action handlers to maintain valid JSON syntax while allowing the null-stripping logic to filter out unwanted null fields. It ensures that every object opened by `sn_object_start` is properly closed, maintaining the integrity of the JSON structure in the filtered output.

## Parameters / Member Variables
- `state`: A void pointer to `StripnullState` structure containing the output string buffer and parsing state information

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoCharMacro (appends character to string buffer)
  - JSON_SUCCESS (return constant)
  - JsonParseErrorType (return type) 
  - [StripnullState](../S/StripnullState.md) (state structure)

- Called from (representative examples):
  - [json_strip_nulls](../j/json_strip_nulls.md)
  - JsObjectFree

## Notes and Other Information
- This is a static function accessible only within jsonfuncs.c
- Part of the semantic action system for `json_strip_nulls` functionality
- Works as a pair with `sn_object_start` to properly bracket JSON objects in the output
- The function performs a simple but essential operation for maintaining valid JSON syntax
- Uses PostgreSQL's StringInfo infrastructure for efficient string building
- Always returns JSON_SUCCESS as object end operations cannot fail
- The actual logic for filtering null fields occurs in other semantic action functions, particularly field and scalar handlers
- Ensures proper nesting and structure preservation in the filtered JSON output