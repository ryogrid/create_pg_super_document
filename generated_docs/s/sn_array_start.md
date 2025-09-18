# sn_array_start

## Location
src/backend/utils/adt/jsonfuncs.c: 4408 - 4417

## Overview
This function handles the start of JSON arrays during null-stripping operations, appending an opening bracket to the output string buffer.

## Definition


## Detailed Description
The `sn_array_start` function is a callback handler used by the JSON null-stripping functionality (`json_strip_nulls`). It serves as a semantic action that processes the beginning of JSON arrays during parsing.

This function is part of the comprehensive semantic action system designed to reproduce input JSON while selectively removing null object fields. When a JSON array starts (indicated by a '[' character), this function appends the opening bracket to the output string buffer, maintaining proper JSON array syntax in the filtered output.

The function works alongside other semantic action handlers including `sn_object_start`, `sn_object_end`, and array end handlers to preserve the overall JSON structure while allowing field-level filtering to remove null values from objects. Unlike object field filtering, array elements are typically preserved regardless of their null status, as arrays maintain positional significance.

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
- Works in coordination with array end handlers to properly bracket JSON arrays
- The function performs a simple but critical operation for maintaining valid JSON array syntax
- Uses PostgreSQL's StringInfo infrastructure for efficient string building
- Always returns JSON_SUCCESS as array start operations cannot fail
- Array elements are generally preserved during null-stripping, unlike object fields which may be filtered
- Essential for maintaining proper nesting and structure in the filtered JSON output
- The null-stripping logic primarily applies to object fields rather than array elements