# sn_object_start

## Location
[src/backend/utils/adt/jsonfuncs.c:4388-4397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4388-L4397)

## Overview
This function handles the start of JSON objects during null-stripping operations, appending an opening brace to the output string buffer.

## Definition


## Detailed Description
The `sn_object_start` function is a callback handler used by the JSON null-stripping functionality (`json_strip_nulls`). It serves as a semantic action that processes the beginning of JSON objects during parsing.

The function is part of a set of semantic actions designed to reproduce the input JSON while selectively removing null object fields. When a JSON object starts (indicated by a '{' character), this function simply appends the opening brace to the output string buffer, maintaining the JSON structure while allowing other handlers to filter out null fields.

This function operates as part of the JSON parsing callback system, working in conjunction with other semantic action functions like `sn_object_end`, field handlers, and scalar handlers to reconstruct valid JSON output with null fields removed.

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
- The function performs a simple operation but is essential for maintaining proper JSON object syntax in the output
- Works in coordination with `sn_object_end` to properly bracket JSON objects
- The actual null-field filtering logic is handled by other semantic action functions, not this one
- Uses PostgreSQL's StringInfo infrastructure for efficient string building
- Always returns JSON_SUCCESS as object start operations cannot fail
- The function comment indicates that null field state management happens in field start handlers and is reset during scalar actions