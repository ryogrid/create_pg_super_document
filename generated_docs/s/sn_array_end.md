# sn_array_end

## Location
src/backend/utils/adt/jsonfuncs.c: 4418 - 4427

## Overview
A callback function used during JSON parsing to handle the end of a JSON array, appending the closing bracket to the output string.

## Definition
```c
static JsonParseErrorType sn_array_end(void *state)
```

## Detailed Description
This function is part of the JSON null-stripping functionality in PostgreSQL. It serves as a callback handler that is invoked when the JSON parser encounters the end of an array. The function simply appends a closing bracket `]` to the string buffer being constructed and returns a success status. This is a straightforward semantic action that maintains the JSON array structure while allowing other parts of the parsing process to handle null value stripping.

## Parameters / Member Variables
- `state`: A void pointer that is cast to `StripnullState *` containing the parsing state and output string buffer

## Dependencies
- Functions called/Symbols referenced:
  - `appendStringInfoCharMacro` - Macro to append a single character to a StringInfo buffer
  - `StripnullState` - State structure for null-stripping operations
  - `JSON_SUCCESS` - Success return code constant
  - `JsonParseErrorType` - Return type for JSON parsing operations

- Called from (representative examples):
  - `json_strip_nulls` - Main function that orchestrates JSON null stripping
  - `JsObjectFree` - Object cleanup function

## Notes and Other Information
This function is part of a set of semantic action callbacks used by the JSON parser when processing JSON data for null value removal. The function is designed to be stateless except for the output buffer manipulation, making it safe for use in the JSON parsing framework. The use of `appendStringInfoCharMacro` provides efficient single-character appending to the string buffer.