# sn_array_element_start

## Location
src/backend/utils/adt/jsonfuncs.c: 4458 - 4468

## Overview
A callback function used during JSON parsing to handle the start of an array element, managing comma separation between array elements.

## Definition
```c
static JsonParseErrorType sn_array_element_start(void *state, bool isnull)
```

## Detailed Description
This function is part of the JSON null-stripping functionality in PostgreSQL. It serves as a callback handler that is invoked when the JSON parser encounters the start of an array element. The function manages proper JSON array syntax by inserting commas between elements. It checks if the previous character in the output buffer is an opening bracket `[` to determine if this is the first element in the array. If not the first element, it appends a comma to maintain valid JSON array format. The function is simple but essential for maintaining proper JSON syntax during the parsing and reconstruction process.

## Parameters / Member Variables
- `state`: A void pointer that is cast to `StripnullState *` containing the parsing state and output string buffer
- `isnull`: Boolean flag indicating whether the array element value is null (currently not used in the implementation)

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
This function is part of a set of semantic action callbacks used by the JSON parser. Unlike the object field handler, this function does not currently use the `isnull` parameter, suggesting that array element null handling might be managed elsewhere in the parsing chain. The function focuses solely on comma insertion logic, making it a specialized handler for array element syntax management. The use of `appendStringInfoCharMacro` provides efficient single-character appending to the string buffer.