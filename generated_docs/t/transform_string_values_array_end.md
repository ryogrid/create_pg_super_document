# transform_string_values_array_end

## Location
[src/backend/utils/adt/jsonfuncs.c:5892-5901](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L5892-L5901)

## Overview
This auxiliary function handles the end of JSON arrays during JSON string value transformation by appending the closing bracket character to the output.

## Definition
static JsonParseErrorType transform_string_values_array_end(void *state)

## Detailed Description
This function serves as a callback for the JSON parser when it encounters the end of a JSON array (indicated by a closing bracket ']'). It maintains the structural integrity of JSON arrays by appending the ']' character to the output string buffer. This function works in tandem with transform_string_values_array_start to ensure that JSON arrays are properly opened and closed in the transformed output, while other callback functions handle the actual string value modifications within the array elements.

The function is part of the comprehensive semantic action callback system that processes different JSON structural elements during parsing, ensuring the reconstructed JSON maintains valid syntax with transformed string values.

## Parameters / Member Variables
- `state`: Pointer to TransformJsonStringValuesState structure containing the parsing context and output buffer

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoCharMacro
- Called from (representative examples):
  - [transform_json_string_values](transform_json_string_values.md) (set as sem->array_end callback)

## Notes and Other Information
- Returns JSON_SUCCESS to indicate successful processing
- Static function only accessible within jsonfuncs.c
- Part of the auxiliary function set for transform_json_string_values
- Located in src/backend/utils/adt/jsonfuncs.c:5892-5901
- Uses macro for efficient character appending to StringInfo buffer

## Simplified Source
```c
static JsonParseErrorType
transform_string_values_array_end(void *state) {
    TransformJsonStringValuesState *_state = (TransformJsonStringValuesState *) state;

    // Append closing bracket for JSON array
    appendStringInfoCharMacro(_state->strval, ']');

    return JSON_SUCCESS;
}
```