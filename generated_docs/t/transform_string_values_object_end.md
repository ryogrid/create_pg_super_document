# transform_string_values_object_end

## Location
[src/backend/utils/adt/jsonfuncs.c:5872-5881](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L5872-L5881)

## Overview
This auxiliary function handles the end of JSON objects during JSON string value transformation by appending the closing brace character to the output.

## Definition
static JsonParseErrorType transform_string_values_object_end(void *state)

## Detailed Description
This function serves as a callback for the JSON parser when it encounters the end of a JSON object (indicated by a closing brace '}'). Like its counterpart transform_string_values_object_start, this function maintains the structural integrity of the JSON document by appending the '}' character to the output string buffer. It ensures that JSON objects are properly closed in the transformed output while other functions handle the actual string value modifications.

The function is part of the semantic action callback system that processes different JSON elements during parsing, working in conjunction with other callback functions to reconstruct the JSON with transformed string values.

## Parameters / Member Variables
- `state`: Pointer to TransformJsonStringValuesState structure containing the parsing context and output buffer

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoCharMacro
- Called from (representative examples):
  - [transform_json_string_values](transform_json_string_values.md) (set as sem->object_end callback)

## Notes and Other Information
- Returns JSON_SUCCESS to indicate successful processing
- Static function only accessible within jsonfuncs.c
- Part of the auxiliary function set for transform_json_string_values
- Located in src/backend/utils/adt/jsonfuncs.c:5872-5881
- Uses macro for efficient character appending to StringInfo buffer

## Simplified Source
```c
static JsonParseErrorType
transform_string_values_object_end(void *state) {
    TransformJsonStringValuesState *_state = (TransformJsonStringValuesState *) state;

    // Append closing brace for JSON object
    appendStringInfoCharMacro(_state->strval, '}');

    return JSON_SUCCESS;
}
```