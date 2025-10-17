# transform_string_values_object_start

## Location
[src/backend/utils/adt/jsonfuncs.c:5862-5871](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L5862-L5871)

## Overview
This auxiliary function handles the start of JSON objects during JSON string value transformation by appending the opening brace character to the output.

## Definition
static JsonParseErrorType transform_string_values_object_start(void *state)

## Detailed Description
This function is a callback used by the JSON parser during the transformation process. It is invoked whenever the parser encounters the beginning of a JSON object (indicated by an opening brace '{'). The function simply appends the '{' character to the output string buffer, ensuring that the JSON structure is preserved in the transformed output. This is part of the semantic action framework that handles different JSON elements during parsing.

The function operates as a simple pass-through for object start events, maintaining the original JSON structure while allowing other callback functions to handle the actual string value transformations.

## Parameters / Member Variables
- `state`: Pointer to TransformJsonStringValuesState structure containing the parsing context and output buffer

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoCharMacro
- Called from (representative examples):
  - [transform_json_string_values](transform_json_string_values.md) (set as sem->object_start callback)

## Notes and Other Information
- Returns JSON_SUCCESS to indicate successful processing
- Static function only accessible within jsonfuncs.c
- Part of the auxiliary function set for transform_json_string_values
- Located in src/backend/utils/adt/jsonfuncs.c:5862-5871
- Uses macro for efficient character appending to StringInfo buffer

## Simplified Source
```c
static JsonParseErrorType
transform_string_values_object_start(void *state) {
    TransformJsonStringValuesState *_state = (TransformJsonStringValuesState *) state;

    // Append opening brace for JSON object
    appendStringInfoCharMacro(_state->strval, '{');

    return JSON_SUCCESS;
}
```