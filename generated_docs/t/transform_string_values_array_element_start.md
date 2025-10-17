# transform_string_values_array_element_start

## Location
[src/backend/utils/adt/jsonfuncs.c:5920-5930](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L5920-L5930)

## Overview
Handles the start of an array element during JSON string value transformation, adding necessary comma separators between array elements.

## Definition
```c
static JsonParseErrorType transform_string_values_array_element_start(void *state, bool isnull)
```

## Detailed Description
This function is a callback handler used during JSON parsing to process the beginning of an array element. It ensures proper JSON array syntax by adding comma separators between array elements when needed. The function checks if the current position in the JSON string being built is immediately after the opening bracket '[', and if not, it adds a comma to separate the new element from previous elements.

This is a simpler counterpart to the object field start handler, focusing solely on array element separation without needing to handle field names or colons.

## Parameters / Member Variables
- `state`: Pointer to TransformJsonStringValuesState containing the parser context and output buffer
- `isnull`: Boolean indicating if the array element is null (parameter appears unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [TransformJsonStringValuesState](../T/TransformJsonStringValuesState.md) (state structure)
  - appendStringInfoCharMacro (macro for appending single characters)
  - JSON_SUCCESS (return value constant)
  - JsonParseErrorType (return type)
- Called from (representative examples):
  - [transform_json_string_values](transform_json_string_values.md) (main transformation function)
  - JsObjectFree (JSON object processing context)

## Notes and Other Information
- This is a static function, indicating it's only used within the jsonfuncs.c file
- Much simpler than its object field counterpart since arrays don't require field names or colons
- The comma logic ensures proper JSON array syntax by checking if we're immediately after the opening bracket
- Part of the JSON string transformation infrastructure used for functions like json_strip_nulls and similar operations
- Returns JSON_SUCCESS on successful completion, following the standard JSON parsing callback pattern
- Works in tandem with other transform_string_values_* functions to rebuild JSON with transformations

## Simplified Source
```c
static JsonParseErrorType
transform_string_values_array_element_start(void *state, bool isnull) {
    TransformJsonStringValuesState *_state = (TransformJsonStringValuesState *) state;

    // Add comma separator if not the first array element
    if (_state->strval->data[_state->strval->len - 1] != '[')
        appendStringInfoCharMacro(_state->strval, ',');

    return JSON_SUCCESS;
}
```