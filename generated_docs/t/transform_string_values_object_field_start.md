# transform_string_values_object_field_start

## Location
[src/backend/utils/adt/jsonfuncs.c:5902-5919](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L5902-L5919)

## Overview
Handles the start of an object field during JSON string value transformation, properly formatting the field name and preparing for the field value.

## Definition

```c
static JsonParseErrorType
transform_string_values_object_field_start(void *state, char *fname, bool isnull)
```
## Detailed Description
This function is a callback handler used during JSON parsing to process the beginning of an object field. It formats the field name by adding necessary punctuation (comma separator if needed) and properly escaping the field name as a JSON string. The function ensures proper JSON syntax by adding a colon after the field name to separate it from the upcoming field value.

The function checks if the current JSON string being built already has content beyond the opening brace, and if so, adds a comma separator before the new field. It then re-escapes the field name (since the original escaped version is no longer available) and appends a colon to prepare for the field value.

## Parameters / Member Variables
- `*state`: Pointer to TransformJsonStringValuesState containing the parser context and output buffer
- `*fname`: The field name string to be processed and escaped
- `isnull`: Boolean indicating if the field name is null (parameter appears unused in this implementation)
## Dependencies
- Functions called/Symbols referenced:
  - [TransformJsonStringValuesState](../T/TransformJsonStringValuesState.md) (state structure)
  - appendStringInfoCharMacro (macro for appending single characters)
  - [escape_json](../e/escape_json.md) (function to properly escape JSON strings)
  - JSON_SUCCESS (return value constant)
  - JsonParseErrorType (return type)
- Called from (representative examples):
  - [transform_json_string_values](transform_json_string_values.md) (main transformation function)
  - JsObjectFree (JSON object processing context)

## Notes and Other Information
- This is a static function, indicating it's only used within the jsonfuncs.c file
- The function assumes the field name needs to be re-escaped since the original escaped version is not preserved
- The comma logic ensures proper JSON object syntax by checking the last character of the current output
- Part of the JSON string transformation infrastructure used for functions like json_strip_nulls and similar operations
- Returns JSON_SUCCESS on successful completion, following the standard JSON parsing callback pattern

## Simplified Source
```c
static JsonParseErrorType
transform_string_values_object_field_start(void *state, char *fname, bool isnull) {
    TransformJsonStringValuesState *_state = (TransformJsonStringValuesState *) state;

    // Add comma separator if not the first field
    if (_state->strval->data[_state->strval->len - 1] != '{')
        appendStringInfoCharMacro(_state->strval, ',');

    // Re-escape and append field name with colon
    escape_json(_state->strval, fname);
    appendStringInfoCharMacro(_state->strval, ':');

    return JSON_SUCCESS;
}
```