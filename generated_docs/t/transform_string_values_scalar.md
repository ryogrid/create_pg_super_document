# transform_string_values_scalar

## Location
src/backend/utils/adt/jsonfuncs.c: 5931 - 5947

## Overview
Handles scalar JSON values during string transformation, applying user-defined transformations to string tokens while preserving other scalar types unchanged.

## Definition
```c
static JsonParseErrorType transform_string_values_scalar(void *state, char *token, JsonTokenType tokentype)
```

## Detailed Description
This function is a callback handler used during JSON parsing to process scalar values (strings, numbers, booleans, and null). It serves as the core transformation logic for the JSON string transformation system. When the token is a JSON string (JSON_TOKEN_STRING), it applies a user-defined transformation action to the string value and properly re-escapes the result. For all other scalar types (numbers, booleans, null), it preserves them unchanged by appending them directly to the output buffer.

The transformation process involves calling the user-provided action function with the string content, receiving a transformed PostgreSQL text object, converting it to a C string, and then properly escaping it as a JSON string literal.

## Parameters / Member Variables
- `state`: Pointer to TransformJsonStringValuesState containing the parser context, output buffer, and transformation action
- `token`: The string representation of the scalar value to process
- `tokentype`: JsonTokenType enum value indicating the type of token (string, number, boolean, null, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - TransformJsonStringValuesState (state structure containing transformation context)
  - JSON_TOKEN_STRING (enum value for string tokens)
  - escape_json (function to properly escape JSON string literals)
  - text_to_cstring (function to convert PostgreSQL text to C string)
  - appendStringInfoString (function to append strings to StringInfo buffer)
  - JSON_SUCCESS (return value constant)
  - JsonTokenType (enum type for token classification)
- Called from (representative examples):
  - transform_json_string_values (main transformation function)
  - JsObjectFree (JSON object processing context)

## Notes and Other Information
- This is a static function, only used within jsonfuncs.c
- The core logic that differentiates between transformable strings and pass-through scalars
- String transformations are applied via the action function pointer stored in the state structure
- Non-string scalars (numbers, booleans, null) are preserved exactly as parsed
- The action function is expected to return a PostgreSQL text object with the transformed string
- Part of the JSON transformation infrastructure supporting functions like json_strip_nulls, json_transform_text, etc.
- Returns JSON_SUCCESS on successful completion following the standard JSON parsing callback pattern
- The function assumes the action callback can handle the string content and return valid transformed text