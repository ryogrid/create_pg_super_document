# alen_scalar

## Location
src/backend/utils/adt/jsonfuncs.c: 1912 - 1925

## Overview
A JSON parser callback function that validates that the JSON input is not a scalar value when determining array length.

## Definition
```c
static JsonParseErrorType alen_scalar(void *state, char *token, JsonTokenType tokentype)
```

## Detailed Description
This function is a callback used by the JSON parser when processing array length operations. It serves as a validation step to ensure that the JSON being parsed is actually an array and not a scalar value (string, number, boolean, or null). The function is part of PostgreSQL's JSON array length checking mechanism and prevents attempts to get the length of scalar JSON values.

When called at the top level (lex_level == 0), it indicates that the JSON is a scalar value, which is invalid for array length operations, and the function raises an error.

## Parameters / Member Variables
- `state`: void pointer that is cast to AlenState structure containing the lexer state and other parsing information
- `token`: character pointer to the scalar token being processed
- `tokentype`: JsonTokenType enumeration indicating the type of scalar token (string, number, boolean, null)

## Dependencies
- Functions called/Symbols referenced:
  - [AlenState](../A/AlenState.md) (structure type)
  - JSON_SUCCESS (return value constant)
  - JsonParseErrorType (return type)
  - [JsonTokenType](../J/JsonTokenType.md) (parameter type)
  - ereport (error reporting function)
- Called from (representative examples):
  - [json_array_length](../j/json_array_length.md) (main array length function)
  - JsObjectFree (JSON object processing)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the jsonfuncs.c file
- The function is specifically designed to catch attempts to get array length from JSON scalar values
- Returns JSON_SUCCESS if the validation passes (not at top level)
- Throws an ERROR with ERRCODE_INVALID_PARAMETER_VALUE if validation fails
- Part of the JSON parsing callback system used throughout PostgreSQL's JSON functionality
- The token and tokentype parameters provide context about the specific scalar value encountered