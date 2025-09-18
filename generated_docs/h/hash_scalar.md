# hash_scalar

## Location
src/backend/utils/adt/jsonfuncs.c: 3941 - 3971

## Overview
A static JSON parsing callback function that handles scalar values during JSON-to-hash conversion, validating input constraints and storing scalar field values.

## Definition
```c
static JsonParseErrorType hash_scalar(void *state, char *token, JsonTokenType tokentype)
```

## Detailed Description
This function serves as a callback for processing scalar values (strings, numbers, booleans, null) in the JSON parsing framework during hash table construction. It performs two main functions:

1. **Input validation**: Ensures that the top-level JSON input is not a scalar value, as hash tables require object structures with key-value pairs
2. **Value storage**: For scalar values at nesting level 1 (top-level object fields), it saves the token value for later storage in the hash table

The function enforces the constraint that only JSON objects can be converted to hash tables, rejecting top-level scalars that cannot provide the necessary key-value structure. For valid object field scalars, it stores the value in the parsing state for subsequent processing by hash_object_field_end.

## Parameters / Member Variables
- `state`: A void pointer cast to JHashState*, containing the parsing state and temporary storage
- `token`: The scalar value as a null-terminated string representation
- `tokentype`: The specific type of the scalar token (JsonTokenType enum value)

## Dependencies
- Functions called/Symbols referenced:
  - [JsonTokenType](../J/JsonTokenType.md) (enum type for token classification)
  - [JHashState](../J/JHashState.md) (struct type for state management)
  - JSON_SUCCESS (return value constant)
  - ereport (PostgreSQL error reporting function)
  - ERROR (error level constant)
  - [errcode](../e/errcode.md) (error code specification function)
  - ERRCODE_INVALID_PARAMETER_VALUE (specific error code)
  - [errmsg](../e/errmsg.md) (error message formatting function)
- Called from (representative examples):
  - [get_json_object_as_hash](../g/get_json_object_as_hash.md)
  - JsObjectFree

## Notes and Other Information
- This is a static function, only accessible within jsonfuncs.c
- The function rejects top-level scalars (lex_level == 0) as they cannot be converted to hash tables
- Only saves scalar values at level 1 (immediate children of the root object)
- Includes an assertion to verify token type consistency with the value saved by hash_object_field_start
- The saved scalar value is later retrieved by hash_object_field_end for storage in the hash table
- Error messages include the calling function name for better debugging context
- The token parameter contains the string representation of the scalar value, regardless of its actual type