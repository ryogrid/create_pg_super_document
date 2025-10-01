# hash_array_start

## Location
[src/backend/utils/adt/jsonfuncs.c:3928-3940](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3928-L3940)

## Overview
A static JSON parsing callback function that validates against top-level arrays during JSON-to-hash conversion, ensuring only JSON objects are processed.

## Definition
```c
static JsonParseErrorType hash_array_start(void *state)
```

## Detailed Description
This function serves as a validation callback in the JSON parsing framework specifically designed to prevent processing of top-level JSON arrays during hash table construction. The function's primary purpose is to enforce the constraint that only JSON objects (not arrays) can be converted to hash tables.

When called at the top level (lex_level == 0), indicating the start of the root JSON structure, the function detects if the input is an array and raises an error. This is because hash tables are key-value structures that correspond to JSON objects, not arrays. Arrays don't have field names that can serve as hash keys.

For nested arrays (lex_level > 0), the function allows processing to continue, as these will be handled as values within object fields.

## Parameters / Member Variables
- `state`: A void pointer cast to JHashState*, containing parsing state including the function name for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [JHashState](../J/JHashState.md) (struct type for state management)
  - JSON_SUCCESS (return value constant)
  - JsonParseErrorType (return type)
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
- The function enforces a fundamental constraint: only JSON objects can be converted to PostgreSQL hash tables
- Error messages include the calling function name for better debugging context
- Nested arrays within objects are permitted and handled by other parts of the parsing framework
- The lex_level check specifically targets the root level (level 0) to catch top-level arrays
- This validation prevents runtime errors that would occur later when trying to use array elements as hash keys

## Simplified Source

```c
static JsonParseErrorType hash_array_start(void *state) {
    JHashState *_state = (JHashState *) state;

    // Error if top-level JSON is an array (arrays can't be hash tables)
    if (_state->lex->lex_level == 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call %s on an array", _state->function_name)));

    return JSON_SUCCESS;
}
```