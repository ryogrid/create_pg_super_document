# okeys_scalar

## Location
[src/backend/utils/adt/jsonfuncs.c:822-843](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L822-L843)

## Overview
A semantic action callback function that validates the top-level JSON structure is an object (not a scalar value) for the json_object_keys function.

## Definition

```c
static JsonParseErrorType
okeys_scalar(void *state, char *token, JsonTokenType tokentype)
```
## Detailed Description
This function serves as a validation callback in PostgreSQL's JSON parser framework specifically for the json_object_keys function. Similar to okeys_array_start, its primary purpose is to ensure that the top-level JSON structure being parsed is an object rather than a scalar value (string, number, boolean, or null).

The function performs validation by checking if a scalar value is encountered at the top level (lex_level == 0). If so, it immediately raises an error since json_object_keys requires a JSON object to extract keys from, and scalar values have no keys to extract.

## Parameters / Member Variables
- `*state`: Void pointer to OkeysState structure containing the JSON lexer context and parsing state
- `*token`: Character pointer to the scalar token value (unused in validation)
- `tokentype`: JsonTokenType enumeration indicating the type of scalar token (unused in validation)
## Dependencies
- Functions called/Symbols referenced:
  - [OkeysState](../O/OkeysState.md) (cast from state parameter)
  - [JsonTokenType](../J/JsonTokenType.md) (parameter type)
  - ereport (for error reporting)
  - ERROR (error level constant)
  - [errcode](../e/errcode.md) (for error code specification)
  - ERRCODE_INVALID_PARAMETER_VALUE
  - [errmsg](../e/errmsg.md) (for error message formatting)
  - JSON_SUCCESS (return value)
- Called from (representative examples):
  - [json_object_keys](../j/json_object_keys.md) (assigned as semantic action callback)
  - JsObjectFree

## Notes and Other Information
- Only triggered when a scalar value is encountered at the top level of JSON input
- Raises a PostgreSQL error with ERRCODE_INVALID_PARAMETER_VALUE when validation fails
- Provides type safety by preventing misuse of json_object_keys on scalar JSON values
- The token and tokentype parameters are available but not used in the current implementation
- Part of the comprehensive validation strategy that includes both array and scalar checks
- The error message specifically mentions 'json_object_keys' for clear user feedback

## Simplified Source

```c
static JsonParseErrorType okeys_scalar(void *state, char *token, JsonTokenType tokentype) {
    OkeysState *_state = (OkeysState *) state;

    // Ensure top level is not a scalar - json_object_keys requires an object
    if (_state->lex->lex_level == 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot call %s on a scalar", "json_object_keys")));

    return JSON_SUCCESS;
}
```