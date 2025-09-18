# iterate_values_scalar

## Location
[src/backend/utils/adt/jsonfuncs.c:5733-5760](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L5733-L5760)

## Overview
An auxiliary callback function for JSON parsing that processes scalar values (strings, numbers, booleans) and conditionally invokes a user-defined action based on type flags.

## Definition
```c
static JsonParseErrorType iterate_values_scalar(void *state, char *token, JsonTokenType tokentype)
```

## Detailed Description
This function serves as a JSON parser callback handler specifically for scalar values encountered during JSON parsing. It examines the token type and the flags stored in the parsing state to determine whether to invoke the user-defined action callback. The function handles three types of scalar JSON tokens: strings, numbers, and boolean values (true/false). For each supported token type, it checks the corresponding flag bit and calls the action callback with the token string and its length if the flag is set.

## Parameters / Member Variables
- `state`: Pointer to IterateJsonStringValuesState containing parsing context and callback information
- `token`: String representation of the JSON token
- `tokentype`: JsonTokenType enum indicating the type of the current token

## Dependencies
- Functions called/Symbols referenced:
  - strlen (for calculating token length)
- Called from (representative examples):
  - [iterate_json_values](iterate_json_values.md) (registered as scalar callback)
  - JsObjectFree

## Notes and Other Information
The function is designed to be used as a callback in the JSON parser's semantic action framework. It filters scalar values based on the jtiString, jtiNumeric, and jtiBool flags, allowing selective processing of different JSON value types. The function always returns JSON_SUCCESS to indicate successful processing. Unlike iterate_jsonb_values which handles value conversion, this function passes the raw token strings directly to the action callback since JSON parsing already provides string representations.