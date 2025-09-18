# okeys_scalar

## Location
src/backend/utils/adt/jsonfuncs.c: 822 - 843

## Overview
A semantic action callback function that validates the top-level JSON structure is an object (not a scalar value) for the json_object_keys function.

## Definition


## Detailed Description
This function serves as a validation callback in PostgreSQL's JSON parser framework specifically for the json_object_keys function. Similar to okeys_array_start, its primary purpose is to ensure that the top-level JSON structure being parsed is an object rather than a scalar value (string, number, boolean, or null).

The function performs validation by checking if a scalar value is encountered at the top level (lex_level == 0). If so, it immediately raises an error since json_object_keys requires a JSON object to extract keys from, and scalar values have no keys to extract.

## Parameters / Member Variables
- : Void pointer to OkeysState structure containing the JSON lexer context and parsing state
- : Character pointer to the scalar token value (unused in validation)
- : JsonTokenType enumeration indicating the type of scalar token (unused in validation)

## Dependencies
- Functions called/Symbols referenced:
  - OkeysState (cast from state parameter)
  - JsonTokenType (parameter type)
  - ereport (for error reporting)
  - ERROR (error level constant)
  - errcode (for error code specification)
  - ERRCODE_INVALID_PARAMETER_VALUE
  - errmsg (for error message formatting)
  - JSON_SUCCESS (return value)
- Called from (representative examples):
  - json_object_keys (assigned as semantic action callback)
  - JsObjectFree

## Notes and Other Information
- Only triggered when a scalar value is encountered at the top level of JSON input
- Raises a PostgreSQL error with ERRCODE_INVALID_PARAMETER_VALUE when validation fails
- Provides type safety by preventing misuse of json_object_keys on scalar JSON values
- The token and tokentype parameters are available but not used in the current implementation
- Part of the comprehensive validation strategy that includes both array and scalar checks
- The error message specifically mentions 'json_object_keys' for clear user feedback