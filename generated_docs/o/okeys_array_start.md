# okeys_array_start

## Location
src/backend/utils/adt/jsonfuncs.c: 807 - 821

## Overview
A semantic action callback function that validates the top-level JSON structure is an object (not an array) for the json_object_keys function.

## Definition


## Detailed Description
This function serves as a validation callback in PostgreSQL's JSON parser framework specifically for the json_object_keys function. Its primary purpose is to ensure that the top-level JSON structure being parsed is an object rather than an array, since json_object_keys is designed to extract keys from objects only.

The function performs a simple but critical validation: if an array is encountered at the top level (lex_level == 0), it immediately raises an error with an appropriate message. This prevents inappropriate usage of json_object_keys on JSON arrays, which would be meaningless since arrays don't have named keys.

## Parameters / Member Variables
- : Void pointer to OkeysState structure containing the JSON lexer context and parsing state

## Dependencies
- Functions called/Symbols referenced:
  - OkeysState (cast from state parameter)
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
- Only triggered when an array is encountered at the top level of JSON input
- Raises a PostgreSQL error with ERRCODE_INVALID_PARAMETER_VALUE when validation fails
- Essential for preventing misuse of json_object_keys function on inappropriate JSON structures
- Part of the semantic action framework that provides type safety for JSON processing functions
- The error message specifically mentions 'json_object_keys' to provide clear user feedback