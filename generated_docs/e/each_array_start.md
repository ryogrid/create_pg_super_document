# each_array_start

## Location
src/backend/utils/adt/jsonfuncs.c: 2166 - 2179

## Overview
A callback function used during JSON parsing that validates the structure when encountering the start of an array, ensuring arrays are not being deconstructed as objects.

## Definition


## Detailed Description
This function is a callback used by PostgreSQL's JSON parser specifically for JSON object expansion operations. It serves as a validation checkpoint when the parser encounters the beginning of an array. The primary purpose is to enforce that arrays cannot be deconstructed as objects, which would be a structural mismatch. When called at the top level (lex_level == 0), it raises an error indicating that the operation is invalid.

This function is part of the JSON parsing callback system used by functions like json_each() and jsonb_each() that expect to work with JSON objects, not arrays.

## Parameters / Member Variables
- : Pointer to an EachState structure containing parser state and configuration information

## Dependencies
- Functions called/Symbols referenced:
  - [EachState](../E/EachState.md) (state structure)
  - ereport (PostgreSQL error reporting function)
  - [errcode](errcode.md) (error code specification)
  - [errmsg](errmsg.md) (error message specification)  
  - JSON_SUCCESS (return value constant)
  - ERRCODE_INVALID_PARAMETER_VALUE (PostgreSQL error code)
- Called from:
  - [each_worker](each_worker.md) (main JSON expansion worker function)
  - JsObjectFree (JSON object callback structure)

## Notes and Other Information
- This function primarily serves as a structural validation checkpoint
- Prevents incorrect usage of object expansion functions on array data
- Only checks for top-level arrays (lex_level == 0) as nested arrays within objects are acceptable
- Part of PostgreSQL's JSON expansion infrastructure for object-specific operations
- Returns JSON_SUCCESS when validation passes (array is not at top level)