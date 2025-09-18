# each_scalar

## Location
src/backend/utils/adt/jsonfuncs.c: 2180 - 2205

## Overview
A callback function used during JSON parsing that handles scalar values, providing validation for object expansion operations and storing de-escaped scalar values when needed.

## Definition


## Detailed Description
This function is a callback used by PostgreSQL's JSON parser to process scalar values during JSON object expansion operations. It serves dual purposes: first, it validates that standalone scalar values are not being deconstructed as objects (which would be invalid), and second, it stores normalized scalar values when required by the expansion operation.

The function checks if a scalar appears at the top level (lex_level == 0) and raises an error in such cases, as object expansion functions expect objects, not standalone scalars. For valid scenarios, it captures the de-escaped token value in the state for later use by field processing functions.

## Parameters / Member Variables
- : Pointer to an EachState structure containing parser state and configuration
- : C string containing the scalar value token from the JSON input
- : JsonTokenType enumeration indicating the type of the scalar token

## Dependencies
- Functions called/Symbols referenced:
  - EachState (state structure)
  - JsonTokenType (token type enumeration)
  - ereport (PostgreSQL error reporting function)
  - errcode (error code specification)
  - errmsg (error message specification)
  - JSON_SUCCESS (return value constant)
  - ERRCODE_INVALID_PARAMETER_VALUE (PostgreSQL error code)
- Called from:
  - each_worker (main JSON expansion worker function)
  - JsObjectFree (JSON object callback structure)

## Notes and Other Information
- Primarily serves as validation for object expansion operations
- Prevents incorrect usage of object expansion functions on standalone scalar data
- Stores de-escaped scalar values in the state's normalized_scalar field when next_scalar flag is set
- Part of PostgreSQL's JSON expansion infrastructure for functions like json_each() and jsonb_each()
- Only validates top-level scalars (lex_level == 0) as scalars within objects are expected and valid
- Returns JSON_SUCCESS when validation passes and scalar is processed correctly