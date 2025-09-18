# elements_scalar

## Location
src/backend/utils/adt/jsonfuncs.c: 2431 - 2461

## Overview
This function serves as a JSON parser callback that handles scalar values during JSON array element processing, performing validation and optionally storing normalized scalar values for later use.

## Definition


## Detailed Description
The  function is a callback function used during JSON parsing to process scalar values encountered within array elements. It serves dual purposes: validation and data preparation.

First, it performs structural validation by checking if a scalar value appears at the top level (lex_level == 0) of the JSON input. When this occurs during array element processing, it indicates invalid input since array processing functions expect arrays, not standalone scalar values.

Second, when operating within valid array contexts (nested levels), the function handles the storage of normalized (de-escaped) scalar values when required. If the  flag is set in the state, it stores the processed token value in  for later retrieval during tuple formation.

## Parameters / Member Variables
- : Pointer to ElementsState structure containing parser state and configuration
- : Character pointer to the processed scalar value (de-escaped if needed)
- : JsonTokenType enumeration indicating the type of JSON token encountered

## Dependencies
- Functions called/Symbols referenced:
  - JsonTokenType (token type enumeration)
  - ElementsState (state structure)
  - JSON_SUCCESS (return value constant)
  - ereport/ERROR (error reporting mechanism)
  - errcode/ERRCODE_INVALID_PARAMETER_VALUE (error code)
  - errmsg (error message formatting)

- Called from (representative examples):
  - elements_worker (main processing function)
  - JsObjectFree (cleanup context)

## Notes and Other Information
- Validates that scalars don't appear at the top level during array processing
- Stores normalized scalar values when the next_scalar flag is active
- Part of PostgreSQL's JSON array element extraction infrastructure
- Raises ERRCODE_INVALID_PARAMETER_VALUE for top-level scalars
- The token parameter contains de-escaped string values ready for use
- Essential for proper handling of string values within json_array_elements() functions
- Returns JSON_SUCCESS on successful processing or validation