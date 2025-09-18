# elements_object_start

## Location
[src/backend/utils/adt/jsonfuncs.c:2416-2430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2416-L2430)

## Overview
This function serves as a JSON parser callback that validates JSON structure when processing array elements, ensuring that objects are not encountered at the top level when array processing is expected.

## Definition


## Detailed Description
The  function is a callback function used during JSON parsing to handle the start of JSON objects within array element processing contexts. Its primary purpose is to perform validation rather than processing - it ensures that when parsing JSON for array element extraction, objects are not encountered at the top level (lex_level == 0).

When an object is found at the top level during array element processing, the function raises an error indicating that the operation cannot be performed on non-array JSON values. This validation is crucial for maintaining the integrity of PostgreSQL's JSON array element extraction functions.

For nested objects within arrays, the function simply returns success, allowing the parsing to continue.

## Parameters / Member Variables
- : Pointer to ElementsState structure containing parser state and configuration information

## Dependencies
- Functions called/Symbols referenced:
  - [ElementsState](../E/ElementsState.md) (state structure)
  - JSON_SUCCESS (return value constant)
  - JsonParseErrorType (return type)
  - ereport/ERROR (error reporting mechanism)
  - [errcode](errcode.md)/ERRCODE_INVALID_PARAMETER_VALUE (error code)
  - [errmsg](errmsg.md) (error message formatting)

- Called from (representative examples):
  - [elements_worker](elements_worker.md) (main processing function)
  - JsObjectFree (cleanup context)

## Notes and Other Information
- Performs structural validation rather than data processing
- Only validates at the top level (lex_level == 0) of JSON structure
- Raises ERRCODE_INVALID_PARAMETER_VALUE when objects are found at top level
- Essential for json_array_elements() and related functions to ensure correct input type
- Part of PostgreSQL's JSON validation and parsing infrastructure
- Returns JSON_SUCCESS for valid nested object structures