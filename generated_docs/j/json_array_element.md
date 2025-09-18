# json_array_element

## Location
src/backend/utils/adt/jsonfuncs.c: 920 - 934

## Overview
Extracts an element from a JSON array by zero-based index and returns the element value as JSON text, or NULL if the index is out of bounds or the input is not a valid JSON array.

## Definition
```c
Datum json_array_element(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the JSON array element access operator (->). It takes a JSON text value and an integer index as input parameters, retrieves the element at the specified zero-based index from the JSON array, and returns the corresponding element as a JSON text datum. The function uses the generic get_worker function to perform the actual JSON parsing and element extraction, specifying that the result should be returned in JSON format (not as text).

The function handles negative indices and out-of-bounds access by returning NULL through the get_worker function's error handling.

## Parameters / Member Variables
- `json`: The input JSON text value representing an array from which to extract an element
- `element`: The zero-based integer index of the array element to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP: Retrieves text argument from function call
  - PG_GETARG_INT32: Retrieves 32-bit integer argument from function call
  - get_worker: Generic JSON processing function that handles array element extraction
  - PG_RETURN_TEXT_P: Returns text result from function
  - PG_RETURN_NULL: Returns NULL result from function
- Called from (representative examples):
  - No direct references found (likely called through SQL function interface)

## Notes and Other Information
- This function is the backend implementation for the JSON -> operator with integer indices in SQL
- Returns NULL if the input is not a valid JSON array or if the specified index is out of bounds
- Uses zero-based indexing like most programming languages
- The function delegates the actual JSON parsing and extraction logic to get_worker
- The false parameter to get_worker indicates that the result should be returned as JSON text, not as plain text
- Located in src/backend/utils/adt/jsonfuncs.c:920-934