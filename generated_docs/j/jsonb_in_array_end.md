# jsonb_in_array_end

## Location
[src/backend/utils/adt/jsonb.c:321-330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L321-L330)

## Overview
A callback function used during JSONB parsing to handle the end of JSON arrays, finalizing the parsing state for completed array processing.

## Definition

```c
static JsonParseErrorType
jsonb_in_array_end(void *pstate)
```
## Detailed Description
This function serves as a callback handler in the JSONB input parsing pipeline. When the JSON parser encounters the end of an array (']' character), this function is invoked to finalize the array parsing process. It pushes a WJB_END_ARRAY token onto the JSONB parse state stack, which signals the completion of array parsing and triggers the finalization of the parsed array structure.

The function works as the counterpart to jsonb_in_array_start, completing the bracketing of array parsing within PostgreSQL's streaming JSON parser framework. Together, these callbacks ensure proper nesting and state management during complex JSON array parsing, including nested arrays and arrays containing objects.

## Parameters / Member Variables
- : A void pointer that is cast to JsonbInState*, containing the current parsing state and accumulated array elements

## Dependencies
- Functions called/Symbols referenced:
  - pushJsonbValue (pushes parsing tokens onto the state stack)
  - WJB_END_ARRAY (token constant indicating array end)
  - JSON_SUCCESS (return value indicating successful parsing)
  - JsonbInState (parsing state structure)
- Called from (representative examples):
  - jsonb_from_cstring (main JSONB input function)
  - datum_to_jsonb_internal (internal conversion function)

## Notes and Other Information
- This is a static function internal to the jsonb.c module
- Part of the callback-based JSON parsing architecture in PostgreSQL
- Works in conjunction with jsonb_in_array_start to bracket array parsing
- Returns JSON_SUCCESS to indicate successful processing of the array end token
- The WJB_END_ARRAY token triggers finalization of the parsed array in the JSONB value construction process
- Handles proper cleanup and consolidation of array elements parsed between the start and end tokens