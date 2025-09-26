# jsonb_in_array_start

## Location
[src/backend/utils/adt/jsonb.c:311-320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L311-L320)

## Overview
A callback function used during JSONB parsing to handle the start of JSON arrays, initializing the parsing state for array element processing.

## Definition

```c
static JsonParseErrorType
jsonb_in_array_start(void *pstate)
```
## Detailed Description
This function serves as a callback handler in the JSONB input parsing pipeline. When the JSON parser encounters the beginning of an array ('[' character), this function is invoked to set up the internal parsing state for array processing. It pushes a WJB_BEGIN_ARRAY token onto the JSONB parse state stack, which signals the start of array parsing and prepares the parser for handling subsequent array elements.

The function operates within PostgreSQL's streaming JSON parser framework, working similarly to jsonb_in_object_start but specifically for array structures. It ensures that array parsing begins with the correct internal state initialization and proper nesting management.

## Parameters / Member Variables
- : A void pointer that is cast to JsonbInState*, containing the current parsing state and configuration

## Dependencies
- Functions called/Symbols referenced:
  - pushJsonbValue (pushes parsing tokens onto the state stack)
  - WJB_BEGIN_ARRAY (token constant indicating array start)
  - JSON_SUCCESS (return value indicating successful parsing)
  - JsonbInState (parsing state structure)
- Called from (representative examples):
  - jsonb_from_cstring (main JSONB input function)
  - datum_to_jsonb_internal (internal conversion function)

## Notes and Other Information
- This is a static function internal to the jsonb.c module
- Part of the callback-based JSON parsing architecture in PostgreSQL
- Works in conjunction with jsonb_in_array_end to bracket array parsing
- Returns JSON_SUCCESS to indicate successful processing of the array start token
- Handles the structural parsing of arrays independently from their element values
- Essential for maintaining proper nesting levels in complex JSON documents containing arrays