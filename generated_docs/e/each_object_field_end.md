# each_object_field_end

## Location
src/backend/utils/adt/jsonfuncs.c: 2118 - 2165

## Overview
A callback function used during JSON parsing that handles the end of each object field, extracting field names and values to populate a tuple store for PostgreSQL's JSON object expansion functions.

## Definition


## Detailed Description
This function is a callback used by PostgreSQL's JSON parser to process the completion of each object field during JSON object expansion operations. It operates at the top level of JSON objects (lex_level == 1) and creates tuples containing field names and their corresponding values, storing them in a tuple store for later retrieval. The function handles both scalar values and complex JSON structures, with special processing for normalized results and null values.

The function switches to a temporary memory context for tuple creation, ensuring proper memory management and cleanup after each field is processed.

## Parameters / Member Variables
- : Pointer to an EachState structure containing parser state and configuration
- : C string containing the field name from the JSON object
- : Boolean indicating whether the field value is null

## Dependencies
- Functions called/Symbols referenced:
  - EachState (state structure)
  - JSON_SUCCESS (return value constant)
  - CStringGetTextDatum (converts C string to PostgreSQL text datum)
  - cstring_to_text_with_len (converts C string with specified length to text)
  - heap_form_tuple (creates a PostgreSQL heap tuple)
  - tuplestore_puttuple (stores tuple in tuple store)
  - MemoryContextSwitchTo (switches memory contexts)
  - MemoryContextReset (resets temporary memory context)
- Called from:
  - each_worker (main JSON expansion worker function)
  - JsObjectFree (JSON object callback structure)

## Notes and Other Information
- Only processes fields at the top level of JSON objects (lex_level == 1), skipping nested objects
- Uses temporary memory context for tuple creation with automatic cleanup
- Supports normalized scalar results and handles null values appropriately
- Part of PostgreSQL's JSON expansion infrastructure for functions like json_each() and jsonb_each()
- Returns JSON_SUCCESS on successful completion