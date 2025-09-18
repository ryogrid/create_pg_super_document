# jsonb_build_object_worker

## Location
src/backend/utils/adt/jsonb.c: 1125 - 1176

## Overview
A worker function that constructs a JSONB object from alternating key-value pairs, with support for null handling and key uniqueness validation.

## Definition


## Detailed Description
The jsonb_build_object_worker function is the core implementation for building JSONB objects from a sequence of alternating key-value pairs. It validates that the argument count is even (since keys and values must be paired), initializes a JsonbInState for building the object, and processes each key-value pair while applying the specified null handling and uniqueness policies. The function enforces that keys cannot be null, but provides flexible handling of null values based on the absent_on_null parameter. When unique_keys is enabled, duplicate keys are detected and handled appropriately.

## Parameters / Member Variables
- : Total number of arguments (must be even for key-value pairs)
- : Array of Datum values representing alternating keys and values
- : Array of boolean flags indicating which arguments are NULL
- : Array of PostgreSQL type OIDs for each argument
- : Boolean flag to skip key-value pairs when the value is NULL
- : Boolean flag to enforce key uniqueness in the resulting object

## Dependencies
- Functions called/Symbols referenced:
  - pushJsonbValue
  - add_jsonb
  - JsonbValueToJsonb
  - JsonbPGetDatum
  - JsonbInState
  - WJB_BEGIN_OBJECT, WJB_END_OBJECT
- Called from (representative examples):
  - jsonb_build_object
  - ExecEvalJsonConstructor
  - PG_RETURN_JSONB_P

## Notes and Other Information
- Validates that the number of arguments is even (key-value pairs)
- Enforces that keys cannot be NULL - raises an error if a key is NULL
- Supports absent_on_null mode where NULL values are omitted from the result
- When unique_keys is enabled, processes all keys even for skipped entries to enable uniqueness checking
- Uses the add_jsonb helper function to convert and add each key-value pair
- The function is central to PostgreSQL's jsonb_build_object() SQL function and JSON constructor expressions
- Handles complex logic around null value processing and key uniqueness validation