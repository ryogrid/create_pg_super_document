# jsonb_populate_record_valid

## Location
src/backend/utils/adt/jsonfuncs.c: 2475 - 2485

## Overview
This SQL function tests whether a JSONB object can successfully populate a PostgreSQL record without raising errors, returning a boolean result indicating validation success or failure.

## Definition


## Detailed Description
The  function implements the SQL function  which serves as a validation wrapper around the core  functionality. Instead of returning the populated record or raising an error on invalid input, this function returns a boolean value indicating whether the population operation would succeed.

The function works by calling the same underlying  function used by , but with an  that captures any errors that occur during processing instead of propagating them to the caller. After the population attempt, it examines whether any errors occurred and returns the inverse of that result - true if no errors occurred (valid), false if errors were encountered (invalid).

This function is particularly useful for data validation scenarios where you need to test whether JSONB data can be safely converted to a specific record type before performing the actual conversion, allowing for error handling and data quality checks without exception handling.

## Parameters / Member Variables
- Function uses  macro to access SQL function arguments:
  - : Base record type to test population against
  - : JSONB object to validate for population

## Dependencies
- Functions called/Symbols referenced:
  - ErrorSaveContext (error capture context structure)
  - populate_record_worker (core implementation function)
  - BoolGetDatum (PostgreSQL boolean conversion macro)

- Called from (representative examples):
  - Direct SQL function calls
  - No internal PostgreSQL references found

## Notes and Other Information
- Designed specifically for testing and validation purposes
- Uses ErrorSaveContext to capture errors without propagation
- Returns boolean result instead of populated record or error
- Shares the same core logic as jsonb_populate_record via populate_record_worker
- Useful for data quality validation and error prevention
- Part of PostgreSQL's JSONB validation infrastructure
- Callable from SQL as 
- Essential for applications requiring pre-validation of JSONB data before processing