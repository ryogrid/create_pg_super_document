# jsonb_path_match_internal

## Location
src/backend/utils/adt/jsonpath_exec.c: 456 - 496

## Overview
Internal implementation function that evaluates a JSONPath predicate against a JSONB value and returns the boolean result, supporting both timezone-aware and timezone-naive operations.

## Definition


## Detailed Description
This function implements the core logic for JSONPath predicate matching in PostgreSQL. Unlike  which only checks for existence, this function evaluates JSONPath expressions that return boolean predicates and validates that exactly one boolean result is produced. The function is designed to support the "@@" operator and follows similar error handling philosophy as the existence functions.

The function executes the JSONPath expression and expects to receive exactly one result that is either a boolean value or null. If the result is not a single boolean or null value, the function either throws an error (in non-silent mode) or returns NULL (in silent mode). This strict validation ensures that predicate operations behave predictably and follow SQL/JSON semantics.

## Parameters / Member Variables
- : Function call information containing the arguments passed to the function
- : Boolean flag indicating whether to use timezone-aware JSONPath execution

Function arguments accessed through :
- Argument 0:  - The JSONB value to evaluate the predicate against
- Argument 1:  - The JSONPath predicate expression to execute
- Argument 2 (optional):  - Variables for JSONPath execution
- Argument 3 (optional):  - Whether to suppress errors during execution

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract JSONB argument from function call info
  -  - Extract JSONPath argument from function call info
  -  - Extract boolean argument from function call info
  -  - Get number of arguments passed to function
  -  - Core JSONPath execution engine
  -  - Variable resolver for JSONPath execution
  -  - Count variables in JSONB context
  -  - Get the count of results in the value list
  -  - Get the first result from the value list
  -  - Memory management for varlena types
  -  - Return boolean value
  -  - Return NULL value
  -  - PostgreSQL error reporting system
  -  - Error code specification
  -  - Error message specification

- Called from:
  -  (src/backend/utils/adt/jsonpath_exec.c:499)
  -  (src/backend/utils/adt/jsonpath_exec.c:505)
  -  (src/backend/utils/adt/jsonpath_exec.c:517)

## Notes and Other Information
- This function is marked as , limiting access to the same compilation unit
- The function performs strict result validation, requiring exactly one boolean or null result
- Uses  to collect results from JSONPath execution, unlike the existence functions
- Returns the actual boolean value when exactly one boolean result is found
- Returns NULL when exactly one null result is found or when validation fails in silent mode
- Throws a specific error () when validation fails in non-silent mode
- The error message "single boolean result is expected" clearly indicates the validation requirement
- Memory management includes proper cleanup of input JSONB and JSONPath arguments
- The function supports both 2-argument and 4-argument calling conventions
- Unlike existence checking, this function requires the JSONPath to produce a predicate (boolean) result
- Part of the implementation supporting the "@@" operator for JSONPath predicate matching