# countVariablesFromJsonb

## Location
src/backend/utils/adt/jsonpath_exec.c: 3203 - 3224

## Overview
A callback function that counts and validates variables stored in a JSONB object for JSON path execution contexts.

## Definition
static int countVariablesFromJsonb(void *varsJsonb)

## Detailed Description
This function serves as a JsonPathCountVarsCallback implementation for JSONB-based variable storage. It validates that the provided JSONB value is actually an object (key-value pairs) rather than an array or scalar value, as variables must be stored as object properties. If validation fails, it reports an appropriate error. When successful, it returns the count of base objects (1 if variables exist, 0 if no variables are provided). This function ensures that variable parameters are properly formatted before JSON path execution begins.

## Parameters / Member Variables
- varsJsonb: Void pointer to the JSONB object containing the variables (cast to Jsonb internally)

## Dependencies
- Functions called/Symbols referenced:
  - JsonContainerIsObject (checks if JSONB container is an object type)
  - ereport (PostgreSQL error reporting)
  - ERRCODE_INVALID_PARAMETER_VALUE (error code constant)
- Called from (representative examples):
  - [jsonb_path_exists_internal](../j/jsonb_path_exists_internal.md)
  - [jsonb_path_match_internal](../j/jsonb_path_match_internal.md)
  - [jsonb_path_query_internal](../j/jsonb_path_query_internal.md)
  - [jsonb_path_query_array_internal](../j/jsonb_path_query_array_internal.md)
  - [jsonb_path_query_first_internal](../j/jsonb_path_query_first_internal.md)

## Notes and Other Information
- This is a static callback function, only accessible within the jsonpath_exec.c module
- Implements the JsonPathCountVarsCallback interface for JSONB-based variable storage
- Performs validation to ensure variables are stored as JSON object (not array or scalar)
- Returns 1 if variables are provided (indicating one base object), 0 if no variables
- Will throw ERRCODE_INVALID_PARAMETER_VALUE error if varsJsonb is not a JSON object
- Used during JSON path execution setup to validate and count variable sources
- Part of PostgreSQL's JSON path variable validation infrastructure