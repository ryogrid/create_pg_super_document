# json_array_elements

## Location
src/backend/utils/adt/jsonfuncs.c: 2294 - 2299

## Overview
A PostgreSQL SQL-callable function that extracts elements from a JSON array and returns them as a set of JSON values.

## Definition


## Detailed Description
This function serves as the main entry point for the json_array_elements SQL function in PostgreSQL. It is a thin wrapper around the elements_worker function, specifically configured to handle JSON (not JSONB) input and return JSON values rather than text. The function processes JSON arrays by delegating the actual work to elements_worker with appropriate parameters to maintain JSON format in the output.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro which provides access to function arguments through the fcinfo structure
- No explicit parameters - arguments are accessed via PostgreSQL's function call interface

## Dependencies
- Functions called/Symbols referenced:
  - elements_worker: Core implementation function for JSON array element extraction
- Called from:
  - SQL queries using the json_array_elements() function
  - PostgreSQL's function call infrastructure

## Notes and Other Information
- Part of PostgreSQL's JSON function suite alongside json_array_elements_text
- Returns JSON values, not text representations
- Designed to work with JSON data type (as opposed to JSONB)
- Uses PostgreSQL's set-returning function (SRF) framework to return multiple rows
- The third parameter (false) to elements_worker indicates that text conversion should not be performed