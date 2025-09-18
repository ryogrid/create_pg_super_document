# json_array_elements_text

## Location
src/backend/utils/adt/jsonfuncs.c: 2300 - 2305

## Overview
A PostgreSQL SQL-callable function that extracts elements from a JSON array and returns them as a set of text values.

## Definition


## Detailed Description
This function serves as the main entry point for the json_array_elements_text SQL function in PostgreSQL. It is a thin wrapper around the elements_worker function, specifically configured to handle JSON input and return text representations of the array elements rather than JSON values. The function processes JSON arrays by delegating to elements_worker with the text conversion flag set to true, ensuring all output values are converted to their string representations.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro which provides access to function arguments through the fcinfo structure
- No explicit parameters - arguments are accessed via PostgreSQL's function call interface

## Dependencies
- Functions called/Symbols referenced:
  - elements_worker: Core implementation function for JSON array element extraction
- Called from:
  - SQL queries using the json_array_elements_text() function
  - PostgreSQL's function call infrastructure

## Notes and Other Information
- Part of PostgreSQL's JSON function suite alongside json_array_elements
- Returns text values, not JSON representations
- Designed to work with JSON data type (as opposed to JSONB)
- The third parameter (true) to elements_worker indicates that text conversion should be performed
- Useful when you need string representations of JSON array elements for text processing operations
- Uses PostgreSQL's set-returning function (SRF) framework to return multiple rows