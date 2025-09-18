# json_object_field

## Location
src/backend/utils/adt/jsonfuncs.c: 844 - 859

## Overview
Extracts the value associated with a specified field name from a JSON object, implementing the -> operator functionality.

## Definition


## Detailed Description
This function implements one of the core JSON getter operations in PostgreSQL, specifically the -> operator for JSON objects. It takes a JSON object and a field name as input parameters and returns the value associated with that field name. The function serves as a wrapper around the get_worker function, which performs the actual JSON parsing and value extraction.

The function handles the conversion of the field name from PostgreSQL's text type to a C string and delegates the extraction work to get_worker. If the specified field exists in the JSON object, its value is returned as text; otherwise, the function returns NULL.

This function is part of the JSON accessor function family that includes operators like ->, ->>, #>, and #>>, providing different ways to navigate and extract data from JSON structures.

## Parameters / Member Variables
- : JSON text input containing the JSON object to search
- : Text field name (key) to extract from the JSON object

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (for parameter extraction)
  - text_to_cstring (for text to C string conversion)
  - [get_worker](../g/get_worker.md) (for actual JSON processing)
  - PG_RETURN_TEXT_P (for returning text result)
  - PG_RETURN_NULL (for returning null result)
- Called from (representative examples):
  - No direct callers found in the codebase (likely called via SQL interface)

## Notes and Other Information
- Implements the -> operator functionality for JSON field extraction
- Returns the raw JSON value (including quotes for strings) rather than the unescaped value
- Part of the json{b?}_extract_path* family of functions
- Handles missing fields gracefully by returning NULL
- The get_worker function handles the complex JSON parsing and path navigation logic
- Typically exposed as a SQL function/operator for JSON processing in queries