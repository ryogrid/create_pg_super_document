# json_array_length

## Location
src/backend/utils/adt/jsonfuncs.c: 1850 - 1875

## Overview
A SQL function that returns the number of elements in a JSON array as an integer.

## Definition
```c
Datum json_array_length(PG_FUNCTION_ARGS)
```

## Detailed Description
The `json_array_length` function implements the SQL function `json_array_length(json) -> int` which counts the number of elements in a JSON array. It uses the PostgreSQL JSON parser with custom semantic action callbacks to count array elements at the top level. The function sets up an AlenState structure to track the count and configures JsonSemAction callbacks that validate the input is an array (not an object or scalar) and increment the count for each array element encountered.

## Parameters / Member Variables
- Takes a single argument via PG_FUNCTION_ARGS:
  - `json`: A text value containing the JSON array to measure

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - palloc0
  - makeJsonLexContext
  - pg_parse_json_or_ereport
  - PG_RETURN_INT32
  - alen_object_start (callback function)
  - alen_scalar (callback function)
  - alen_array_element_start (callback function)
- Types used:
  - AlenState
  - JsonLexContext
  - JsonSemAction
- Called from:
  - No direct callers found (SQL-callable function)

## Notes and Other Information
- This is a SQL-callable function exposed to PostgreSQL users
- The function validates that the input JSON is an array by using callback functions that error on objects or scalars
- Uses the AlenState structure which contains a JsonLexContext pointer and an integer count
- The alen_array_element_start callback increments the count for each array element
- Returns the total count as a 32-bit integer
- Part of PostgreSQL's JSON functionality for analyzing JSON array structures
- Related to the summary symbols alen_array_element_start, alen_object_start, and alen_scalar which provide the parsing callbacks