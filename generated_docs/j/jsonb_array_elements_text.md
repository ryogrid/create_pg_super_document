# jsonb_array_elements_text

## Location
src/backend/utils/adt/jsonfuncs.c: 2212 - 2217

## Overview
A PostgreSQL SQL function that extracts all elements from a JSONB array and returns them as a set of rows, with each row containing one element converted to text format.

## Definition


## Detailed Description
This function implements the SQL function  which decomposes a JSONB array into its constituent elements and converts each element to text representation. Unlike  which returns elements as JSONB values, this function converts each element to PostgreSQL's text data type. This is particularly useful when you need the array elements as strings for further text processing or when the consuming code expects text rather than JSONB.

The function serves as a thin wrapper around the  function with the text conversion flag set to true, which performs the actual array element extraction and text conversion logic.

## Parameters / Member Variables
- : Standard PostgreSQL function call information structure containing the JSONB array input parameter

## Dependencies
- Functions called/Symbols referenced:
  - elements_worker_jsonb (core worker function that extracts array elements from JSONB and converts to text)
  - PG_FUNCTION_ARGS (PostgreSQL function calling convention macro)
  - Datum (PostgreSQL data type for function return values)
- Called from:
  - No direct references found (likely called via SQL function call mechanism)

## Notes and Other Information
- Part of PostgreSQL's JSONB manipulation functions available in SQL
- Returns elements as PostgreSQL text values (converted from original JSONB format)
- Companion function to jsonb_array_elements() but with text output instead of JSONB output
- Similar processing logic to json_each* functions but focused on array element extraction with text conversion
- The function name is registered in PostgreSQL's system catalogs to make it available as a SQL function
- Useful when you need array elements as strings for text processing operations
- The third parameter (true) to elements_worker_jsonb enables text conversion mode