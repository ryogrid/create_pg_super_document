# jsonb_array_elements

## Location
[src/backend/utils/adt/jsonfuncs.c:2206-2211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2206-L2211)

## Overview
A PostgreSQL SQL function that extracts all elements from a JSONB array and returns them as a set of rows, with each row containing one JSONB element.

## Definition

```c
Datum
jsonb_array_elements(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the SQL function  which decomposes a JSONB array into its constituent elements. Each element of the input JSONB array is returned as a separate row containing the element as a JSONB value. The function is designed to work with PostgreSQL's set-returning function infrastructure, allowing it to be used in FROM clauses and other contexts where multiple rows are expected.

The function serves as a thin wrapper around the  function, which performs the actual array element extraction and iteration logic. It specifically handles JSONB data (binary JSON format) as opposed to textual JSON.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call information structure containing the JSONB array input parameter
## Dependencies
- Functions called/Symbols referenced:
  - [elements_worker_jsonb](../e/elements_worker_jsonb.md) (core worker function that extracts array elements from JSONB)
  - PG_FUNCTION_ARGS (PostgreSQL function calling convention macro)
  - Datum (PostgreSQL data type for function return values)
- Called from:
  - No direct references found (likely called via SQL function call mechanism)

## Notes and Other Information
- Part of PostgreSQL's JSONB manipulation functions available in SQL
- Returns elements as JSONB values (preserves original data types and structure)
- Similar processing logic to json_each* functions but focused on array element extraction
- Designed for use in SQL queries where JSONB array expansion is needed
- The function name is registered in PostgreSQL's system catalogs to make it available as a SQL function
- Works specifically with JSONB format, which provides better performance than text-based JSON processing

## Simplified Source
```c
/*
 * SQL functions json_array_elements and json_array_elements_text
 *
 * get the elements from a json array
 *
 * a lot of this processing is similar to the json_each* functions
 */

Datum jsonb_array_elements(PG_FUNCTION_ARGS) {
    // Extract all elements from JSONB array as set of JSONB rows
    // Returns elements as JSONB (not text)
    return elements_worker_jsonb(fcinfo, "jsonb_array_elements", false);
}
```