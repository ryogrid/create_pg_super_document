# json_array_elements

## Location
[src/backend/utils/adt/jsonfuncs.c:2294-2299](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2294-L2299)

## Overview
A PostgreSQL SQL-callable function that extracts elements from a JSON array and returns them as a set of JSON values.

## Definition

```c
Datum
json_array_elements(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the main entry point for the json_array_elements SQL function in PostgreSQL. It is a thin wrapper around the elements_worker function, specifically configured to handle JSON (not JSONB) input and return JSON values rather than text. The function processes JSON arrays by delegating the actual work to elements_worker with appropriate parameters to maintain JSON format in the output.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [elements_worker](../e/elements_worker.md): Core implementation function for JSON array element extraction
- Called from:
  - SQL queries using the json_array_elements() function
  - PostgreSQL's function call infrastructure

## Notes and Other Information
- Part of PostgreSQL's JSON function suite alongside json_array_elements_text
- Returns JSON values, not text representations
- Designed to work with JSON data type (as opposed to JSONB)
- Uses PostgreSQL's set-returning function (SRF) framework to return multiple rows
- The third parameter (false) to elements_worker indicates that text conversion should not be performed

## Simplified Source

```c
Datum json_array_elements(PG_FUNCTION_ARGS) {
    // Simple wrapper that calls the main worker function
    // with text conversion disabled (false parameter)
    return elements_worker(fcinfo, "json_array_elements", false);
}
```