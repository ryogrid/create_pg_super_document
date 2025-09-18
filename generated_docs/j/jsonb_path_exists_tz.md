# jsonb_path_exists_tz

## Location
src/backend/utils/adt/jsonpath_exec.c: 433 - 443

## Overview
Public PostgreSQL function that checks whether a JSONPath expression returns at least one item for a given JSONB value, with timezone awareness enabled.

## Definition


## Detailed Description
This function serves as the timezone-aware version of JSONPath existence checking in PostgreSQL. It is a wrapper around  that enables timezone-aware JSONPath execution. The function is designed to handle JSONPath expressions that involve datetime operations and timezone conversions.

This function is particularly important when working with JSONB documents that contain timestamp or datetime values, as it ensures that timezone information is properly considered during JSONPath evaluation. Like its timezone-naive counterpart, it implements the functionality behind JSON existence checking operations but with full timezone support.

## Parameters / Member Variables
The function uses PostgreSQL's standard function argument mechanism:
- Arguments are accessed through the  parameter passed to the internal function
- Argument 0: JSONB document to search within
- Argument 1: JSONPath expression to evaluate
- Argument 2 (optional): JSONB object containing variables for the JSONPath expression
- Argument 3 (optional): Boolean flag for silent mode operation

## Dependencies
- Functions called/Symbols referenced:
  -  - The internal implementation function with timezone support enabled

- Called from:
  - This is a public PostgreSQL function, typically called from SQL queries or the function manager system
  - No direct C function references found in the codebase

## Notes and Other Information
- This function is marked as a public -returning function, making it accessible as a PostgreSQL built-in function
- The function passes  as the timezone parameter to , enabling timezone-aware JSONPath processing
- Essential for applications that need to perform JSONPath queries on JSONB data containing temporal information
- Part of PostgreSQL's comprehensive JSONPath functionality that supports SQL/JSON standard operations with timezone handling
- The timezone-aware functionality is crucial for applications dealing with distributed systems where timezone handling is critical
- All actual processing is delegated to  with timezone support explicitly enabled
- Complements the timezone-naive  function by providing the timezone-aware alternative