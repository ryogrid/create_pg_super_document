# jsonb_to_record

## Location
[src/backend/utils/adt/jsonfuncs.c:2486-2492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2486-L2492)

## Overview
Converts a JSONB object into a PostgreSQL record/row type, extracting fields that match the columns of the expected output record type.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that transforms a JSONB object into a record (row) type. This function is a thin wrapper around the  function, specifically configured for JSONB input without requiring a record argument template. It extracts field values from the JSONB object that correspond to the columns of the target record type, performing automatic type conversion as needed.

The function operates by:
1. Accepting a JSONB object as input
2. Determining the expected output record type from the query context
3. Extracting matching fields from the JSONB object
4. Converting and populating the output record structure

## Parameters / Member Variables
- : Standard PostgreSQL function call information structure containing:
  - JSONB input argument
  - Function context and metadata
  - Output type information

## Dependencies
- Functions called/Symbols referenced:
  -  (the main implementation function)
- Called from (representative examples):
  - SQL queries using  function calls
  - PostgreSQL function call infrastructure

## Notes and Other Information
- This function is part of PostgreSQL's JSON/JSONB support system
- The actual work is delegated to  with parameters:
  - : "jsonb_to_record"
  - : false (indicating JSONB input)
  - : false (no record template argument)
  - : NULL (no soft error context)
- Located in 
- Typically used in SQL contexts where the return type is explicitly specified
- Requires the output record type to be determinable from the calling context