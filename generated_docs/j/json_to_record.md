# json_to_record

## Location
[src/backend/utils/adt/jsonfuncs.c:2500-2507](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2500-L2507)

## Overview
Converts a JSON object into a PostgreSQL record/row type, extracting fields that match the columns of the expected output record type.

## Definition

```c
Datum
json_to_record(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that transforms a JSON object into a record (row) type. This function is a thin wrapper around the  function, specifically configured for JSON input without requiring a record argument template. It extracts field values from the JSON object that correspond to the columns of the target record type, performing automatic type conversion as needed.

The function operates by:
1. Accepting a JSON object as input
2. Determining the expected output record type from the query context
3. Extracting matching fields from the JSON object
4. Converting and populating the output record structure

This function is the JSON equivalent of , handling text-based JSON input instead of binary JSONB format.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call information structure containing:
## Dependencies
- Functions called/Symbols referenced:
  -  (the main implementation function)
- Called from (representative examples):
  - SQL queries using  function calls
  - PostgreSQL function call infrastructure

## Notes and Other Information
- This function is part of PostgreSQL's JSON/JSONB support system
- The actual work is delegated to  with parameters:
  - : "json_to_record"
  - : true (indicating JSON input, not JSONB)
  - : false (no record template argument)
  - : NULL (no soft error context)
- Located in 
- Typically used in SQL contexts where the return type is explicitly specified
- Requires the output record type to be determinable from the calling context
- Differs from  in that it doesn't require an existing record template
- The JSON input is processed as text and parsed during execution, unlike JSONB which is pre-parsed