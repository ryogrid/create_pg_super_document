# jsonb_to_recordset

## Location
src/backend/utils/adt/jsonfuncs.c: 3979 - 3985

## Overview
A PostgreSQL SQL function that converts a JSONB array of objects into a recordset, where each object becomes a row in the result set.

## Definition
```c
Datum jsonb_to_recordset(PG_FUNCTION_ARGS)
```

## Detailed Description
The `jsonb_to_recordset` function is a wrapper that provides the SQL interface for converting JSONB arrays into recordsets. It delegates the actual work to the `populate_recordset_worker` function with specific parameters indicating that it handles JSONB data and does not take a record argument (the output record structure is inferred from the query context). This function is part of PostgreSQL`s JSON/JSONB functionality that allows treating JSON arrays as relational data.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention structure containing function arguments and context

## Dependencies
- Functions called/Symbols referenced:
  - [populate_recordset_worker](../p/populate_recordset_worker.md)
- Called from (representative examples):
  - SQL queries using jsonb_to_recordset()

## Notes and Other Information
- Located at src/backend/utils/adt/jsonfuncs.c:3979-3985
- This is a thin wrapper that calls populate_recordset_worker with parameters (fcinfo, "jsonb_to_recordset", false, false)
- The `false, false` parameters indicate: not JSON (is JSONB), and no record argument provided
- Part of PostgreSQL`s extensive JSON/JSONB support for treating JSON data relationally