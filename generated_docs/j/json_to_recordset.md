# json_to_recordset

## Location
[src/backend/utils/adt/jsonfuncs.c:3993-3999](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3993-L3999)

## Overview
A PostgreSQL SQL function that converts a JSON array of objects into a recordset, where each object becomes a row in the result set.

## Definition
```c
Datum json_to_recordset(PG_FUNCTION_ARGS)
```

## Detailed Description
The `json_to_recordset` function is a wrapper that provides the SQL interface for converting JSON arrays into recordsets. It delegates the actual work to the `populate_recordset_worker` function with specific parameters indicating that it handles JSON data (not JSONB) and does not take a record argument (the output record structure is inferred from the query context). This function is part of PostgreSQL`s JSON functionality that allows treating JSON arrays as relational data.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention structure containing function arguments and context

## Dependencies
- Functions called/Symbols referenced:
  - [populate_recordset_worker](../p/populate_recordset_worker.md)
- Called from (representative examples):
  - SQL queries using json_to_recordset()

## Notes and Other Information
- Located at src/backend/utils/adt/jsonfuncs.c:3993-3999
- This is a thin wrapper that calls populate_recordset_worker with parameters (fcinfo, "json_to_recordset", true, false)
- The `true, false` parameters indicate: is JSON (not JSONB), and no record argument provided
- The output record structure is inferred from the query context rather than from an explicit record argument
- Part of PostgreSQL`s JSON functionality for treating JSON data relationally without explicit type definitions