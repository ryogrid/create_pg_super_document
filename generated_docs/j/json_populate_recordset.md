# json_populate_recordset

## Location
[src/backend/utils/adt/jsonfuncs.c:3986-3992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3986-L3992)

## Overview
A PostgreSQL SQL function that populates a recordset from a JSON array using a prototype record to define the output structure.

## Definition
```c
Datum json_populate_recordset(PG_FUNCTION_ARGS)
```

## Detailed Description
The `json_populate_recordset` function is a wrapper that provides the SQL interface for populating recordsets from JSON arrays. Unlike `json_to_recordset`, this function takes a record argument that serves as a template for the output structure. It delegates the actual work to the `populate_recordset_worker` function with parameters indicating that it handles JSON data (not JSONB) and requires a record argument for output structure definition.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention structure containing function arguments and context

## Dependencies
- Functions called/Symbols referenced:
  - [populate_recordset_worker](../p/populate_recordset_worker.md)
- Called from (representative examples):
  - SQL queries using json_populate_recordset(record_type, json_array)

## Notes and Other Information
- Located at src/backend/utils/adt/jsonfuncs.c:3986-3992
- This is a thin wrapper that calls populate_recordset_worker with parameters (fcinfo, "json_populate_recordset", true, true)
- The `true, true` parameters indicate: is JSON (not JSONB), and has a record argument provided
- The record argument defines the structure and types of the output recordset
- Part of PostgreSQL`s JSON functionality for converting JSON arrays to relational data with explicit type definitions

## Simplified Source

```c
Datum json_populate_recordset(PG_FUNCTION_ARGS) {
    // Delegate to the main recordset population worker
    // Parameters: function context, function name, is_json=true, has_record_arg=true
    return populate_recordset_worker(fcinfo, "json_populate_recordset", true, true);
}
```

This function is a simple wrapper that:
1. Takes the standard PostgreSQL function arguments
2. Calls the main recordset worker with JSON format and record argument flags
3. Returns the populated recordset result