# json_object_agg_transfn

## Location
src/backend/utils/adt/json.c: 1141 - 1149

## Overview
This function serves as the PostgreSQL SQL-callable wrapper for the basic json_object_agg aggregate function, providing the standard behavior without unique key enforcement or null value filtering.

## Definition
```c
Datum json_object_agg_transfn(PG_FUNCTION_ARGS)
```

## Detailed Description
The function is a thin wrapper around json_object_agg_transfn_worker that implements the basic json_object_agg aggregate function behavior. It delegates all actual work to the worker function with default parameters:
- absent_on_null: false (includes entries with null values)
- unique_keys: false (allows duplicate keys)

This represents the standard SQL json_object_agg function that accumulates key-value pairs into a JSON object without any special handling for duplicates or nulls. It follows PostgreSQL's function calling convention using PG_FUNCTION_ARGS and returns a Datum.

## Parameters / Member Variables
- Uses PostgreSQL's standard PG_FUNCTION_ARGS macro which expands to FunctionCallInfo fcinfo

## Dependencies
- Functions called/Symbols referenced:
  - json_object_agg_transfn_worker
- Called from (representative examples):
  - This function is typically registered in PostgreSQL's system catalogs and called by the SQL executor during aggregate processing

## Notes and Other Information
- This is a public function (non-static) designed to be called from PostgreSQL's function manager
- Implements the basic json_object_agg SQL aggregate function
- Part of PostgreSQL's SQL standard JSON functionality
- Uses PG_FUNCTION_ARGS calling convention for SQL-callable functions
- Returns Datum as required by PostgreSQL's function interface
- Allows duplicate keys and preserves null values in the resulting JSON object