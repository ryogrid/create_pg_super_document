# json_object_agg_strict_transfn

## Location
[src/backend/utils/adt/json.c:1150-1158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L1150-L1158)

## Overview
This function serves as the PostgreSQL SQL-callable wrapper for the strict variant of json_object_agg aggregate function, which excludes entries with null values from the resulting JSON object.

## Definition
```c
Datum json_object_agg_strict_transfn(PG_FUNCTION_ARGS)
```

## Detailed Description
The function is a thin wrapper around json_object_agg_transfn_worker that implements the strict json_object_agg aggregate function behavior. It delegates all actual work to the worker function with specific parameters:
- absent_on_null: true (excludes entries with null values)
- unique_keys: false (allows duplicate keys)

This represents the "strict" variant of the SQL json_object_agg function that filters out key-value pairs where the value is NULL, resulting in a JSON object that only contains entries with non-null values. This follows SQL standard behavior for "ABSENT ON NULL" semantics.

## Parameters / Member Variables
- Uses PostgreSQL's standard PG_FUNCTION_ARGS macro which expands to FunctionCallInfo fcinfo

## Dependencies
- Functions called/Symbols referenced:
  - [json_object_agg_transfn_worker](json_object_agg_transfn_worker.md)
- Called from (representative examples):
  - This function is typically registered in PostgreSQL's system catalogs and called by the SQL executor during aggregate processing

## Notes and Other Information
- This is a public function (non-static) designed to be called from PostgreSQL's function manager
- Implements the strict json_object_agg SQL aggregate function with ABSENT ON NULL behavior
- Part of PostgreSQL's SQL standard JSON functionality
- Uses PG_FUNCTION_ARGS calling convention for SQL-callable functions
- Returns Datum as required by PostgreSQL's function interface
- Allows duplicate keys but excludes entries with null values from the resulting JSON object
- Provides SQL standard compliant "ABSENT ON NULL" semantics for JSON object aggregation