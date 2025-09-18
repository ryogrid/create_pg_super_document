# pg_prepared_statement

## Location
src/backend/commands/prepare.c: 684 - 745

## Overview
A set-returning function that reads all prepared statements and returns detailed metadata about each statement including name, query text, preparation time, parameter types, and plan statistics.

## Definition
```c
Datum pg_prepared_statement(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a system view that provides introspection capabilities for prepared statements in the current session. It scans the prepared_queries hash table and returns a comprehensive set of information for each prepared statement:

1. **Statement Identification**: Statement name and original query text
2. **Timing Information**: When the statement was prepared
3. **Type Information**: Parameter types and result column types (if applicable)
4. **Execution Statistics**: Count of generic vs custom plans used
5. **Source Tracking**: Whether the statement was created from SQL

The function uses PostgreSQL's set-returning function infrastructure with materialized results, putting all tuples into a tuplestore in a single hash table scan to avoid concurrency issues. For statements without result descriptors (like DML statements), the result types field is set to NULL.

## Parameters / Member Variables
This function follows PostgreSQL's SRF (Set-Returning Function) convention:
- Takes `PG_FUNCTION_ARGS` which provides access to function call context
- Returns `Datum` (0 for SRFs)
- Uses `fcinfo->resultinfo` to access the ReturnSetInfo structure

## Dependencies
- Functions called/Symbols referenced:
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - CStringGetTextDatum
  - TimestampTzGetDatum
  - [build_regtype_array](../b/build_regtype_array.md)
  - palloc_array
  - [BoolGetDatum](../B/BoolGetDatum.md)
  - Int64GetDatumFast
  - tuplestore_putvalues
- Data structures used:
  - ReturnSetInfo
  - HASH_SEQ_STATUS
  - PreparedStatement
  - [TupleDesc](../T/TupleDesc.md)
  - prepared_queries (global hash table)
- Called from (representative examples):
  - System view queries (via SQL interface)

## Notes and Other Information
- Returns 8 columns: name, statement, prepare_time, param_types, result_types, from_sql, generic_plans, custom_plans
- Safely handles the case where no prepared statements exist (prepared_queries is NULL)
- Uses materialized SRF approach to avoid hash table changes during iteration
- Parameter and result types are returned as regtype arrays using build_regtype_array
- The result_types column is NULL for statements without result descriptors (e.g., INSERT, UPDATE, DELETE)
- [Plan](../P/Plan.md) statistics (generic_plans, custom_plans) provide insight into PostgreSQL's adaptive planning behavior
- The from_sql flag distinguishes between statements prepared via SQL PREPARE vs. protocol-level preparation
- This function is typically exposed through the pg_prepared_statements system view