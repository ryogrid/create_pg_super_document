# jsonb_object_agg_unique_strict_transfn

## Location
src/backend/utils/adt/jsonb.c: 1924 - 1929

## Overview
Strict transition function for the jsonb_object_agg_unique aggregate that builds JSONB objects from key-value pairs while enforcing key uniqueness and strict null handling.

## Definition
```c
Datum jsonb_object_agg_unique_strict_transfn(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the strict transition function for the jsonb_object_agg_unique aggregate operation in PostgreSQL. It acts as a thin wrapper around the core jsonb_object_agg_transfn_worker function, specifically configured to handle unique key aggregation with strict null handling. The function delegates all the actual work to jsonb_object_agg_transfn_worker with parameters that enforce both strict null handling (true for nulls) and unique key constraints (true for unique).

The "strict" variant means that NULL values in either keys or values will cause the entire aggregate to return NULL, following PostgreSQL's strict function semantics. This provides more stringent behavior compared to the non-strict variant.

## Parameters / Member Variables
- Uses the standard PostgreSQL function call interface (PG_FUNCTION_ARGS)
- Parameters are handled internally by the worker function:
  - Aggregate state (first call vs. subsequent calls)
  - Key value (text or other type convertible to text)
  - Value to be aggregated into the JSONB object

## Dependencies
- Functions called/Symbols referenced:
  - jsonb_object_agg_transfn_worker
- Called from (representative examples):
  - PostgreSQL aggregate execution engine during jsonb_object_agg_unique_strict operations

## Notes and Other Information
- This is a wrapper function that provides a specific strict configuration of the general jsonb_object_agg_transfn_worker
- The function enforces both unique key constraints and strict null handling
- Strict semantics mean NULL inputs will cause the aggregate to return NULL
- Located in src/backend/utils/adt/jsonb.c:1924-1929
- Part of PostgreSQL's extensive JSONB manipulation and aggregation capabilities with enhanced null safety