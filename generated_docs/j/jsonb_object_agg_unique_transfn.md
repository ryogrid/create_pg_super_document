# jsonb_object_agg_unique_transfn

## Location
[src/backend/utils/adt/jsonb.c:1915-1923](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1915-L1923)

## Overview
Transition function for the jsonb_object_agg_unique aggregate that builds JSONB objects from key-value pairs while enforcing key uniqueness constraints.

## Definition

```c
Datum
jsonb_object_agg_unique_transfn(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the transition function for the jsonb_object_agg_unique aggregate operation in PostgreSQL. It acts as a thin wrapper around the core jsonb_object_agg_transfn_worker function, specifically configured to handle unique key aggregation. The function delegates all the actual work to jsonb_object_agg_transfn_worker with parameters that enforce both null handling (false for nulls) and unique key constraints (true for unique).

The function is part of PostgreSQL's JSONB aggregate functionality, which allows users to build JSONB objects from rows of data while ensuring that duplicate keys are handled according to the unique constraint policy.

## Parameters / Member Variables
- Parameters are handled internally by the worker function:

## Dependencies
- Functions called/Symbols referenced:
  - [jsonb_object_agg_transfn_worker](jsonb_object_agg_transfn_worker.md)
- Called from (representative examples):
  - PostgreSQL aggregate execution engine during jsonb_object_agg_unique operations

## Notes and Other Information
- This is a wrapper function that provides a specific configuration of the general jsonb_object_agg_transfn_worker
- The function enforces unique key constraints, meaning duplicate keys will be handled according to the unique policy
- Located in src/backend/utils/adt/jsonb.c:1915-1923
- Part of PostgreSQL's extensive JSONB manipulation and aggregation capabilities