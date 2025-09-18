# jsonb_agg_transfn

## Location
[src/backend/utils/adt/jsonb.c:1625-1633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1625-L1633)

## Overview
Transition function for the jsonb_agg aggregate that accumulates values into a JSONB array, including null values.

## Definition


## Detailed Description
The  function serves as the transition function for PostgreSQL's  aggregate function. It acts as a simple wrapper around , passing  for the  parameter. This means that null input values will be included in the resulting JSONB array as JSON null values, rather than being omitted. The function handles the standard behavior of jsonb_agg where all input values (including nulls) are collected into a JSON array.

## Parameters / Member Variables
- Function arguments via : Standard PostgreSQL function argument structure containing aggregate state and input value

## Dependencies
- Functions called/Symbols referenced:
  -  - Core worker function that implements the aggregation logic
- Called from:
  - PostgreSQL aggregate framework during jsonb_agg aggregate execution

## Notes and Other Information
- Simple wrapper function that delegates to the worker implementation
- Passes  for absent_on_null, meaning null values are included in the result
- Part of PostgreSQL's aggregate function infrastructure for JSONB operations
- Counterpart to jsonb_agg_strict_transfn which omits null values
- Used internally by the PostgreSQL query executor during aggregate processing