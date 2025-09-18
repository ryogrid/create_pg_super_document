# jsonb_object_agg_strict_transfn

## Location
[src/backend/utils/adt/jsonb.c:1906-1914](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1906-L1914)

## Overview
Transition function for strict JSONB object aggregation that excludes key-value pairs where the value is NULL.

## Definition


## Detailed Description
This function serves as the transition function for the  aggregate function, which collects key-value pairs into a JSONB object while excluding pairs where the value is NULL. It acts as a wrapper around , passing  for  and  for  parameters. This means NULL values are skipped during aggregation, but duplicate keys are still allowed (later non-NULL values overwrite earlier ones). The function is called once for each input row during aggregate processing.

## Parameters / Member Variables
- : Function call information structure containing the aggregate state and input key/value pair

## Dependencies
- Functions called/Symbols referenced:
  - [jsonb_object_agg_transfn_worker](jsonb_object_agg_transfn_worker.md)
- Called from (representative examples):
  - PostgreSQL aggregate framework (no direct code references found)

## Notes and Other Information
- This function cannot be called directly due to its internal-type argument requirement
- The function delegates all processing to  with 
- Skips key-value pairs where the value is NULL
- Does not enforce key uniqueness (duplicate keys with non-NULL values result in value overwriting)
- Part of PostgreSQL's JSONB aggregate function family located in src/backend/utils/adt/jsonb.c:1906-1914