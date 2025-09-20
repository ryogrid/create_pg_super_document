# jsonb_agg_strict_transfn

## Location
[src/backend/utils/adt/jsonb.c:1634-1639](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1634-L1639)

## Overview
Transition function for the strict variant of JSONB array aggregation that excludes NULL values from the aggregated array.

## Definition

```c
Datum
jsonb_agg_strict_transfn(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the transition function for the  aggregate function, which collects input values into a JSONB array while excluding NULL values. It acts as a thin wrapper around , passing  for the  parameter to ensure NULL values are not included in the resulting array. This function is called once for each input row during aggregate processing.

## Parameters / Member Variables
- : Function call information structure containing the aggregate state and input value

## Dependencies
- Functions called/Symbols referenced:
  - [jsonb_agg_transfn_worker](jsonb_agg_transfn_worker.md)
- Called from (representative examples):
  - PostgreSQL aggregate framework (no direct code references found)

## Notes and Other Information
- This function cannot be called directly due to its internal-type argument requirement
- The function delegates all processing to  with 
- Part of PostgreSQL's JSONB aggregate function family located in src/backend/utils/adt/jsonb.c:1634-1639