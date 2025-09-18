# json_agg_transfn

## Location
src/backend/utils/adt/json.c: 852 - 860

## Overview
The json_agg_transfn function serves as the standard transition function for PostgreSQL's json_agg aggregate, preserving null values in the output JSON array.

## Definition
```c
Datum json_agg_transfn(PG_FUNCTION_ARGS)
```

## Detailed Description
This function acts as a simple wrapper around json_agg_transfn_worker, providing the standard behavior for the json_agg aggregate function. It ensures that null input values are included in the resulting JSON array as JSON null values, maintaining the complete set of input data. The function is registered as the transition function for the json_agg aggregate in PostgreSQL's system catalogs.

## Parameters / Member Variables
- No explicit parameters (uses PG_FUNCTION_ARGS macro)
- Passes through all arguments to the worker function
- Sets absent_on_null to false for standard null-preserving behavior

## Dependencies
- Functions called/Symbols referenced:
  - [json_agg_transfn_worker](json_agg_transfn_worker.md) (with absent_on_null=false)
- Called from:
  - PostgreSQL aggregate execution engine (no direct internal callers)

## Notes and Other Information
- Part of the json_agg aggregate function implementation
- Differs from json_agg_strict_transfn by preserving null values
- Located in src/backend/utils/adt/json.c:852-860
- Registered in system catalogs as the transition function for json_agg
- Simple delegation pattern that configures the worker function for standard behavior