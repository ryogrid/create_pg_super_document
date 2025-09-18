# json_agg_strict_transfn

## Location
src/backend/utils/adt/json.c: 861 - 869

## Overview
The json_agg_strict_transfn function serves as the strict transition function for PostgreSQL's json_agg aggregate, excluding null values from the output JSON array.

## Definition
```c
Datum json_agg_strict_transfn(PG_FUNCTION_ARGS)
```

## Detailed Description
This function acts as a wrapper around json_agg_transfn_worker, providing the strict behavior variant for JSON aggregation. It ensures that null input values are completely omitted from the resulting JSON array, creating a more compact result that contains only non-null values. This behavior is useful when null values are not meaningful in the aggregate context and should be filtered out rather than represented as JSON null values.

## Parameters / Member Variables
- No explicit parameters (uses PG_FUNCTION_ARGS macro)
- Passes through all arguments to the worker function
- Sets absent_on_null to true for strict null-filtering behavior

## Dependencies
- Functions called/Symbols referenced:
  - json_agg_transfn_worker (with absent_on_null=true)
- Called from:
  - PostgreSQL aggregate execution engine (no direct internal callers)

## Notes and Other Information
- Part of the strict variant of json_agg aggregate function implementation
- Differs from json_agg_transfn by filtering out null values entirely
- Located in src/backend/utils/adt/json.c:861-869
- Registered in system catalogs as the transition function for the strict json_agg variant
- Simple delegation pattern that configures the worker function for strict null-filtering behavior