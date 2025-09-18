# percentile_cont_interval_final

## Location
src/backend/utils/adt/orderedsetaggs.c: 622 - 633

## Overview
PostgreSQL function implementing the PERCENTILE_CONT ordered-set aggregate for interval data types with continuous percentile calculation and interpolation.

## Definition
```c
Datum percentile_cont_interval_final(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the final computation step for the PERCENTILE_CONT aggregate when operating on interval data types. It is a thin wrapper around the common percentile_cont_final_common function, providing the interval-specific type validation (INTERVALOID) and interpolation function (interval_lerp).

The function implements the SQL standard PERCENTILE_CONT aggregate for temporal interval data, which computes continuous percentiles by potentially interpolating between adjacent interval values in the sorted dataset. The interpolation of interval values is more complex than numeric types due to the multi-component nature of intervals (years, months, days, hours, minutes, seconds, microseconds).

This function is typically called by PostgreSQL's aggregate execution framework as the final step after all input interval values have been collected and prepared for processing.

## Parameters / Member Variables
- Uses the standard PG_FUNCTION_ARGS macro which provides access to:
  - Aggregate state (OSAPerGroupState)  
  - Percentile value (float8)
  - Function call context information

## Dependencies
- Functions called/Symbols referenced:
  - [percentile_cont_final_common](percentile_cont_final_common.md) (core percentile calculation logic)
  - [interval_lerp](../i/interval_lerp.md) (interval-specific linear interpolation function)
  - INTERVALOID (type validation constant)
- Called from (representative examples):
  - PostgreSQL aggregate execution framework (no direct C references shown)

## Notes and Other Information
- This is a public PostgreSQL function exposed to the SQL layer
- Part of PostgreSQL's implementation of SQL standard ordered-set aggregate functions
- The function signature follows PostgreSQL's version-1 calling convention (PG_FUNCTION_ARGS)
- Used specifically for: `PERCENTILE_CONT(percentile_value) WITHIN GROUP (ORDER BY interval_expression)`
- The actual percentile computation and complex interval interpolation logic is delegated to the common implementation and interval_lerp
- This function must be registered in PostgreSQL's system catalogs to be callable from SQL
- Supports continuous percentiles for temporal data, enabling sophisticated time-based analytics
- Handles all interval components properly through the specialized interval_lerp interpolation function