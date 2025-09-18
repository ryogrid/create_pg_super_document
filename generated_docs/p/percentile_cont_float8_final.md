# percentile_cont_float8_final

## Location
[src/backend/utils/adt/orderedsetaggs.c:613-621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L613-L621)

## Overview
PostgreSQL function implementing the PERCENTILE_CONT ordered-set aggregate for float8 (double precision) data types with continuous percentile calculation.

## Definition
```c
Datum percentile_cont_float8_final(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the final computation step for the PERCENTILE_CONT aggregate when operating on float8 (double precision) data types. It is a thin wrapper around the common percentile_cont_final_common function, providing the float8-specific type validation (FLOAT8OID) and interpolation function (float8_lerp).

The function implements the SQL standard PERCENTILE_CONT aggregate, which computes continuous percentiles by potentially interpolating between adjacent values in the sorted dataset. For float8 data, this interpolation is performed using standard arithmetic operations.

This function is typically called by PostgreSQL's aggregate execution framework as the final step after all input values have been collected and prepared for processing.

## Parameters / Member Variables
- Uses the standard PG_FUNCTION_ARGS macro which provides access to:
  - Aggregate state (OSAPerGroupState)
  - Percentile value (float8)
  - Function call context information

## Dependencies
- Functions called/Symbols referenced:
  - [percentile_cont_final_common](percentile_cont_final_common.md) (core percentile calculation logic)
  - [float8_lerp](../f/float8_lerp.md) (float8-specific linear interpolation function)
  - FLOAT8OID (type validation constant)
- Called from (representative examples):
  - PostgreSQL aggregate execution framework (no direct C references shown)

## Notes and Other Information
- This is a public PostgreSQL function exposed to the SQL layer
- Part of PostgreSQL's implementation of SQL standard ordered-set aggregate functions
- The function signature follows PostgreSQL's version-1 calling convention (PG_FUNCTION_ARGS)
- Used specifically for: `PERCENTILE_CONT(percentile_value) WITHIN GROUP (ORDER BY float8_expression)`
- The actual percentile computation and interpolation logic is delegated to the common implementation
- This function must be registered in PostgreSQL's system catalogs to be callable from SQL
- Supports continuous percentiles, meaning results may be interpolated values not present in the original dataset