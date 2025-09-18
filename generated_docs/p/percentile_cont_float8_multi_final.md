# percentile_cont_float8_multi_final

## Location
[src/backend/utils/adt/orderedsetaggs.c:1004-1018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L1004-L1018)

## Overview  
The final aggregate function that computes continuous percentiles for float8 (double precision) data types when multiple percentile values are requested simultaneously.

## Definition
```c
Datum percentile_cont_float8_multi_final(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a type-specific wrapper for computing continuous percentiles on float8 (double precision floating point) data. It delegates the main processing logic to percentile_cont_multi_final_common while providing the necessary type-specific parameters for proper handling of float8 values.

The function handles arrays of percentile values and returns interpolated results using linear interpolation between adjacent sorted values. Unlike discrete percentiles which return exact data values, continuous percentiles can return values that don't exist in the original dataset by interpolating between neighboring rows.

This is part of PostgreSQL's implementation of the SQL standard percentile_cont aggregate function, specifically optimized for double precision floating point arithmetic with proper IEEE 754 floating point interpolation semantics.

## Parameters / Member Variables  
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: OSAPerGroupState pointer (aggregate state)
  - Argument 1: ArrayType pointer (array of percentile values as float8[])

## Dependencies
- Functions called/Symbols referenced:
  - [percentile_cont_multi_final_common](percentile_cont_multi_final_common.md) (core implementation)
  - FLOAT8OID (type identifier constant)  
  - FLOAT8PASSBYVAL (pass-by-value flag for float8 type)
  - TYPALIGN_DOUBLE (alignment constant for double precision)
  - [float8_lerp](../f/float8_lerp.md) (linear interpolation function for float8)
- Called from (representative examples):
  - PostgreSQL aggregate execution framework (no direct code references found)

## Notes and Other Information
- This is a PostgreSQL aggregate final function, invoked automatically during aggregate processing
- Provides type-specific parameters to the common percentile implementation:
  - Type OID: FLOAT8OID for double precision identification
  - Size: sizeof(float8) for memory allocation  
  - Pass by value: FLOAT8PASSBYVAL (platform-dependent, typically true on 64-bit systems)
  - Alignment: TYPALIGN_DOUBLE for proper memory alignment
  - Interpolation: float8_lerp for IEEE 754 compliant linear interpolation
- Part of PostgreSQL's ordered-set aggregate framework implementing SQL standard statistical functions
- Handles all edge cases through the common implementation including NULL values and empty datasets
- Results maintain IEEE 754 floating point semantics including proper handling of infinities and NaN values
- Memory management handled by PostgreSQL's memory context system through the common function