# percentile_cont_multi_final_common

## Location
src/backend/utils/adt/orderedsetaggs.c: 848 - 1003

## Overview
A static helper function that implements the common logic for computing continuous percentiles across multiple data types using linear interpolation between adjacent values.

## Definition
```c
static Datum percentile_cont_multi_final_common(FunctionCallInfo fcinfo,
                                               Oid expect_type,
                                               int16 typLen, bool typByVal, char typAlign,
                                               LerpFunc lerpfunc)
```

## Detailed Description  
This function serves as the common implementation for all continuous percentile aggregate functions that operate on arrays of percentile values. Unlike discrete percentiles which return exact row values, continuous percentiles use linear interpolation between adjacent rows to compute fractional positions.

The function handles the complex logic of managing row positions, fetching appropriate data values, and performing interpolation calculations. It optimally reuses previously fetched values when multiple percentiles require interpolation between the same pair of rows, minimizing data access operations.

Key operations include processing NULL values in percentile arrays, sorting required row positions for efficient access, performing tuplesort operations to fetch specific rows, and applying type-specific interpolation through the provided LerpFunc callback.

## Parameters / Member Variables
- `fcinfo`: Standard PostgreSQL function call information structure
- `expect_type`: Expected OID of the data type being processed
- `typLen`: Length of the data type (-1 for variable length types)
- `typByVal`: Whether the data type is passed by value (true) or reference (false) 
- `typAlign`: Alignment requirement for the data type ('c', 's', 'i', 'd')
- `lerpfunc`: Linear interpolation function pointer for the specific data type

## Dependencies
- Functions called/Symbols referenced:
  - [OSAPerGroupState](../O/OSAPerGroupState.md) (struct type)
  - [pct_info](pct_info.md) (struct type) 
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (struct type)
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - PG_GETARG_ARRAYTYPE_P
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - [construct_empty_array](../c/construct_empty_array.md)  
  - [setup_pct_info](../s/setup_pct_info.md)
  - tuplesort_performsort
  - tuplesort_rescan
  - [tuplesort_skiptuples](../t/tuplesort_skiptuples.md)
  - [tuplesort_getdatum](../t/tuplesort_getdatum.md)
  - [construct_md_array](../c/construct_md_array.md)
  - ARR_NDIM/ARR_DIMS/ARR_LBOUND (array macros)
  - [palloc](palloc.md) (memory allocation)
- Called from (representative examples):
  - [percentile_cont_float8_multi_final](percentile_cont_float8_multi_final.md)
  - [percentile_cont_interval_multi_final](percentile_cont_interval_multi_final.md)

## Notes and Other Information
- This is a static function providing shared implementation for type-specific percentile functions
- Uses linear interpolation via type-specific LerpFunc to compute values between adjacent rows
- Optimizes performance by reusing fetched values when consecutive percentiles interpolate between same row pairs
- Handles edge cases where first_row equals second_row (no interpolation needed)
- Maintains strict type safety by validating expected data type matches aggregate state
- Memory management uses palloc for result arrays within PostgreSQL's memory context system
- Part of PostgreSQL's ordered-set aggregate framework enabling statistical functions
- Critical for implementing SQL standard percentile_cont aggregates with proper continuous semantics