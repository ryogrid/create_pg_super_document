# numeric_fast_cmp

## Location
src/backend/utils/adt/numeric.c: 2197 - 2218

## Overview
Provides an optimized non-fmgr comparison interface for numeric values, designed specifically for sort support to eliminate function manager overhead during sorting operations.

## Definition


## Detailed Description
The `numeric_fast_cmp` function serves as a direct comparison interface that bypasses PostgreSQL's function manager (fmgr) system to provide faster numeric comparisons during sorting operations. While the performance gain is relatively small compared to the inherent cost of numeric comparisons, it is a required component of the sort support API when abbreviation strategies are employed.

The function performs the core numeric comparison by:
1. Converting Datum inputs to Numeric types using `DatumGetNumeric`
2. Delegating the actual comparison to `cmp_numerics`
3. Properly cleaning up any detoasted numeric values to prevent memory leaks

The function carefully manages memory by checking if the numeric pointers differ from the original datums (indicating detoasting occurred) and freeing them when necessary.

## Parameters / Member Variables
- `x`: First numeric Datum to compare
- `y`: Second numeric Datum to compare  
- `ssup`: SortSupport structure (unused in this function but required by API)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetNumeric](../D/DatumGetNumeric.md) (datum to numeric conversion)
  - [cmp_numerics](../c/cmp_numerics.md) (core numeric comparison logic)
  - [pfree](../p/pfree.md) (memory deallocation for detoasted values)
- Called from (representative examples):
  - [numeric_sortsupport](numeric_sortsupport.md) (as primary comparator)

## Notes and Other Information
- This is a static function internal to numeric.c module
- Required component of PostgreSQL's sort support API
- Eliminates function manager call overhead compared to regular fmgr-based comparison
- Includes proper memory management for detoasted values to prevent leaks
- Performance improvement is modest due to inherent cost of numeric operations
- Could potentially be optimized further with persistent buffers for short varlena inputs
- Returns standard comparison result: negative, zero, or positive integer