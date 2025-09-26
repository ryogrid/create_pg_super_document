# ssup_datum_int32_cmp

## Location
src/backend/utils/sort/tuplesort.c: 3204 - 3215

## Overview
A generic comparison function for signed 32-bit integer Datum values used in PostgreSQL's SortSupport framework for efficient sorting operations.

## Definition
```c
int ssup_datum_int32_cmp(Datum x, Datum y, SortSupport ssup)
```

## Detailed Description
This function provides optimized comparison for Datum values when they represent signed 32-bit integers. It extracts the underlying int32 values from the Datum wrapper and performs a direct integer comparison, implementing the standard three-way comparison semantics required by PostgreSQL's sorting infrastructure.

The function is designed for data types that can be represented as or converted to signed 32-bit integers, providing significant performance benefits over calling type-specific comparison functions. This optimization is particularly effective for common numeric data types like integer and date values that have a natural int32 representation.

## Parameters / Member Variables
- `x`: First Datum value to compare (converted to signed int32)
- `y`: Second Datum value to compare (converted to signed int32)
- `ssup`: SortSupport context (unused in this implementation but required by interface)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetInt32 (macro to extract int32 value from Datum)
  - SortSupport (sort support framework structure)

- Called from (representative examples):
  - btint4sortsupport (B-tree support for int4/integer sorting)
  - date_sortsupport (date sorting support)
  - tuplesort_sort_memtuples (in-memory tuple sorting)
  - ApplySortAbbrevFullComparator (abbreviated key comparison)

## Notes and Other Information
- Specifically designed for signed 32-bit integer comparisons
- Returns -1 if x < y, 1 if x > y, and 0 if x == y
- Uses DatumGetInt32 to safely extract int32 values from Datum wrappers
- The SortSupport parameter is unused but maintained for interface compatibility
- Provides significant performance improvements for int32-based data types
- Handles negative values correctly due to signed comparison semantics
- Part of PostgreSQL's SortSupport framework for optimizing sort operations
- Commonly used for integer, date, and other int32-based data types
- More memory-efficient than int64 variant for data that fits in 32 bits