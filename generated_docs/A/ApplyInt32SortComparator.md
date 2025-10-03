# ApplyInt32SortComparator

## Location
[src/include/utils/sortsupport.h:302-340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/sortsupport.h#L302-L340)

## Overview
ApplyInt32SortComparator is an inline function that performs comparison of two Datum values as 32-bit signed integers, handling NULL values and sort direction according to sort support configuration.

## Definition

```c
static inline int
ApplyInt32SortComparator(Datum datum1, bool isNull1,
						 Datum datum2, bool isNull2,
						 SortSupport ssup)
```
## Detailed Description
This function provides a specialized comparison for 32-bit signed integer values stored as Datum. It converts Datum values to signed 32-bit integers using DatumGetInt32() and compares them directly. The function implements the same NULL handling logic as other comparator functions and supports sort direction reversal.

This comparator is specifically optimized for 32-bit integer data types, providing better performance than generic comparator functions by avoiding function pointer overhead and performing direct comparison after type conversion.

## Parameters / Member Variables
- `datum1`: The first Datum value to compare (converted to signed int32)
- `isNull1`: Boolean flag indicating whether datum1 is NULL
- `datum2`: The second Datum value to compare (converted to signed int32)
- `isNull2`: Boolean flag indicating whether datum2 is NULL
- `ssup`: SortSupport structure containing sort configuration
## Dependencies
- Functions called/Symbols referenced:
  - [SortSupport](../S/SortSupport.md) (struct type)
  - [DatumGetInt32](../D/DatumGetInt32.md) (conversion macro/function)
  - INVERT_COMPARE_RESULT (macro)
- Called from (representative examples):
  - [qsort_tuple_int32_compare](../q/qsort_tuple_int32_compare.md) (src/backend/utils/sort/tuplesort.c:549)

## Notes and Other Information
This function is a performance optimization specifically for 32-bit signed integer data types such as int4. It provides the same NULL handling and sort direction semantics as the general ApplySortComparator but avoids function pointer overhead. The use of DatumGetInt32() ensures proper conversion from the Datum representation to a 32-bit signed integer value for comparison. This comparator is typically used in tuplesort operations where the data type is known to be a 32-bit integer.