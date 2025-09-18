# ApplyUnsignedSortComparator

## Location
src/include/utils/sortsupport.h: 233 - 266

## Overview
ApplyUnsignedSortComparator is an inline function that performs unsigned comparison of two Datum values, handling NULL values and sort direction according to sort support configuration.

## Definition


## Detailed Description
This function provides a specialized comparison for unsigned integer values stored as Datum. Unlike ApplySortComparator which uses a function pointer, this function performs direct unsigned comparison of the Datum values. It implements the same NULL handling logic as the general comparator but uses direct comparison operators for better performance with unsigned data types.

The function treats Datum values as unsigned integers and compares them directly using C comparison operators, making it suitable for unsigned integer types like OID, unsigned integers, and similar data types.

## Parameters / Member Variables
- : The first Datum value to compare (treated as unsigned)
- : Boolean flag indicating whether datum1 is NULL
- : The second Datum value to compare (treated as unsigned)
- : Boolean flag indicating whether datum2 is NULL
- : SortSupport structure containing sort configuration

## Dependencies
- Functions called/Symbols referenced:
  - SortSupport (struct type)
  - INVERT_COMPARE_RESULT (macro)
  - SIZEOF_DATUM (macro)
- Called from (representative examples):
  - [qsort_tuple_unsigned_compare](../q/qsort_tuple_unsigned_compare.md) (src/backend/utils/sort/tuplesort.c:502)

## Notes and Other Information
This function is a performance optimization for unsigned data types, avoiding the overhead of function pointer calls used in the general ApplySortComparator. It's specifically designed for sorting operations where the data type is known to be unsigned and can be safely compared using direct integer comparison. The direct comparison approach (datum1 < datum2 ? -1 : datum1 > datum2 ? 1 : 0) provides better performance than calling through a function pointer while maintaining the same 3-way comparison semantics.