# ApplySignedSortComparator

## Location
src/include/utils/sortsupport.h: 267 - 301

## Overview
ApplySignedSortComparator is an inline function that performs signed comparison of two Datum values, handling NULL values and sort direction according to sort support configuration.

## Definition


## Detailed Description
This function provides a specialized comparison for signed integer values stored as Datum. It converts Datum values to signed 64-bit integers using DatumGetInt64() and compares them directly. Like other comparator functions, it implements proper NULL handling logic and supports sort direction reversal.

The function is specifically designed for signed integer data types and provides better performance than using a generic comparator function pointer by performing direct comparison after type conversion.

## Parameters / Member Variables
- : The first Datum value to compare (converted to signed int64)
- : Boolean flag indicating whether datum1 is NULL
- : The second Datum value to compare (converted to signed int64)
- : Boolean flag indicating whether datum2 is NULL
- : SortSupport structure containing sort configuration

## Dependencies
- Functions called/Symbols referenced:
  - SortSupport (struct type)
  - DatumGetInt64 (conversion macro/function)
  - INVERT_COMPARE_RESULT (macro)
- Called from (representative examples):
  - qsort_tuple_signed_compare (src/backend/utils/sort/tuplesort.c:525)

## Notes and Other Information
This function is a performance optimization specifically for signed integer data types that can be safely converted to int64. It avoids the overhead of function pointer calls while providing the same NULL handling and sort direction semantics as the general ApplySortComparator. The use of DatumGetInt64() ensures proper sign extension and handling of signed values across different platforms and architectures.