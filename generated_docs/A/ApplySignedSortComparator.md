# ApplySignedSortComparator

## Location
[src/include/utils/sortsupport.h:267-301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/sortsupport.h#L267-L301)

## Overview
ApplySignedSortComparator is an inline function that performs signed comparison of two Datum values, handling NULL values and sort direction according to sort support configuration.

## Definition

```c
static inline int
ApplySignedSortComparator(Datum datum1, bool isNull1,
						  Datum datum2, bool isNull2,
						  SortSupport ssup)
```
## Detailed Description
This function provides a specialized comparison for signed integer values stored as Datum. It converts Datum values to signed 64-bit integers using DatumGetInt64() and compares them directly. Like other comparator functions, it implements proper NULL handling logic and supports sort direction reversal.

The function is specifically designed for signed integer data types and provides better performance than using a generic comparator function pointer by performing direct comparison after type conversion.

## Parameters / Member Variables
- `datum1`: The first Datum value to compare (converted to signed int64)
- `isNull1`: Boolean flag indicating whether datum1 is NULL
- `datum2`: The second Datum value to compare (converted to signed int64)
- `isNull2`: Boolean flag indicating whether datum2 is NULL
- `ssup`: SortSupport structure containing sort configuration
## Dependencies
- Functions called/Symbols referenced:
  - [SortSupport](../S/SortSupport.md) (struct type)
  - [DatumGetInt64](../D/DatumGetInt64.md) (conversion macro/function)
  - INVERT_COMPARE_RESULT (macro)
- Called from (representative examples):
  - [qsort_tuple_signed_compare](../q/qsort_tuple_signed_compare.md) (src/backend/utils/sort/tuplesort.c:525)

## Notes and Other Information
This function is a performance optimization specifically for signed integer data types that can be safely converted to int64. It avoids the overhead of function pointer calls while providing the same NULL handling and sort direction semantics as the general ApplySortComparator. The use of DatumGetInt64() ensures proper sign extension and handling of signed values across different platforms and architectures.

## Simplified Source

```c
static inline int ApplySignedSortComparator(Datum datum1, bool isNull1,
                                            Datum datum2, bool isNull2,
                                            SortSupport ssup) {
    int compare;

    // Handle NULL value comparisons first
    if (isNull1) {
        if (isNull2)
            compare = 0;                    // NULL == NULL
        else if (ssup->ssup_nulls_first)
            compare = -1;                   // NULL < NOT_NULL
        else
            compare = 1;                    // NULL > NOT_NULL
    } else if (isNull2) {
        if (ssup->ssup_nulls_first)
            compare = 1;                    // NOT_NULL > NULL
        else
            compare = -1;                   // NOT_NULL < NULL
    } else {
        // Convert to signed int64 and compare directly for performance
        compare = DatumGetInt64(datum1) < DatumGetInt64(datum2) ? -1 :
                  DatumGetInt64(datum1) > DatumGetInt64(datum2) ? 1 : 0;

        // Apply reverse sort order if configured
        if (ssup->ssup_reverse)
            INVERT_COMPARE_RESULT(compare);
    }

    return compare;
}
```