# compareDoubles

## Location
[src/backend/utils/adt/geo_spgist.c:93-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L93-L106)

## Overview
A static comparator function used by qsort for comparing floating-point values in the context of SP-GiST geometric index performance optimizations.

## Definition

```c
typedef struct
{
	float8		low;
	float8		high;
} Range;
```
## Detailed Description
This function serves as a simple floating-point comparator for the qsort library function. It's specifically designed for use in geometric indexing operations where performance optimization is the primary concern rather than strict floating-point correctness. The function compares two float8 (double-precision floating-point) values and returns the standard comparison result expected by qsort.

The implementation deliberately avoids using PostgreSQL's floating-point macros that handle special cases like NaN or infinity, as noted in the comments. This design choice prioritizes performance over comprehensive floating-point handling since this comparator is only used to improve index performance, not to ensure correctness of floating-point operations.

## Parameters / Member Variables
- `low`: Pointer to the first float8 value to compare
- `high`: Pointer to the second float8 value to compare
## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic C operations)
- Called from (representative examples):
  - [spg_box_quad_picksplit](../s/spg_box_quad_picksplit.md) (multiple times for sorting coordinates)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the geo_spgist.c file
- The function intentionally does not handle special floating-point cases (NaN, infinity) for performance reasons
- Used specifically in the context of SP-GiST (Space-Partitioned Generalized Search Tree) index operations
- Returns standard qsort comparison values: -1 (less than), 0 (equal), 1 (greater than)
- The comment emphasizes that this affects index performance, not correctness, indicating it's used in optimization paths

## Simplified Source

```c
static int compareDoubles(const void *a, const void *b) {
    // Extract float8 values from void pointers
    float8 x = *(float8 *) a;
    float8 y = *(float8 *) b;

    // Simple comparison for qsort (performance optimized)
    if (x == y) return 0;
    return (x > y) ? 1 : -1;
}
```