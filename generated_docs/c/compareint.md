# compareint

## Location
[src/backend/utils/adt/tsgistidx.c:135-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L135-L143)

## Overview
A static utility function that compares two 32-bit integers for use with sorting algorithms, typically qsort.

## Definition
static int compareint(const void *va, const void *vb)

## Detailed Description
The compareint function is a comparison function designed to be used with sorting routines like qsort. It takes two void pointers that point to int32 values, dereferences them, and returns the result of comparing the two integers. The function follows the standard C library comparison function convention, returning a negative value if the first integer is less than the second, zero if they are equal, and a positive value if the first is greater than the second. This function is used internally within the GiST indexing system for tsvector to sort integer arrays during signature compression operations.

## Parameters / Member Variables
- : Pointer to the first int32 value to compare
- : Pointer to the second int32 value to compare

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cmp_s32](../p/pg_cmp_s32.md) (PostgreSQL's signed 32-bit integer comparison function)
- Called from (representative examples):
  - [gtsvector_compress](../g/gtsvector_compress.md) (used for sorting during signature compression)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the tsgistidx.c file
- Follows the standard comparison function interface expected by qsort and similar sorting functions
- Uses PostgreSQL's pg_cmp_s32 function which handles proper comparison semantics including overflow considerations
- Part of the GiST indexing infrastructure for tsvector full-text search
- The function signature matches the standard comparator pattern used throughout PostgreSQL's codebase

## Simplified Source

```c
static int compareint(const void *va, const void *vb) {
    // Extract int32 values from void pointers
    int32 a = *((const int32 *) va);
    int32 b = *((const int32 *) vb);

    // Compare using PostgreSQL's safe comparison function
    return pg_cmp_s32(a, b);
}
```