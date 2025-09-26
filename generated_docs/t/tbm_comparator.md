# tbm_comparator

## Location
[src/backend/nodes/tidbitmap.c:1424-1437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L1424-L1437)

## Overview
A qsort-compatible comparator function that sorts PagetableEntry pointers by their block numbers in ascending order.

## Definition
```c
static int tbm_comparator(const void *left, const void *right)
```

## Detailed Description
This function serves as a comparison function for the standard C library qsort() routine, specifically designed to sort arrays of PagetableEntry pointers. It extracts the block numbers from the PagetableEntry structures and compares them using PostgreSQL's pg_cmp_u32() utility function.

The function is essential for preparing TID bitmaps for iteration in sorted order, ensuring that pages are processed sequentially by block number. This ordering is important for performance optimization in scan operations, as accessing pages in sequential order typically provides better I/O locality.

The function handles the double pointer indirection required by qsort when sorting arrays of pointers - it dereferences both the void pointers to get PagetableEntry pointers, then accesses their blockno fields for comparison.

## Parameters / Member Variables
- `left`: Pointer to a PagetableEntry pointer (const void * that points to PagetableEntry *)
- `right`: Pointer to a PagetableEntry pointer (const void * that points to PagetableEntry *)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cmp_u32](../p/pg_cmp_u32.md) (PostgreSQL utility for comparing 32-bit unsigned integers)
- Types used:
  - [PagetableEntry](../P/PagetableEntry.md) (for accessing blockno fields)
  - BlockNumber (the type of blockno field)
- Called from:
  - [tbm_begin_iterate](tbm_begin_iterate.md) (used in qsort calls to sort page entries)
  - Referenced in TBMSharedIterator

## Notes and Other Information
- This is a static function, only accessible within tidbitmap.c
- Follows the standard qsort comparator interface returning negative, zero, or positive values
- Uses PostgreSQL's pg_cmp_u32() rather than direct comparison for consistency and potential overflow safety
- Critical for ensuring TID bitmap iterations occur in block number order
- The double pointer indirection is necessary because qsort sorts arrays of pointers to PagetableEntry structures
- Sorting by block number provides optimal I/O access patterns during bitmap scans