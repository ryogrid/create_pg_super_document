# cmpOffsetNumbers

## Location
src/backend/access/spgist/spgdoinsert.c: 112 - 130

## Overview
A qsort comparator function used for sorting arrays of OffsetNumber values in ascending order.

## Definition


## Detailed Description
This function serves as a comparison function for the standard C library qsort function, specifically designed to sort arrays of OffsetNumber values. It compares two OffsetNumber values by dereferencing the void pointers to OffsetNumber pointers and using the PostgreSQL utility function pg_cmp_u16 to perform the comparison. The function returns a negative value if the first offset is smaller, zero if they are equal, or a positive value if the first offset is larger, following standard comparator conventions.

## Parameters / Member Variables
- `a`: Pointer to the first OffsetNumber to compare (passed as void* for qsort compatibility)
- `b`: Pointer to the second OffsetNumber to compare (passed as void* for qsort compatibility)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cmp_u16](../p/pg_cmp_u16.md) (PostgreSQL utility function for comparing 16-bit unsigned integers)
- Called from (representative examples):
  - [lazy_scan_prune](../l/lazy_scan_prune.md) (heap vacuum operations)
  - [spgPageIndexMultiDelete](../s/spgPageIndexMultiDelete.md) (SP-GiST index operations)

## Notes and Other Information
- The function is static, meaning it's only accessible within the vacuumlazy.c module
- Uses pg_cmp_u16 for the actual comparison, which handles OffsetNumber (uint16) values correctly
- This comparator is essential for maintaining sorted order of offset numbers during vacuum operations
- The sorted order ensures efficient processing of heap page modifications during lazy vacuum
- Located in src/backend/access/heap/vacuumlazy.c:1389-1409