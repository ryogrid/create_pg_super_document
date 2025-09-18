# pg_cmp_s32

## Location
src/include/common/int.h: 483 - 488

## Overview
A fast inline comparison function for 32-bit signed integers that returns a standardized comparison result (-1, 0, or 1) without using conditional branches.

## Definition


## Detailed Description
The  function implements a three-way comparison for 32-bit signed integers using a branchless algorithm. It returns -1 if , 0 if , and 1 if . The implementation uses the expression  which leverages the fact that boolean expressions evaluate to 0 or 1 in C, creating an efficient branchless comparison that avoids conditional jumps and potential pipeline stalls.

This function is part of PostgreSQL's collection of standardized comparison utilities that provide consistent three-way comparison semantics across different data types, making it particularly useful for sorting algorithms and binary search operations.

## Parameters / Member Variables
- : First 32-bit signed integer to compare
- : Second 32-bit signed integer to compare

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only basic arithmetic operations)
- Called from (representative examples):
  - [list_int_cmp](../l/list_int_cmp.md) (src/backend/nodes/list.c:1696)
  - [db_comparator](../d/db_comparator.md) (src/backend/postmaster/autovacuum.c:1057)
  - [compareint](../c/compareint.md) (src/backend/utils/adt/tsgistidx.c:140)
  - [int_cmp](../i/int_cmp.md) (src/bin/pg_dump/pg_dump_sort.c:1734)

## Notes and Other Information
- The branchless implementation  is more efficient than traditional if-else comparison logic
- This function is declared as  for maximum performance in hot code paths
- Part of a family of comparison functions for different integer types (pg_cmp_u32, pg_cmp_s64, etc.)
- Widely used throughout PostgreSQL for sorting operations, particularly in list sorting and index maintenance