# heap_log_freeze_cmp

## Location
src/backend/access/heap/pruneheap.c: 1912 - 1957

## Overview
A comparator function used to sort and deduplicate HeapTupleFreeze structures for efficient WAL logging of XLOG_HEAP2_FREEZE_PAGE operations.

## Definition


## Detailed Description
This function implements a comparison routine for HeapTupleFreeze structures, designed to be used with sorting algorithms (like qsort) to order freeze operations systematically. The comparison follows a hierarchical ordering based on the freeze operation characteristics: xmax (transaction ID), t_infomask2, t_infomask, frzflags, and finally the page offset number as a tiebreaker.

The primary purpose is to enable deduplication of equivalent freeze plans in WAL logging. By sorting the freeze operations, the system can group together tuples that require identical freeze operations, reducing the number of distinct freeze plans that need to be logged. The final tiebreaker on offset number ensures that even equivalent freeze operations maintain a consistent ordering.

The function is specifically designed to work with heap_log_freeze_eq for identifying and consolidating equivalent freeze operations during WAL record generation.

## Parameters / Member Variables
- : Pointer to the first HeapTupleFreeze structure to compare
- : Pointer to the second HeapTupleFreeze structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleFreeze (struct type)
- Called from (representative examples):
  - heap_log_freeze_plan

## Notes and Other Information
- Designed as a comparator for standard library sorting functions (qsort)
- Returns standard comparison values: -1 (less than), 0 (equal), 1 (greater than)
- Uses hierarchical comparison: xmax → t_infomask2 → t_infomask → frzflags → offset
- The offset tiebreaker ensures deterministic ordering even for equivalent freeze operations
- Critical for WAL logging optimization by enabling efficient grouping of similar freeze operations
- The assertion at the end indicates that true equality should not occur after proper deduplication
- Works in conjunction with heap_log_freeze_eq to identify equivalent operations that can share freeze plans