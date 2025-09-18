# TocEntrySizeCompareBinaryheap

## Location
src/bin/pg_dump/pg_backup_archiver.c: 4504 - 4518

## Overview
A binary heap comparator function that sorts TocEntry pointers by data length, designed to create a max-heap where the largest entries have the highest priority.

## Definition
```c
static int TocEntrySizeCompareBinaryheap(void *p1, void *p2, void *arg)
```

## Detailed Description
This function serves as a comparison function for PostgreSQL's binary heap data structure, specifically designed to sort TocEntry pointers. It creates a max-heap by negating the result of TocEntrySizeCompareQsort, which means entries with larger dataLength values will have higher priority in the heap. This is the opposite of typical min-heap behavior, allowing the heap to efficiently retrieve the largest data chunks first.

The function leverages the existing TocEntrySizeCompareQsort comparator but inverts its result to achieve max-heap semantics. This design ensures consistency between the two comparison functions while adapting the logic for binary heap usage. The max-heap property means that the root of the heap will always contain the TocEntry with the largest dataLength.

## Parameters / Member Variables
- `p1`: Pointer to the first TocEntry being compared. Unlike the qsort version, this is a direct void pointer to a TocEntry.
- `p2`: Pointer to the second TocEntry being compared. Also a direct void pointer to a TocEntry.
- `arg`: Additional argument parameter required by the binary heap interface. This parameter is not used in this implementation but is required for compatibility with the binary heap API.

## Dependencies
- Functions called/Symbols referenced:
  - TocEntrySizeCompareQsort (delegates comparison logic to this function)
- Called from (representative examples):
  - restore_toc_entries_parallel (for parallel processing optimization)
  - Functions related to TEXT_DUMPALL_HEADER processing

## Notes and Other Information
- This is a static function within pg_backup_archiver.c for internal use within the archiver module
- The function creates a max-heap by negating the qsort comparator result, ensuring largest entries have highest priority
- Used primarily in parallel restore operations where prioritizing larger data chunks can improve performance
- The binary heap provides efficient insertion and extraction of the highest-priority elements
- The unused `arg` parameter maintains compatibility with PostgreSQL's binary heap interface
- By reusing TocEntrySizeCompareQsort logic, it maintains consistency in sorting criteria across different data structures
- The function is located at src/bin/pg_dump/pg_backup_archiver.c:4504-4518
- Part of the parallel processing optimization system in pg_dump/pg_restore