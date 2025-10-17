# TocEntrySizeCompareQsort

## Location
[src/bin/pg_dump/pg_backup_archiver.c:4482-4503](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L4482-L4503)

## Overview
A qsort-compatible comparator function that sorts TocEntry pointers by data length in descending order, with dumpId as a secondary sort key for stability.

## Definition
```c
static int TocEntrySizeCompareQsort(const void *p1, const void *p2)
```

## Detailed Description
This function serves as a comparison function for the standard C library qsort() routine, designed to sort arrays of TocEntry pointers. The primary sorting criterion is the dataLength field, arranged in descending order (largest entries first). When two entries have identical dataLength values, the function uses dumpId as a secondary sort key in ascending order to ensure a stable, deterministic sort.

The function follows the standard qsort comparator contract: it returns a negative value if the first element should come before the second, a positive value if the first should come after the second, and zero if they are equal. This implementation prioritizes larger data chunks first, which is typically useful for optimizing processing order in database dump/restore operations.

## Parameters / Member Variables
- `p1`: Pointer to the first element being compared. This is actually a pointer to a pointer to a TocEntry (const TocEntry *const *).
- `p2`: Pointer to the second element being compared. This is also a pointer to a pointer to a TocEntry (const TocEntry *const *).

## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](TocEntry.md) (struct type, accessing dataLength and dumpId fields)
- Called from (representative examples):
  - [WriteDataChunks](../W/WriteDataChunks.md) (for optimizing data writing order)
  - [TocEntrySizeCompareBinaryheap](TocEntrySizeCompareBinaryheap.md) (as a comparison reference)
  - Functions related to TEXT_DUMPALL_HEADER processing

## Notes and Other Information
- This is a static function within pg_backup_archiver.c for internal use within the archiver module
- The function implements a stable sort by using dumpId as a tiebreaker when dataLength values are equal
- Sorting by decreasing dataLength helps optimize I/O patterns and processing efficiency during dump/restore operations
- The double pointer parameter pattern is required for qsort when sorting arrays of pointers
- Used primarily in data chunk processing where larger chunks should be handled first
- The function is located at src/bin/pg_dump/pg_backup_archiver.c:4482-4503
- Compatible with the standard C library qsort() function signature

## Simplified Source

```c
static int
TocEntrySizeCompareQsort(const void *p1, const void *p2)
{
    const TocEntry *te1 = *(const TocEntry *const *) p1;
    const TocEntry *te2 = *(const TocEntry *const *) p2;

    // Primary sort: decreasing dataLength (largest first)
    if (te1->dataLength > te2->dataLength)
        return -1;
    if (te1->dataLength < te2->dataLength)
        return 1;

    // Secondary sort: increasing dumpId for stability
    if (te1->dumpId < te2->dumpId)
        return -1;
    if (te1->dumpId > te2->dumpId)
        return 1;

    return 0;
}
```