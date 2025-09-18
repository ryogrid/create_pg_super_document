# buffer_cmp

## Location
src/backend/storage/smgr/bulk_write.c: 226 - 242

## Overview
A static comparison function used to sort PendingWrite structures by block number in ascending order for efficient bulk write operations.

## Definition
```c
static int buffer_cmp(const void *a, const void *b)
```

## Detailed Description
This is a standard qsort-style comparison function that compares two PendingWrite structures based on their block numbers. It enforces the invariant that no duplicate block writes should exist by asserting that the block numbers are never equal. The function returns a negative value if the first block number is smaller, positive if larger, following the standard comparison function contract used by sorting routines.

## Parameters / Member Variables
- `a`: Pointer to the first PendingWrite structure to compare
- `b`: Pointer to the second PendingWrite structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - [PendingWrite](../P/PendingWrite.md) (structure type)
  - Assert (debugging macro)
- Called from (representative examples):
  - [smgr_bulk_flush](../s/smgr_bulk_flush.md) (used in qsort operation)

## Notes and Other Information
- This is a static function internal to the bulk_write.c module
- The function assumes that duplicate writes to the same block number should never occur and will assert if they do
- Used primarily by smgr_bulk_flush to sort pending writes by block number for optimal I/O performance
- Sorting by block number allows for sequential disk writes, reducing seek overhead
- The comparison follows the standard C library qsort comparison function interface