# heap_mask

## Location
[src/backend/access/heap/heapam.c:10423-10518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L10423-L10518)

## Overview
Masks volatile fields in heap pages before consistency checks to normalize differences between primary and standby servers during WAL replay verification.

## Definition
```c
void heap_mask(char *pagedata, BlockNumber blkno)
```

## Detailed Description
The `heap_mask` function prepares heap pages for consistency checking by masking out fields that can legitimately differ between a primary server and its standby replicas. This function is crucial for PostgreSQL's consistency verification mechanisms, ensuring that only meaningful differences are detected during page comparisons.

The function systematically processes each tuple on the page and masks various fields that can vary due to timing differences, hint bit updates, or replay mechanisms. It handles LSN and checksum masking, hint bits, unused space, transaction visibility information, command IDs, and speculative insertion details. Special attention is given to frozen tuples, speculative tuples, and proper alignment padding.

## Parameters / Member Variables
- `pagedata`: Character pointer to the raw page data to be masked
- `blkno`: Block number of the page being processed, used for reconstructing speculative tuple CTIDs

## Dependencies
- Functions called/Symbols referenced:
  - [mask_page_lsn_and_checksum](../m/mask_page_lsn_and_checksum.md) (masks LSN and checksum fields)
  - [mask_page_hint_bits](../m/mask_page_hint_bits.md) (masks page-level hint bits)  
  - [mask_unused_space](../m/mask_unused_space.md) (masks unused space in page)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md) (gets highest offset number on page)
  - [PageGetItemId](../P/PageGetItemId.md) (retrieves item identifier)
  - ItemIdGetOffset, ItemIdIsNormal, ItemIdHasStorage, ItemIdGetLength (item identifier operations)
  - HeapTupleHeaderXminFrozen, HeapTupleHeaderIsSpeculative (tuple header checks)
  - [ItemPointerSet](../I/ItemPointerSet.md) (sets tuple CTID)
- Called from:
  - WAL consistency checking infrastructure (not directly referenced)

## Notes and Other Information
- Critical component of PostgreSQL's consistency verification system
- Handles different masking strategies for frozen vs non-frozen tuples
- Special handling for speculative insertions which use backend-specific tokens
- Masks command IDs since they're set to FirstCommandId during replay
- Properly handles tuple alignment padding to avoid false consistency failures
- Part of the broader WAL verification framework used to ensure standby consistency
- Does not mask CTID changes for moved partitions as that information must remain consistent