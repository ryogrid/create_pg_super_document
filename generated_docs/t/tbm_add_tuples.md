# tbm_add_tuples

## Location
src/backend/nodes/tidbitmap.c: 377 - 442

## Overview
Adds multiple tuple identifiers (TIDs) to a TID bitmap, efficiently handling consecutive tuples on the same page and managing memory constraints through lossification.

## Definition


## Detailed Description
The `tbm_add_tuples` function adds an array of tuple identifiers to a TID bitmap structure. It efficiently processes multiple TIDs by optimizing for the common case where consecutive tuples belong to the same page, avoiding redundant page lookups. The function handles both exact page entries (where individual tuple bits are set) and lossy chunk entries (where only page-level bits are set).

When the bitmap exceeds its memory limit (`maxentries`), the function triggers lossification (`tbm_lossify`) to convert exact page entries to lossy chunks, trading precision for memory efficiency. The function also validates tuple offsets to prevent buffer overruns and sets recheck flags as needed.

## Parameters / Member Variables
- `tbm`: Pointer to the TIDBitmap structure to add tuples to
- `tids`: Array of ItemPointer structures containing the tuple identifiers to add
- `ntids`: Number of tuple identifiers in the tids array
- `recheck`: Boolean flag indicating whether these tuples require rechecking during scan

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerGetBlockNumber
  - ItemPointerGetOffsetNumber
  - tbm_page_is_lossy
  - tbm_get_pageentry
  - tbm_lossify
  - elog
  - Assert (macro)
  - WORDNUM (macro)
  - BITNUM (macro)
  - TBM_NOT_ITERATING (enum value)
  - MAX_TUPLES_PER_PAGE (constant)
  - bitmapword (type)
  - PagetableEntry (struct type)
- Called from (representative examples):
  - GinDataLeafPageGetItemsToTbm
  - collectMatchBitmap
  - scanPendingInsert
  - gingetbitmap
  - gistScanPage
  - hashgetbitmap
  - btgetbitmap
  - storeBitmap

## Notes and Other Information
- Optimizes for consecutive tuples on the same page by caching the current page lookup
- Validates tuple offsets are within valid range (1 to MAX_TUPLES_PER_PAGE) to prevent errors
- Handles both exact pages (individual tuple bits) and lossy chunks (page-level bits)
- Automatically triggers lossification when memory limit is exceeded to maintain performance
- Sets recheck flag on affected pages when recheck parameter is true
- Forces new page lookup after lossification since pages may have been converted to lossy
- Critical function called by all major index access methods (btree, hash, GIN, GiST, SP-GiST)
- Must not be called while the bitmap is being iterated (checked via assertion)