# vacuumLeafRoot

## Location
src/backend/access/spgist/spgvacuum.c: 408 - 492

## Overview
Vacuums the root page of an SP-GiST index when it also serves as a leaf page, using a simplified deletion approach without chain management complexity.

## Definition
```c
static void vacuumLeafRoot(spgBulkDeleteState *bds, Relation index, Buffer buffer)
```

## Detailed Description
This function provides a specialized vacuum implementation for SP-GiST root pages that also contain leaf tuples. Unlike regular leaf page vacuuming, this function uses a much simpler approach since root pages don't have the complex chain structures found in regular leaf pages.

The function operates in a straightforward manner:
1. **Scan phase**: Iterates through all tuples on the root page
2. **Validation**: Ensures all tuples are in LIVE state (no redirects or placeholders expected)
3. **Deletion**: Uses the vacuum callback to determine which tuples to delete
4. **Execution**: Performs simple tuple deletion with WAL logging

This simplified approach is possible because:
- Root pages don't participate in complex tuple chains
- No placeholder or redirect tuples should exist on root pages
- No tuple movement or chain link updates are needed

## Parameters / Member Variables
- `bds`: Pointer to spgBulkDeleteState containing vacuum state, callback function, and statistics
- `index`: The SP-GiST index relation being vacuumed  
- `buffer`: Buffer containing the root page that also serves as a leaf

## Dependencies
- Functions called/Symbols referenced:
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md): Performs bulk tuple deletion from page
  - [BufferGetPage](../B/BufferGetPage.md), PageGetItem, PageGetItemId: Page access functions
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md): Validates tuple heap pointer
  - XLog functions: WAL logging (XLogBeginInsert, XLogInsert, etc.)
  - STORE_STATE: Saves SP-GiST state information for WAL
  - [spgxlogVacuumRoot](../s/spgxlogVacuumRoot.md): WAL record structure for root vacuum operations
- Called from (representative examples):
  - [spgvacuumpage](../s/spgvacuumpage.md): Main vacuum entry point when processing root pages that contain leaf data

## Notes and Other Information
- This is a static function within the spgvacuum.c file
- Much simpler than vacuumLeafPage due to absence of chain structures on root pages
- All tuples on root pages are expected to be in LIVE state - any other state triggers an error
- Uses PageIndexMultiDelete for efficient bulk deletion since tuple numbers are in order
- Includes proper WAL logging with XLOG_SPGIST_VACUUM_ROOT record type
- Updates vacuum statistics by counting deleted and remaining tuples
- Part of the SP-GiST vacuum subsystem, specifically handling the special case of root pages with leaf data
- The function validates that only LIVE tuples exist on root pages, enforcing SP-GiST structural invariants
- Performs operations within critical sections for crash safety