# vacuumLeafPage

## Location
src/backend/access/spgist/spgvacuum.c: 125 - 407

## Overview
Vacuums a regular (non-root) leaf page in an SP-GiST index, deleting tuples targeted for deletion while preserving chain structure and handling concurrent redirections.

## Definition
```c
static void vacuumLeafPage(spgBulkDeleteState *bds, Relation index, Buffer buffer, bool forPending)
```

## Detailed Description
This complex function performs vacuum operations on SP-GiST leaf pages with sophisticated chain management. It identifies tuples to delete based on the vacuum callback, but carefully preserves tuple chains by not moving tuples referenced by outside links (assumed to be chain heads).

The function handles three types of tuple states:
- **LIVE tuples**: Checked against vacuum callback, with chain links tracked
- **REDIRECT tuples**: Added to pending list if created by concurrent transactions
- **Other states**: Validated for consistency

The vacuum process operates in several phases:
1. **Scan phase**: Identifies deletable tuples and builds predecessor map
2. **Planning phase**: Determines exact operations needed (dead, placeholder, move, chain updates)
3. **Execution phase**: Performs operations within critical section with WAL logging

Chain management is sophisticated - the function processes entire chains to maintain consistency, using placeholder tuples for mid-chain deletions and moving tuples when necessary to preserve chain heads.

## Parameters / Member Variables
- `bds`: Pointer to spgBulkDeleteState containing vacuum state and callback function
- `index`: The SP-GiST index relation being vacuumed
- `buffer`: Buffer containing the leaf page to vacuum
- `forPending`: Boolean indicating if this call is from pending list processing (affects statistics counting)

## Dependencies
- Functions called/Symbols referenced:
  - [spgAddPendingTID](../s/spgAddPendingTID.md): Adds redirect targets to pending list
  - [spgPageIndexMultiDelete](../s/spgPageIndexMultiDelete.md): Performs bulk tuple state changes
  - [BufferGetPage](../B/BufferGetPage.md), PageGetItem, PageGetItemId: Page access functions
  - [TransactionIdFollowsOrEquals](../T/TransactionIdFollowsOrEquals.md): Transaction visibility check
  - XLog functions: WAL logging (XLogBeginInsert, XLogInsert, etc.)
  - Various SP-GiST tuple access macros (SGLT_GET_NEXTOFFSET, etc.)
- Called from (representative examples):
  - [spgvacuumpage](../s/spgvacuumpage.md): Main vacuum entry point for sequential page processing
  - [spgprocesspending](../s/spgprocesspending.md): Called when processing pending redirect targets

## Notes and Other Information
- This is a static function within the spgvacuum.c file
- Implements sophisticated chain management to maintain SP-GiST invariants during vacuum
- Uses critical sections and WAL logging for crash safety
- The `forPending` parameter prevents double-counting tuples in statistics
- Handles concurrent insertions through the pending list mechanism
- Performs extensive validation to detect chain corruption
- Uses four types of operations: dead tuple creation, placeholder insertion, tuple movement, and chain link updates
- The tuple movement is implemented by swapping line pointers for efficiency
- Includes comprehensive WAL logging for crash recovery
- Part of the SP-GiST vacuum subsystem designed to handle concurrent operations safely