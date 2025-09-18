# heap_xlog_visible

## Location
src/backend/access/heap/heapam.c: 9363 - 9497

## Overview
Replays XLOG_HEAP2_VISIBLE WAL records to restore visibility map state and page-level visibility bits during PostgreSQL recovery, ensuring integrity between visibility map and page visibility flags.

## Definition
```c
static void heap_xlog_visible(XLogReaderState *record)
```

## Detailed Description
This function is responsible for replaying visibility map changes during WAL recovery. It handles the critical integrity requirement that the visibility map bit must never be set while the page-level PD_ALL_VISIBLE bit is clear, as this would cause subsequent page modifications to fail to clear the visibility map bit.

The function performs several key operations:
1. Resolves Hot Standby conflicts for transactions with old xmin horizons
2. Updates the heap page PD_ALL_VISIBLE bit if needed
3. Updates the visibility map with appropriate bits
4. Updates the Free Space Map (FSM) to prevent stale free space information in standbys

The recovery process carefully handles cases where the heap file may have been dropped or truncated, and ensures proper LSN handling to avoid torn page hazards.

## Parameters
- `record`: XLogReaderState pointer containing the WAL record data for the visibility operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extract xl_heap_visible structure from WAL record
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md): Get relation and block information
  - [ResolveRecoveryConflictWithSnapshot](../R/ResolveRecoveryConflictWithSnapshot.md): Handle Hot Standby conflicts
  - XLogReadBufferForRedo: Read heap page for redo
  - [PageSetAllVisible](../P/PageSetAllVisible.md): Set page-level visibility bit
  - XLogHintBitIsNeeded: Check if LSN update is required
  - [PageGetFreeSpace](../P/PageGetFreeSpace.md): Calculate free space for FSM update
  - XLogRecordPageWithFreeSpace: Update FSM with free space info
  - XLogReadBufferForRedoExtended: Read visibility map page
  - [visibilitymap_pin](../v/visibilitymap_pin.md): Pin visibility map page
  - [visibilitymap_set](../v/visibilitymap_set.md): Update visibility map bits
  - [CreateFakeRelcacheEntry](../C/CreateFakeRelcacheEntry.md)/FreeFakeRelcacheEntry: Temporary relation cache handling

- Called from:
  - [heap2_redo](heap2_redo.md): Main heap WAL record replay dispatcher

## Notes and Other Information
- This is a static function only called during WAL recovery operations
- Critical for maintaining visibility map consistency during crash recovery
- Handles both heap page and visibility map updates atomically
- Includes special handling for Hot Standby conflict resolution
- Updates FSM to prevent performance issues in promoted standbys
- Uses careful LSN management to handle torn page scenarios safely
- The function must handle cases where heap files are truncated/dropped during recovery