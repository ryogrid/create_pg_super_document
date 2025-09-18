# spgRedoVacuumRedirect

## Location
src/backend/access/spgist/spgxlog.c: 860 - 934

## Overview
Replays a vacuum redirect operation from the WAL for SP-GiST indexes, reconstructing the cleanup of redirect tuples and conversion to placeholders during crash recovery.

## Definition


## Detailed Description
This function handles the replay of SP-GiST vacuum redirect operations, which clean up redirect tuples that are no longer needed. Redirect tuples are temporary placeholders created during page splits to maintain consistency, but they need to be cleaned up eventually. The function performs several key operations:

1. Resolves potential Hot Standby conflicts if running in standby mode
2. Converts redirect tuples to plain placeholder tuples by changing their state
3. Updates page opaque data counters (nRedirection and nPlaceholder)
4. Removes trailing placeholder tuples at the end of the page for space reclamation
5. Maintains proper page statistics and layout

The operation ensures that redirect cleanup during recovery maintains the same consistency and Hot Standby compatibility as during normal operation.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record with vacuum redirect operation details

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - XLogRecGetBlockTag
  - ResolveRecoveryConflictWithSnapshot
  - XLogReadBufferForRedo
  - SpGistPageGetOpaque
  - PageGetItem
  - PageGetItemId
  - ItemPointerSetInvalid
  - PageGetMaxOffsetNumber
  - PageIndexMultiDelete
  - BufferGetPage
  - PageSetLSN
  - MarkBufferDirty
  - UnlockReleaseBuffer
- Called from:
  - spg_redo (main SP-GiST redo dispatcher)

## Notes and Other Information
- Handles Hot Standby conflicts by resolving snapshot conflicts before processing
- Converts SPGIST_REDIRECT tuples to SPGIST_PLACEHOLDER state
- Updates page opaque structure counters to maintain accurate statistics
- Performs trailing placeholder cleanup for space efficiency
- Uses palloc/pfree for temporary memory allocation during cleanup
- Ensures proper ordering with PageIndexMultiDelete for batch deletions
- Part of the SP-GiST index WAL recovery subsystem located in src/backend/access/spgist/spgxlog.c:860-934
- Critical for maintaining SP-GiST index consistency and preventing redirect tuple accumulation