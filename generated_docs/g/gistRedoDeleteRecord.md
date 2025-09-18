# gistRedoDeleteRecord

## Location
src/backend/access/gist/gistxlog.c: 172 - 222

## Overview
Replays deletion operations on GiST index pages during WAL recovery, removing tuples that were marked as DEAD during index tuple insertion.

## Definition
```c
static void gistRedoDeleteRecord(XLogReaderState *record)
```

## Detailed Description
This function handles WAL recovery for GiST delete operations that remove dead tuples from index pages. These deletions typically occur during vacuum operations or when cleaning up tuples marked as dead during index insertions.

Key functionalities:
1. **Conflict Resolution**: In Hot Standby mode, it resolves recovery conflicts with standby queries before updating the page
2. **Tuple Deletion**: Removes specified tuples using their offset numbers
3. **Page State Management**: Clears the "has garbage" flag and marks tuples as deleted
4. **Consistency Maintenance**: Updates page LSN and marks buffer dirty

The function includes sophisticated conflict handling for Hot Standby scenarios. GiST delete records can conflict with standby queries, so the function checks the snapshot conflict horizon to ensure safe recovery without breaking query consistency.

Unlike vacuum records which handle conflicts globally, individual GiST delete records must resolve conflicts individually based on the snapshot conflict horizon stored in the WAL record.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record with deletion information including offsets and conflict horizon data

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData (extracts gistxlogDelete structure from WAL record)
  - XLogRecGetBlockTag (gets block tag for conflict resolution)
  - ResolveRecoveryConflictWithSnapshot (handles Hot Standby conflicts)
  - XLogReadBufferForRedo (reads buffer for redo operation)
  - PageIndexMultiDelete (performs the actual tuple deletions)
  - GistClearPageHasGarbage (clears page garbage flag)
  - GistMarkTuplesDeleted (marks tuples as deleted)
  - InHotStandby (global variable indicating Hot Standby mode)
- Called from (representative examples):
  - gist_redo (main GiST WAL redo dispatcher)

## Notes and Other Information
- This is a static function only used within gistxlog.c
- Handles Hot Standby conflict resolution before performing deletions
- Part of the GiST index vacuum and cleanup infrastructure
- Critical for maintaining index consistency during recovery
- Processes multiple tuple deletions in a single operation
- The conflict resolution must happen before updating the page to maintain query consistency
- Different from heap vacuum records in terms of conflict handling approach
- Updates both the page state flags and LSN to maintain recovery consistency