# LogicalSlotAdvanceAndCheckSnapState

## Location
src/backend/replication/logical/logical.c: 2108 - 2223

## Overview
Helper function that advances a logical replication slot forward to a specified LSN position while maintaining proper snapshot state and allowing WAL recycling.

## Definition
```c
XLogRecPtr LogicalSlotAdvanceAndCheckSnapState(XLogRecPtr moveto, bool *found_consistent_snapshot)
```

## Detailed Description
This function advances a logical replication slot by reading and processing WAL records from the slot's restart_lsn up to the specified target LSN (`moveto`). The advancement is done in fast_forward mode, meaning no actual logical changes are decoded or output, but the slot's internal state (including snapshot building) is properly maintained.

The function serves as a critical component for logical replication slot management, allowing slots to advance their position without generating decoded changes. This is essential for:
- Preventing WAL accumulation by advancing restart_lsn
- Allowing removal of old catalog tuples
- Building initial snapshots for consistent decoding
- Maintaining slot state consistency

The operation is performed within a PG_TRY/PG_CATCH block to ensure proper cleanup of system caches in case of errors.

## Parameters / Member Variables
- `moveto`: Target XLogRecPtr to advance the slot to. Must be a valid LSN (not InvalidXLogRecPtr)
- `found_consistent_snapshot`: Output parameter that indicates whether an initial consistent snapshot has been built during the advancement process

## Dependencies
- Functions called/Symbols referenced:
  - CreateDecodingContext - Creates logical decoding context in fast_forward mode
  - WaitForStandbyConfirmation - Waits for standby servers to confirm WAL receipt
  - XLogBeginRead - Begins reading from slot's restart_lsn
  - XLogReadRecord - Reads individual WAL records
  - LogicalDecodingProcessRecord - Processes records for snapshot building
  - DecodingContextReady - Checks if decoding context has consistent snapshot
  - LogicalConfirmReceivedLocation - Updates slot's confirmed_flush position
  - ReplicationSlotMarkDirty - Marks slot for checkpoint writing
  - FreeDecodingContext - Cleans up decoding context
  - InvalidateSystemCaches - Invalidates cached catalog information

- Called from (representative examples):
  - update_local_synced_slot - Updates synchronized replication slots
  - pg_logical_replication_slot_advance - SQL interface for slot advancement

## Notes and Other Information
- The function uses fast_forward mode to avoid generating actual decoded changes while still maintaining internal state
- System caches are invalidated before and after processing to ensure catalog consistency
- Resource owner is preserved and restored to handle transaction management side effects
- The slot is marked dirty after advancement to ensure persistence at next checkpoint
- Error handling ensures proper cleanup of system caches even in failure cases
- The function is essential for slot synchronization and SQL-interface slot management
- Located in src/backend/replication/logical/logical.c at lines 2108-2223