# LogicalConfirmReceivedLocation

## Location
src/backend/replication/logical/logical.c: 1839 - 1968

## Overview
Handles a logical replication consumer's confirmation that it has received all changes up to a specified LSN, updating the slot's confirmed flush position and applying any pending candidate xmin/restart LSN values.

## Definition
void LogicalConfirmReceivedLocation(XLogRecPtr lsn)

## Detailed Description
This function is central to the logical replication flow control mechanism. When a logical replication consumer confirms receipt of data up to a specific LSN, this function updates the replication slot's state accordingly. It performs several critical operations:

1. Updates the confirmed_flush position (but never moves it backwards to prevent data duplication)
2. Applies pending candidate catalog_xmin values when the confirmation LSN reaches the required threshold
3. Applies pending candidate restart_lsn values when appropriate
4. Persists changes to disk and updates global xmin tracking

The function implements a two-phase protocol for xmin updates: first writing the new xmin to disk, then updating the effective in-memory value. This ensures crash consistency by guaranteeing that catalog cleanup cannot proceed beyond what's been safely confirmed on disk.

## Parameters / Member Variables
- : The LSN up to which the consumer has confirmed receipt of all changes

## Dependencies
- Functions called/Symbols referenced:
  - ReplicationSlotMarkDirty
  - ReplicationSlotSave
  - ReplicationSlotsComputeRequiredXmin
  - ReplicationSlotsComputeRequiredLSN
  - XLByteToSeg
  - INJECTION_POINT (when USE_INJECTION_POINTS enabled)
  - elog (DEBUG1 logging)
- Called from (representative examples):
  - LogicalIncreaseXminForSlot
  - LogicalIncreaseRestartDecodingForSlot
  - ProcessStandbyReplyMessage
  - pg_logical_slot_get_changes_guts
  - LogicalSlotAdvanceAndCheckSnapState

## Notes and Other Information
- Includes protection against moving confirmed_flush backwards to prevent data duplication
- Uses a candidate system for xmin and restart LSN updates to ensure atomic application
- Includes injection point support for testing segment transitions
- Critical for preventing premature catalog cleanup by maintaining accurate xmin tracking
- The two-phase xmin update protocol ensures crash consistency between disk and memory state