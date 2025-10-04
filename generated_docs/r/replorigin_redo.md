# replorigin_redo

## Location
[src/backend/replication/logical/origin.c:827-887](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L827-L887)

## Overview
Processes replication origin-related WAL records during recovery, handling both SET and DROP operations for replication origins.

## Definition

```c
void
replorigin_redo(XLogReaderState *record)
```
## Detailed Description
replorigin_redo is a WAL record replay function that processes replication origin-related operations during PostgreSQL recovery. It handles two types of operations: XLOG_REPLORIGIN_SET (which advances the replication progress of a specific origin) and XLOG_REPLORIGIN_DROP (which removes/resets a replication origin state). The function extracts the operation type from the WAL record info and executes the corresponding action, ensuring that replication state is properly maintained during recovery scenarios.

## Parameters / Member Variables
- `*record`: XLogReaderState pointer containing the WAL record to be processed
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - XLogRecGetData
  - [replorigin_advance](replorigin_advance.md)
  - elog (PANIC level)
  - [xl_replorigin_set](../x/xl_replorigin_set.md) (struct)
  - [xl_replorigin_drop](../x/xl_replorigin_drop.md) (struct)
  - [ReplicationState](../R/ReplicationState.md) (struct)
  - XLOG_REPLORIGIN_SET
  - XLOG_REPLORIGIN_DROP
  - XLR_INFO_MASK
  - InvalidRepOriginId
  - InvalidXLogRecPtr
- Called from (representative examples):
  - WAL recovery system (via function pointer in rmgr table)

## Notes and Other Information
- This function is part of the resource manager interface for replication origins
- XLOG_REPLORIGIN_SET operations call replorigin_advance with the backward and WAL log flags appropriately set
- XLOG_REPLORIGIN_DROP operations iterate through all replication slots to find and reset the matching origin
- Uses PANIC level error reporting for unknown operation codes, indicating critical system consistency issues
- Essential for maintaining replication state consistency during crash recovery and standby replay

## Simplified Source

```c
void
replorigin_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_REPLORIGIN_SET:
            {
                // Advance replication origin progress
                xl_replorigin_set *xlrec = (xl_replorigin_set *) XLogRecGetData(record);
                replorigin_advance(xlrec->node_id, xlrec->remote_lsn, record->EndRecPtr,
                                 xlrec->force /* backward */, false /* WAL log */);
                break;
            }
        case XLOG_REPLORIGIN_DROP:
            {
                // Reset replication origin state
                xl_replorigin_drop *xlrec = (xl_replorigin_drop *) XLogRecGetData(record);

                // Find and reset the matching replication state
                for (int i = 0; i < max_replication_slots; i++) {
                    ReplicationState *state = &replication_states[i];
                    if (state->roident == xlrec->node_id) {
                        // Reset entry
                        state->roident = InvalidRepOriginId;
                        state->remote_lsn = InvalidXLogRecPtr;
                        state->local_lsn = InvalidXLogRecPtr;
                        break;
                    }
                }
                break;
            }
        default:
            elog(PANIC, "replorigin_redo: unknown op code %u", info);
    }
}
```