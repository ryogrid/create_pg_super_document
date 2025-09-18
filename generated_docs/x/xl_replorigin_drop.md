# xl_replorigin_drop

## Location
src/include/replication/origin.h: 25 - 28

## Overview
WAL record structure that stores information for dropping a replication origin during recovery or replication operations.

## Definition
```c
typedef struct xl_replorigin_drop
{
    RepOriginId node_id;
} xl_replorigin_drop;
```

## Detailed Description
The `xl_replorigin_drop` structure is used in PostgreSQL's Write-Ahead Logging (WAL) system to record the removal of a replication origin. This structure is written to WAL when a replication origin is dropped, ensuring that the operation can be properly replayed during recovery. During WAL replay, this record is processed to clean up the replication state for the specified origin node.

The structure represents a WAL record of type `XLOG_REPLORIGIN_DROP` and contains the minimal information needed to identify which replication origin should be removed from the system.

## Parameters / Member Variables
- `node_id`: The unique identifier of the replication origin node to be dropped (of type RepOriginId, which is uint16)

## Dependencies
- Functions called/Symbols referenced:
  - RepOriginId (type definition)

- Called from (representative examples):
  - [replorigin_desc](../r/replorigin_desc.md) (WAL record description function)
  - [replorigin_redo](../r/replorigin_redo.md) (WAL record replay function)
  - replorigin_state_clear (clears replication state during drop operation)

## Notes and Other Information
- This structure is part of PostgreSQL's logical replication infrastructure for managing replication origins
- During WAL replay, this record is processed by `replorigin_redo()` which iterates through replication slots to find and reset the state for the specified origin
- The drop operation clears the replication state by setting the origin identifier to `InvalidRepOriginId`
- The structure is defined in `src/include/replication/origin.h` alongside other replication origin related definitions
- WAL record type constant: `XLOG_REPLORIGIN_DROP` (0x10)
- This ensures that replication origin drops are crash-safe and can be properly replayed during recovery scenarios