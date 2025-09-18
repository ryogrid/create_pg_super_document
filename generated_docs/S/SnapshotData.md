# SnapshotData

## Location
src/include/utils/snapshot.h: 142 - 217

## Overview
SnapshotData is the core structure representing all kinds of snapshots in PostgreSQL's MVCC system, containing transaction visibility information and metadata for different snapshot types.

## Definition
```c
typedef struct SnapshotData
{
    SnapshotType snapshot_type;
    TransactionId xmin;
    TransactionId xmax;
    TransactionId *xip;
    uint32 xcnt;
    TransactionId *subxip;
    int32 subxcnt;
    bool suboverflowed;
    bool takenDuringRecovery;
    bool copied;
    CommandId curcid;
    uint32 speculativeToken;
    struct GlobalVisState *vistest;
    uint32 active_count;
    uint32 regd_count;
    pairingheap_node ph_node;
    TimestampTz whenTaken;
    XLogRecPtr lsn;
    uint64 snapXactCompletionCount;
} SnapshotData;
```

## Detailed Description
SnapshotData represents all kinds of possible snapshots including normal MVCC snapshots, recovery snapshots in Hot-Standby mode, historic snapshots for logical decoding, and special-purpose snapshots. The structure contains transaction ID ranges and arrays to determine tuple visibility according to MVCC rules. Different snapshot types use different subsets of the fields, with most fields being relevant only for MVCC snapshots.

## Parameters / Member Variables
- `snapshot_type`: Type of snapshot (SnapshotType enum value)
- `xmin`: All transaction IDs less than this are visible
- `xmax`: All transaction IDs greater than or equal to this are invisible
- `xip`: Array of transaction IDs that are in-progress (normal MVCC) or committed (historic MVCC)
- `xcnt`: Number of transaction IDs in xip array
- `subxip`: Array of subtransaction IDs that are in-progress or all xids for replayed transactions
- `subxcnt`: Number of transaction IDs in subxip array
- `suboverflowed`: Whether the subxip array has overflowed
- `takenDuringRecovery`: Whether this is a recovery-shaped snapshot
- `copied`: Whether this is a copied snapshot (false for static snapshots)
- `curcid`: Current command ID for visibility within the current transaction
- `speculativeToken`: Token for speculative insertions (used by HeapTupleSatisfiesDirty)
- `vistest`: Global visibility state for non-vacuumable snapshots
- `active_count`: Reference count on ActiveSnapshot stack
- `regd_count`: Reference count on RegisteredSnapshots
- `ph_node`: Link in the RegisteredSnapshots pairing heap
- `whenTaken`: Timestamp when snapshot was taken
- `lsn`: WAL position when snapshot was taken
- `snapXactCompletionCount`: Transaction completion count for optimization

## Dependencies
- Functions called/Symbols referenced:
  - SnapshotType
  - TransactionId
  - CommandId
  - GlobalVisState
  - pairingheap_node
  - TimestampTz
  - XLogRecPtr
- Called from (representative examples):
  - CopySnapshot
  - GetOldestSnapshot
  - ImportSnapshot
  - Various heap access methods

## Notes and Other Information
The structure is designed to handle multiple snapshot types with different semantics. A TODO comment suggests splitting this into separate structures using NodeTag similar to parser/executor nodes to avoid field overloading. The structure supports both normal operation and recovery scenarios, with specific optimizations for avoiding redundant work when no transactions have completed.