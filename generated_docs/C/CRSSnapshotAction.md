# CRSSnapshotAction

## Location
[src/include/replication/walsender.h:25-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/walsender.h#L25-L58)

## Overview
An enumeration that defines actions to be taken with snapshots when creating replication slots in PostgreSQL's logical replication system.

## Definition
```c
typedef enum
{
    CRS_EXPORT_SNAPSHOT,
    CRS_NOEXPORT_SNAPSHOT,
    CRS_USE_SNAPSHOT,
} CRSSnapshotAction;
```

## Detailed Description
CRSSnapshotAction is an enumeration type that specifies different strategies for handling transaction snapshots during the creation of replication slots. This is particularly important for logical replication where consistent snapshots are needed to establish the initial state for replication. The enum provides three distinct approaches for snapshot management, each suited for different replication scenarios and requirements.

The enumeration is used primarily in the context of the CREATE_REPLICATION_SLOT command and related functions to control how PostgreSQL handles snapshot export and usage during the slot creation process.

## Parameters / Member Variables
- `CRS_EXPORT_SNAPSHOT`: Export a new snapshot that can be used to establish consistent initial state for logical replication
- `CRS_NOEXPORT_SNAPSHOT`: Do not export a snapshot during replication slot creation
- `CRS_USE_SNAPSHOT`: Use an existing snapshot instead of creating/exporting a new one

## Dependencies
- Functions called/Symbols referenced:
  - [exec_replication_command](../e/exec_replication_command.md) (replication command execution)
  - [WalSndErrorCleanup](../W/WalSndErrorCleanup.md) (error cleanup for WAL senders)
  - [WalSndResourceCleanup](../W/WalSndResourceCleanup.md) (resource cleanup for WAL senders)
  - [PhysicalWakeupLogicalWalSnd](../P/PhysicalWakeupLogicalWalSnd.md) (wakeup logical WAL senders)
  - [GetStandbyFlushRecPtr](../G/GetStandbyFlushRecPtr.md) (get standby flush position)
  - [WalSndSignals](../W/WalSndSignals.md) (WAL sender signal handling)
  - [WalSndShmemSize](../W/WalSndShmemSize.md) (shared memory size calculation)
  - [WalSndShmemInit](../W/WalSndShmemInit.md) (shared memory initialization)
  - [WalSndWakeup](../W/WalSndWakeup.md) (wakeup WAL senders)
  - [WalSndInitStopping](../W/WalSndInitStopping.md) (initialize stopping procedure)
  - [WalSndWaitStopping](../W/WalSndWaitStopping.md) (wait for stopping)
  - [HandleWalSndInitStopping](../H/HandleWalSndInitStopping.md) (handle stopping initialization)
  - [WalSndRqstFileReload](../W/WalSndRqstFileReload.md) (request file reload)
- Used by:
  - [WalReceiverConn](../W/WalReceiverConn.md) (WAL receiver connection structure)
  - [libpqrcv_create_slot](../l/libpqrcv_create_slot.md) (libpq create slot function)
  - [parseCreateReplSlotOptions](../p/parseCreateReplSlotOptions.md) (parse replication slot options)
  - [CreateReplicationSlot](CreateReplicationSlot.md) (create replication slot function)

## Notes and Other Information
- This enumeration is central to PostgreSQL's logical replication infrastructure
- [Snapshot](../S/Snapshot.md) handling is crucial for maintaining data consistency in logical replication
- The choice of snapshot action affects the initial synchronization process between master and replica
- CRS_EXPORT_SNAPSHOT is typically used when setting up new logical replication subscribers
- CRS_USE_SNAPSHOT allows reusing existing snapshots for efficiency
- CRS_NOEXPORT_SNAPSHOT is used when snapshot export is not needed or handled elsewhere
- The enum is defined in the WAL sender header as it's closely tied to replication slot management