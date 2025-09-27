# XLogGetReplicationSlotMinimumLSN

## Location
[src/backend/access/transam/xlog.c:2678-2698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L2678-L2698)

## Overview
Retrieves the minimum LSN that must be retained to satisfy all active replication slot requirements, serving as the boundary for safe WAL cleanup operations.

## Definition
```c
static XLogRecPtr XLogGetReplicationSlotMinimumLSN(void)
```

## Detailed Description
XLogGetReplicationSlotMinimumLSN is a getter function that provides thread-safe access to the shared minimum replication slot LSN. This value represents the earliest WAL position that any replication slot still requires, making it a critical input for WAL cleanup decisions, checkpoint operations, and WAL availability assessments. The function ensures consistent reads of this shared state through proper spinlock protection.

## Parameters / Member Variables
- Returns: XLogRecPtr representing the minimum LSN required by replication slots

## Dependencies
- Functions called/Symbols referenced:
  - None (simple getter function with spinlock protection)
- Global variables used:
  - XLogCtl->replicationSlotMinLSN (shared replication slot minimum LSN)
  - XLogCtl->info_lck (spinlock for protecting shared control data)
- Called from (representative examples):
  - RefreshXLogWriteResult (in xlog.c:672)
  - [CreateCheckPoint](../C/CreateCheckPoint.md) (in xlog.c:7112, 7316)
  - [CreateRestartPoint](../C/CreateRestartPoint.md) (in xlog.c:7687, 7787)
  - [GetWALAvailability](../G/GetWALAvailability.md) (in xlog.c:7904)

## Notes and Other Information
- Static function (internal to xlog.c) - not part of the public API
- Provides consistent, atomic reads of the replication slot minimum LSN
- Essential component of WAL retention logic and cleanup decision-making
- Used by critical recovery and checkpointing operations to ensure WAL availability
- The returned LSN directly influences disk space usage and WAL segment lifecycle
- Complementary function to XLogSetReplicationSlotMinimumLSN for complete get/set access pattern
- Thread-safe implementation prevents race conditions when reading shared state
- Key input for functions that determine WAL availability and cleanup boundaries

## Simplified Source

```c
// Simplified version of XLogGetReplicationSlotMinimumLSN
static XLogRecPtr XLogGetReplicationSlotMinimumLSN(void) {
    XLogRecPtr minimum_lsn;

    // Thread-safe read of shared replication slot minimum LSN
    SpinLockAcquire(&XLogCtl->info_lck);
    minimum_lsn = XLogCtl->replicationSlotMinLSN;
    SpinLockRelease(&XLogCtl->info_lck);

    return minimum_lsn;
}
```

Key simplifications made:
- Renamed variable from `retval` to `minimum_lsn` for clarity
- Added descriptive comment explaining the thread-safe read operation
- Maintained the essential spinlock protection pattern
- Preserved the core functionality of atomically reading shared state