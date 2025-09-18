# XLogSetReplicationSlotMinimumLSN

## Location
[src/backend/access/transam/xlog.c:2665-2677](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L2665-L2677)

## Overview
Sets the minimum LSN required by all replication slots, indicating the earliest WAL position that must be retained for replication purposes.

## Definition
```c
void XLogSetReplicationSlotMinimumLSN(XLogRecPtr lsn)
```

## Detailed Description
XLogSetReplicationSlotMinimumLSN is a critical component of PostgreSQL's replication system that maintains the boundary for WAL retention. It records the lowest LSN that any active replication slot still requires, which directly impacts WAL cleanup decisions. This LSN acts as a protection barrier preventing the removal of WAL segments that are still needed by replication slaves or logical replication consumers. The function provides thread-safe updates to this shared boundary value.

## Parameters / Member Variables
- `lsn`: XLogRecPtr representing the minimum LSN position that must be preserved for replication slots

## Dependencies
- Functions called/Symbols referenced:
  - None (simple setter function with spinlock protection)
- Global variables used:
  - XLogCtl->replicationSlotMinLSN (shared replication slot minimum LSN)
  - XLogCtl->info_lck (spinlock for protecting shared control data)
- Called from (representative examples):
  - [ReplicationSlotsComputeRequiredLSN](../R/ReplicationSlotsComputeRequiredLSN.md) (in slot.c:1138)

## Notes and Other Information
- Essential for WAL retention policy enforcement in replication scenarios
- Works in conjunction with WAL cleanup processes to prevent premature WAL removal
- Thread-safe implementation using spinlocks for atomic updates to shared state
- The LSN value directly influences how much disk space WAL files consume
- Critical for preventing replication lag issues where slaves lose required WAL data
- Part of the broader replication slot management system that tracks replication consumer requirements
- Declared in src/include/access/xlog.h at line 214
- Simple but vital function - its proper functioning ensures replication reliability and prevents data loss scenarios