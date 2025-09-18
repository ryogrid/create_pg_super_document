# create_physical_replication_slot

## Location
[src/backend/replication/slotfuncs.c:40-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slotfuncs.c#L40-L68)

## Overview
A static helper function for creating a new physical replication slot with given arguments without releasing the created slot afterward.

## Definition
```c
static void create_physical_replication_slot(char *name, bool immediately_reserve, 
                                           bool temporary, XLogRecPtr restart_lsn)
```

## Detailed Description
This function creates a new physical replication slot used for streaming replication. It handles the creation process by calling ReplicationSlotCreate() with appropriate parameters and optionally reserving WAL space if requested. The function is designed as a helper and doesn't perform slot cleanup - the caller is responsible for releasing the slot. If a valid restart_lsn is provided, it uses that value directly without WAL reservation, requiring the caller to guarantee WAL availability.

## Parameters
- `name`: Name of the replication slot to create
- `immediately_reserve`: Whether to immediately reserve WAL space for the slot  
- `temporary`: Whether to create a temporary slot (RS_TEMPORARY) or persistent slot (RS_PERSISTENT)
- `restart_lsn`: Starting LSN position for the slot; if valid, used without WAL reservation

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotCreate](../R/ReplicationSlotCreate.md)
  - XLogRecPtrIsInvalid
  - [ReplicationSlotReserveWal](../R/ReplicationSlotReserveWal.md)
  - [ReplicationSlotMarkDirty](../R/ReplicationSlotMarkDirty.md)
  - [ReplicationSlotSave](../R/ReplicationSlotSave.md)
  - RS_PERSISTENT
  - RS_TEMPORARY
- Called from (representative examples):
  - [pg_create_physical_replication_slot](../p/pg_create_physical_replication_slot.md)
  - [copy_replication_slot](copy_replication_slot.md)

## Notes and Other Information
- Asserts that MyReplicationSlot is NULL before execution
- When immediately_reserve is true and restart_lsn is invalid, it calls ReplicationSlotReserveWal() to reserve WAL
- When immediately_reserve is true, the slot is marked dirty and saved to disk
- The function doesn't release the created slot - this responsibility lies with the caller
- Used internally by the SQL function pg_create_physical_replication_slot and slot copying functionality