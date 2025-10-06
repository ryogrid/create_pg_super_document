# binary_upgrade_logical_slot_has_caught_up

## Location
[src/backend/utils/adt/pg_upgrade_support.c:285-324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_upgrade_support.c#L285-L324)

## Overview
Verifies that a logical replication slot has consumed all available WAL changes and is ready for binary upgrade without data loss.

## Definition
```c
Datum binary_upgrade_logical_slot_has_caught_up(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a specialized utility for PostgreSQL binary upgrades that ensures logical replication slots are in a safe state before proceeding with the upgrade. It verifies that the specified logical replication slot has already consumed all decodable WAL (Write-Ahead Log) records up to the current end of WAL.

The function performs several critical checks:
1. Ensures the function is called only during binary upgrade operations
2. Verifies the user has replication permissions (required for binary upgrades)
3. Acquires and validates the specified replication slot
4. Checks if there are pending WAL records after the slot's confirmed_flush_lsn
5. Returns true if the slot has caught up (no pending WAL), false otherwise

This verification prevents data loss during binary upgrades by ensuring replication slots are synchronized with the current WAL state.

## Parameters / Member Variables
- `slot_name (Name)`: The name of the logical replication slot to check

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_IS_BINARY_UPGRADE
  - [has_rolreplication](../h/has_rolreplication.md)
  - [GetUserId](../G/GetUserId.md)
  - PG_GETARG_NAME
  - [ReplicationSlotAcquire](../R/ReplicationSlotAcquire.md)
  - SlotIsLogical
  - [GetFlushRecPtr](../G/GetFlushRecPtr.md)
  - [LogicalReplicationSlotHasPendingWal](../L/LogicalReplicationSlotHasPendingWal.md)
  - [ReplicationSlotRelease](../R/ReplicationSlotRelease.md)
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct callers found (likely called via SQL during binary upgrades)

## Notes and Other Information
- This is a special-purpose function designed specifically for binary upgrade scenarios
- Only users with replication privileges can execute this function during binary upgrades
- The function includes assertions to ensure slot validity and logical slot type
- Returns false if pending WAL exists, true if the slot has fully caught up
- Located in src/backend/utils/adt/pg_upgrade_support.c:285-324
- Critical for preventing data loss during binary upgrades with logical replication

## Simplified Source

```c
Datum
binary_upgrade_logical_slot_has_caught_up(PG_FUNCTION_ARGS)
{
    Name slot_name;
    XLogRecPtr end_of_wal;
    bool has_pending_wal;

    // Ensure function runs only during binary upgrade
    CHECK_IS_BINARY_UPGRADE;

    // Binary upgrades require superuser permissions for replication slots
    Assert(has_rolreplication(GetUserId()));

    // Get slot name from arguments and acquire the slot
    slot_name = PG_GETARG_NAME(0);
    ReplicationSlotAcquire(NameStr(*slot_name), true);

    // Verify this is a logical slot and it's valid
    Assert(SlotIsLogical(MyReplicationSlot));
    Assert(MyReplicationSlot->data.invalidated == RS_INVAL_NONE);

    // Check if slot has consumed all WAL up to current end
    end_of_wal = GetFlushRecPtr(NULL);
    has_pending_wal = LogicalReplicationSlotHasPendingWal(end_of_wal);

    // Release slot and return whether slot has caught up
    ReplicationSlotRelease();
    PG_RETURN_BOOL(!has_pending_wal);
}
```