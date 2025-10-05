# pg_replication_slot_advance

## Location
[src/backend/replication/slotfuncs.c:508-600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slotfuncs.c#L508-L600)

## Overview
SQL function that moves the position of a replication slot (both physical and logical) to a specified WAL LSN position, returning the slot name and actual position reached.

## Definition
```c
Datum pg_replication_slot_advance(PG_FUNCTION_ARGS)
```

## Detailed Description
This is the main SQL-callable function for advancing replication slots in PostgreSQL. It provides a unified interface for advancing both physical and logical replication slots while performing comprehensive validation and safety checks.

The function performs the following key operations:
1. **Input validation**: Validates the target LSN and slot permissions
2. **Position clamping**: Ensures the target position doesn't exceed what's been flushed (in normal operation) or replayed (during recovery)
3. **Slot acquisition**: Acquires exclusive access to the specified slot
4. **Backward movement prevention**: Prevents moving the slot to a position earlier than its current minimum viable position
5. **Type-specific advancement**: Calls the appropriate helper function based on whether it's a logical or physical slot
6. **Global state updates**: Recomputes required LSN and xmin across all slots
7. **Result formatting**: Returns a composite type with slot name and final position

The function distinguishes between logical and physical slots: logical slots use confirmed_flush as the minimum position while physical slots use restart_lsn.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL function arguments containing:
  - Slot name (Name/text): The name of the replication slot to advance
  - Target LSN (XLogRecPtr/pg_lsn): The WAL position to advance the slot to

## Dependencies
- Functions called/Symbols referenced:
  - [CheckSlotPermissions](../C/CheckSlotPermissions.md) - Validates user permissions for slot operations
  - [ReplicationSlotAcquire](../R/ReplicationSlotAcquire.md) - Acquires exclusive access to the slot
  - [GetFlushRecPtr](../G/GetFlushRecPtr.md) - Gets the current WAL flush position
  - [GetXLogReplayRecPtr](../G/GetXLogReplayRecPtr.md) - Gets the current WAL replay position during recovery
  - [pg_logical_replication_slot_advance](pg_logical_replication_slot_advance.md) - Advances logical replication slots
  - [pg_physical_replication_slot_advance](pg_physical_replication_slot_advance.md) - Advances physical replication slots
  - [ReplicationSlotsComputeRequiredXmin](../R/ReplicationSlotsComputeRequiredXmin.md) - Recomputes global minimum xmin across all slots
  - [ReplicationSlotsComputeRequiredLSN](../R/ReplicationSlotsComputeRequiredLSN.md) - Recomputes global minimum LSN across all slots
  - [ReplicationSlotRelease](../R/ReplicationSlotRelease.md) - Releases the acquired slot
- Called from:
  - SQL interface - directly callable as pg_replication_slot_advance(slot_name, lsn)

## Notes and Other Information
- Returns a composite type (slot_name, end_lsn) showing the final position reached
- Cannot advance a slot that has never reserved WAL or has been invalidated
- Automatically clamps the target position to prevent advancing beyond available WAL
- Logical slots (database != InvalidOid) use confirmed_flush as minimum position
- Physical slots use restart_lsn as minimum position
- Updates global slot state after advancement to maintain cluster consistency
- Requires appropriate permissions to execute slot operations
- Thread-safe through slot acquisition mechanism

## Simplified Source

```c
Datum pg_replication_slot_advance(PG_FUNCTION_ARGS) {
    // Extract function arguments
    Name slotname = PG_GETARG_NAME(0);
    XLogRecPtr moveto = PG_GETARG_LSN(1);
    XLogRecPtr endlsn, minlsn;
    TupleDesc tupdesc;
    Datum values[2];
    bool nulls[2];

    Assert(!MyReplicationSlot);

    // Validate permissions and target LSN
    CheckSlotPermissions();
    if (XLogRecPtrIsInvalid(moveto))
        ereport(ERROR, (errmsg("invalid target WAL LSN")));

    // Validate return type
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    // Clamp target position to available WAL
    if (!RecoveryInProgress())
        moveto = Min(moveto, GetFlushRecPtr(NULL));
    else
        moveto = Min(moveto, GetXLogReplayRecPtr(NULL));

    // Acquire exclusive access to the slot
    ReplicationSlotAcquire(NameStr(*slotname), true);

    // Ensure slot can be advanced (has reserved WAL)
    if (XLogRecPtrIsInvalid(MyReplicationSlot->data.restart_lsn))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("replication slot \"%s\" cannot be advanced",
                               NameStr(*slotname))));

    // Determine minimum position based on slot type
    if (OidIsValid(MyReplicationSlot->data.database))
        minlsn = MyReplicationSlot->data.confirmed_flush;  // Logical slot
    else
        minlsn = MyReplicationSlot->data.restart_lsn;      // Physical slot

    // Prevent backward movement
    if (moveto < minlsn)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot advance replication slot to %X/%X, minimum is %X/%X",
                               LSN_FORMAT_ARGS(moveto), LSN_FORMAT_ARGS(minlsn))));

    // Advance slot based on type
    if (OidIsValid(MyReplicationSlot->data.database))
        endlsn = pg_logical_replication_slot_advance(moveto);
    else
        endlsn = pg_physical_replication_slot_advance(moveto);

    // Update global slot state
    ReplicationSlotsComputeRequiredXmin(false);
    ReplicationSlotsComputeRequiredLSN();

    // Build result tuple
    values[0] = NameGetDatum(&MyReplicationSlot->data.name);
    values[1] = LSNGetDatum(endlsn);
    nulls[0] = nulls[1] = false;

    ReplicationSlotRelease();

    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
```