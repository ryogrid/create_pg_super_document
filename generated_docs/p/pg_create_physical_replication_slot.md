# pg_create_physical_replication_slot

## Location
[src/backend/replication/slotfuncs.c:69-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slotfuncs.c#L69-L120)

## Overview
SQL function that creates a new physical (streaming replication) replication slot and returns slot name and optionally the restart LSN.

## Definition
```c
Datum pg_create_physical_replication_slot(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL SQL function creates a physical replication slot used for streaming replication. It validates permissions and requirements, creates the slot via the helper function, and returns a composite result containing the slot name and restart LSN (if immediately reserved). The function handles all necessary permission checks and ensures proper return format for SQL interface. After successful creation, it releases the slot for use by replication clients.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro with the following arguments:
  - Argument 0: `name` (Name) - Name of the replication slot to create
  - Argument 1: `immediately_reserve` (bool) - Whether to immediately reserve WAL space
  - Argument 2: `temporary` (bool) - Whether to create a temporary slot

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME
  - PG_GETARG_BOOL
  - [get_call_result_type](../g/get_call_result_type.md)
  - [CheckSlotPermissions](../C/CheckSlotPermissions.md)
  - [CheckSlotRequirements](../C/CheckSlotRequirements.md)
  - [create_physical_replication_slot](../c/create_physical_replication_slot.md)
  - [NameGetDatum](../N/NameGetDatum.md)
  - [LSNGetDatum](../L/LSNGetDatum.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md)
  - [ReplicationSlotRelease](../R/ReplicationSlotRelease.md)
  - PG_RETURN_DATUM
  - TYPEFUNC_COMPOSITE
- Called from:
  - This is a SQL-callable function (no direct C callers found)

## Notes and Other Information
- Returns a composite type (row) containing slot_name and lsn columns
- Performs comprehensive permission and requirement checks before slot creation
- Uses InvalidXLogRecPtr as restart_lsn parameter to the helper function
- Returns NULL for LSN column when immediately_reserve is false
- Automatically releases the slot after returning the result
- This function is exposed to SQL as pg_create_physical_replication_slot()

## Simplified Source

```c
Datum pg_create_physical_replication_slot(PG_FUNCTION_ARGS) {
    // Extract function arguments
    Name name = PG_GETARG_NAME(0);
    bool immediately_reserve = PG_GETARG_BOOL(1);
    bool temporary = PG_GETARG_BOOL(2);

    Datum values[2];
    bool nulls[2];
    TupleDesc tupdesc;

    // Validate return type is composite
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    // Check permissions and requirements
    CheckSlotPermissions();
    CheckSlotRequirements();

    // Create the physical replication slot
    create_physical_replication_slot(NameStr(*name),
                                     immediately_reserve,
                                     temporary,
                                     InvalidXLogRecPtr);

    // Build return tuple with slot name and LSN
    values[0] = NameGetDatum(&MyReplicationSlot->data.name);
    nulls[0] = false;

    if (immediately_reserve) {
        values[1] = LSNGetDatum(MyReplicationSlot->data.restart_lsn);
        nulls[1] = false;
    } else {
        nulls[1] = true;  // No LSN when not immediately reserved
    }

    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    Datum result = HeapTupleGetDatum(tuple);

    // Release the slot for replication use
    ReplicationSlotRelease();

    PG_RETURN_DATUM(result);
}
```