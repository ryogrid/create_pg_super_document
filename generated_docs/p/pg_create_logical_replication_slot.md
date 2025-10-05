# pg_create_logical_replication_slot

## Location
[src/backend/replication/slotfuncs.c:173-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slotfuncs.c#L173-L221)

## Overview
SQL function for creating a new logical replication slot with comprehensive configuration support and returns slot name and confirmed flush LSN.

## Definition
```c
Datum pg_create_logical_replication_slot(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL SQL function creates a logical replication slot for logical decoding with full configuration options including two-phase commit and failover support. It performs necessary permission and requirement checks specific to logical decoding, creates the slot using the helper function with automatic startpoint finding, and returns a composite result with slot name and confirmed flush LSN. For non-temporary slots, it marks the slot as persistent after successful creation, ensuring durability across server restarts.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro with the following arguments:
  - Argument 0: `name` (Name) - Name of the logical replication slot to create
  - Argument 1: `plugin` (Name) - Name of the logical decoding output plugin
  - Argument 2: `temporary` (bool) - Whether to create a temporary slot
  - Argument 3: `two_phase` (bool) - Whether to enable two-phase commit support
  - Argument 4: `failover` (bool) - Whether to enable failover support

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME
  - PG_GETARG_BOOL
  - [get_call_result_type](../g/get_call_result_type.md)
  - [CheckSlotPermissions](../C/CheckSlotPermissions.md)
  - [CheckLogicalDecodingRequirements](../C/CheckLogicalDecodingRequirements.md)
  - [create_logical_replication_slot](../c/create_logical_replication_slot.md)
  - [NameGetDatum](../N/NameGetDatum.md)
  - [LSNGetDatum](../L/LSNGetDatum.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md)
  - [ReplicationSlotPersist](../R/ReplicationSlotPersist.md)
  - [ReplicationSlotRelease](../R/ReplicationSlotRelease.md)
  - PG_RETURN_DATUM
  - TYPEFUNC_COMPOSITE
- Called from:
  - This is a SQL-callable function (no direct C callers found)

## Notes and Other Information
- Returns a composite type (row) containing slot_name and lsn columns
- Performs both general slot permissions and logical decoding-specific requirement checks
- Calls the helper function with find_startpoint=true to automatically determine the decoding start position
- Uses InvalidXLogRecPtr as restart_lsn parameter, letting the helper function find the appropriate start point
- For persistent slots, calls ReplicationSlotPersist() to convert from ephemeral to persistent state
- Always returns the confirmed_flush LSN as the second column in the result
- This function is exposed to SQL as pg_create_logical_replication_slot()
- Supports advanced features like two-phase commit and failover capabilities for modern replication scenarios

## Simplified Source

```c
Datum pg_create_logical_replication_slot(PG_FUNCTION_ARGS) {
    // Extract function arguments
    Name name = PG_GETARG_NAME(0);
    Name plugin = PG_GETARG_NAME(1);
    bool temporary = PG_GETARG_BOOL(2);
    bool two_phase = PG_GETARG_BOOL(3);
    bool failover = PG_GETARG_BOOL(4);

    TupleDesc tupdesc;
    Datum values[2];
    bool nulls[2];

    // Validate return type is composite
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    // Check permissions and logical decoding requirements
    CheckSlotPermissions();
    CheckLogicalDecodingRequirements();

    // Create the logical replication slot with automatic startpoint finding
    create_logical_replication_slot(NameStr(*name),
                                    NameStr(*plugin),
                                    temporary,
                                    two_phase,
                                    failover,
                                    InvalidXLogRecPtr,
                                    true);  // find_startpoint = true

    // Build return tuple with slot name and confirmed flush LSN
    values[0] = NameGetDatum(&MyReplicationSlot->data.name);
    values[1] = LSNGetDatum(MyReplicationSlot->data.confirmed_flush);
    memset(nulls, 0, sizeof(nulls));

    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    Datum result = HeapTupleGetDatum(tuple);

    // Make slot persistent if not temporary
    if (!temporary)
        ReplicationSlotPersist();

    // Release the slot for replication use
    ReplicationSlotRelease();

    PG_RETURN_DATUM(result);
}
```