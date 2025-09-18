# pg_create_logical_replication_slot

## Location
src/backend/replication/slotfuncs.c: 173 - 221

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
  - get_call_result_type
  - CheckSlotPermissions
  - CheckLogicalDecodingRequirements
  - create_logical_replication_slot
  - NameGetDatum
  - LSNGetDatum
  - heap_form_tuple
  - HeapTupleGetDatum
  - ReplicationSlotPersist
  - ReplicationSlotRelease
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