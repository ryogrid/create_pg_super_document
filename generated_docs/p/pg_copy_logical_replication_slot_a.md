# pg_copy_logical_replication_slot_a

## Location
src/backend/replication/slotfuncs.c: 858 - 863

## Overview
SQL wrapper function that provides the basic 2-parameter interface for copying logical replication slots (source name and destination name only).

## Definition
```c
Datum pg_copy_logical_replication_slot_a(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a SQL-callable wrapper around the copy_replication_slot helper function, specifically configured for logical replication slots. It provides the simplest interface for copying logical slots, accepting only the essential parameters: source slot name and destination slot name.

The function delegates all actual copying logic to copy_replication_slot with the logical_slot parameter set to true, ensuring that logical replication slot semantics are enforced throughout the copy operation.

This is one of several wrapper functions created to satisfy PostgreSQL's opr_sanity checks, which require specific function signatures for SQL-callable functions with different parameter counts.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL function arguments containing:
  - Source slot name (Name/text): The name of the existing logical replication slot to copy from
  - Destination slot name (Name/text): The name for the new logical replication slot

## Dependencies
- Functions called/Symbols referenced:
  - `[copy_replication_slot](../c/copy_replication_slot.md)` - The main slot copying implementation, called with logical_slot=true
- Called from:
  - SQL interface - directly callable as pg_copy_logical_replication_slot(src_slot_name, dst_slot_name)

## Notes and Other Information
- Returns a composite type (slot_name, lsn) containing the destination slot name and its confirmed flush LSN
- Uses default values for optional parameters (temporary=false, inherits plugin from source)
- Part of a family of wrapper functions with different parameter counts:
  - [pg_copy_logical_replication_slot_a](pg_copy_logical_replication_slot_a.md) (2 params) - this function
  - [pg_copy_logical_replication_slot_b](pg_copy_logical_replication_slot_b.md) (3 params) - adds temporary parameter
  - [pg_copy_logical_replication_slot_c](pg_copy_logical_replication_slot_c.md) (4 params) - adds plugin parameter
- Requires appropriate permissions for logical replication slot operations
- Source slot must be a logical slot; attempting to copy a physical slot will result in an error
- The copied slot will inherit the source slot's plugin configuration
- The copied slot will be persistent (non-temporary) by default