# pg_drop_replication_slot

## Location
src/backend/replication/slotfuncs.c: 222 - 239

## Overview
SQL function for dropping an existing replication slot by name with proper permission validation.

## Definition
```c
Datum pg_drop_replication_slot(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL SQL function provides the interface to drop replication slots from SQL. It performs necessary permission and requirement checks before calling the internal ReplicationSlotDrop function to remove the specified slot. The function handles both physical and logical replication slots uniformly, ensuring proper cleanup and validation. It's designed as a simple wrapper around the core slot deletion functionality with appropriate access control.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro with the following arguments:
  - Argument 0: `name` (Name) - Name of the replication slot to drop

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME
  - [CheckSlotPermissions](../C/CheckSlotPermissions.md)
  - [CheckSlotRequirements](../C/CheckSlotRequirements.md)
  - [ReplicationSlotDrop](../R/ReplicationSlotDrop.md)
  - PG_RETURN_VOID
- Called from:
  - This is a SQL-callable function (no direct C callers found)

## Notes and Other Information
- Returns void (no return value)
- Performs comprehensive permission checks before attempting slot deletion
- Uses CheckSlotRequirements() to validate general slot operation prerequisites
- Calls ReplicationSlotDrop() with force=true parameter to ensure slot removal
- Works with both physical and logical replication slots
- This function is exposed to SQL as pg_drop_replication_slot()
- The actual slot deletion logic is handled by ReplicationSlotDrop() in the replication slot management subsystem
- Simple and straightforward interface with minimal parameters - just the slot name to drop