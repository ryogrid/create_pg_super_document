# CheckSlotRequirements

## Location
src/backend/replication/slot.c: 1362 - 1383

## Overview
Validates that the server's configuration meets the requirements for using replication slots by checking essential configuration parameters.

## Definition
```c
void CheckSlotRequirements(void)
```

## Detailed Description
This function performs prerequisite checks to ensure that the PostgreSQL server is properly configured to support replication slots. It validates two critical configuration requirements:

1. **max_replication_slots > 0**: Ensures that replication slots are enabled at the server level
2. **wal_level >= replica**: Ensures that WAL logging is sufficient for replication purposes

If either requirement is not met, the function raises an ERROR with an appropriate error message explaining the configuration issue. The function includes a note that any new requirements added here should likely also be checked in RestoreSlotFromDisk().

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ereport/ERROR (error reporting)
  - [errcode](../e/errcode.md) (ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
  - [errmsg](../e/errmsg.md) (error message formatting)
  - WAL_LEVEL_REPLICA (WAL level constant)
  - max_replication_slots (GUC variable)
  - wal_level (GUC variable)

- Called from (representative examples):
  - CheckLogicalDecodingRequirements
  - [pg_create_physical_replication_slot](../p/pg_create_physical_replication_slot.md)
  - [pg_drop_replication_slot](../p/pg_drop_replication_slot.md)
  - [copy_replication_slot](../c/copy_replication_slot.md)

## Notes and Other Information
- This is a validation function that errors out rather than returning a boolean result
- The comment specifically notes that RestoreSlotFromDisk() should be updated when new requirements are added
- Uses the standard PostgreSQL error reporting mechanism with specific error codes
- Serves as a central validation point for replication slot prerequisites
- Called by both logical and physical replication slot functions
- Essential for preventing slot operations when the server is not properly configured for replication