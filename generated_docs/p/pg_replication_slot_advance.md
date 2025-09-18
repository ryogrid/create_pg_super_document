# pg_replication_slot_advance

## Location
src/backend/replication/slotfuncs.c: 508 - 600

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
  - `CheckSlotPermissions` - Validates user permissions for slot operations
  - `ReplicationSlotAcquire` - Acquires exclusive access to the slot
  - `GetFlushRecPtr` - Gets the current WAL flush position
  - `GetXLogReplayRecPtr` - Gets the current WAL replay position during recovery
  - `pg_logical_replication_slot_advance` - Advances logical replication slots
  - `pg_physical_replication_slot_advance` - Advances physical replication slots
  - `ReplicationSlotsComputeRequiredXmin` - Recomputes global minimum xmin across all slots
  - `ReplicationSlotsComputeRequiredLSN` - Recomputes global minimum LSN across all slots
  - `ReplicationSlotRelease` - Releases the acquired slot
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