# pg_logical_replication_slot_advance

## Location
src/backend/replication/slotfuncs.c: 499 - 507

## Overview
A helper function that advances a logical replication slot forward by delegating to the logical slot advancement and snapshot state checking functionality.

## Definition
```c
static XLogRecPtr pg_logical_replication_slot_advance(XLogRecPtr moveto)
```

## Detailed Description
This function serves as a simple wrapper around LogicalSlotAdvanceAndCheckSnapState for advancing logical replication slots. It provides a consistent interface for logical slot advancement that matches the physical slot advancement function signature. The actual advancement logic, including snapshot state validation and consistency checks, is handled by the underlying LogicalSlotAdvanceAndCheckSnapState function.

Unlike physical slots which only need to update restart_lsn, logical slots require more complex processing to maintain consistency of the logical decoding state, which is why this function delegates to specialized logical slot handling code.

## Parameters / Member Variables
- `moveto`: The target WAL LSN position to advance the logical slot to

## Dependencies
- Functions called/Symbols referenced:
  - `LogicalSlotAdvanceAndCheckSnapState` - Performs the actual logical slot advancement with snapshot state validation
- Called from:
  - `pg_replication_slot_advance` - Main SQL function for advancing replication slots

## Notes and Other Information
- This is a static helper function, not directly accessible from SQL
- Provides a uniform interface for logical slot advancement within the slot management system
- The NULL parameter passed to LogicalSlotAdvanceAndCheckSnapState indicates no additional snapshot state checking is required
- Logical slots have more complex advancement requirements than physical slots due to the need to maintain logical decoding consistency
- The function returns the actual LSN position reached after advancement