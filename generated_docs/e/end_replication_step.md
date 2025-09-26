# end_replication_step

## Location
[src/backend/replication/logical/worker.c:533-560](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L533-L560)

## Overview
Completes a single replication step by cleaning up the snapshot context and making the step's effects visible within the current transaction.

## Definition
```c
static void end_replication_step(void)
```

## Detailed Description
This function performs the necessary cleanup operations to finalize a single replication step that was initiated by begin_replication_step(). It handles two critical tasks:

1. Removes the active snapshot that was established for the replication step, restoring the previous snapshot state
2. Increments the command counter to make the effects of the current step visible to subsequent operations within the same transaction

The function is designed as a lightweight cleanup mechanism that maintains transaction boundaries - it does not commit or abort the transaction, allowing multiple replication steps to be batched within a single transaction for efficiency.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md) (removes the current active snapshot from the stack)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md) (increments the command counter to make changes visible)
- Called from (representative examples):
  - [apply_handle_insert](../a/apply_handle_insert.md) (after INSERT operation processing)
  - [apply_handle_update](../a/apply_handle_update.md) (after UPDATE operation processing) 
  - [apply_handle_delete](../a/apply_handle_delete.md) (after DELETE operation processing)
  - [apply_handle_truncate](../a/apply_handle_truncate.md) (after TRUNCATE operation processing)
  - [apply_spooled_messages](../a/apply_spooled_messages.md) (after processing queued messages)
  - [stream_start_internal](../s/stream_start_internal.md) (after streaming transaction setup)

## Notes and Other Information
- Must be called by every caller of begin_replication_step() for proper cleanup
- Does not close the transaction - allows multiple steps to be batched efficiently
- [Command](../C/Command.md) counter increment ensures that changes from this step are visible to subsequent steps
- Part of the snapshot management infrastructure that ensures consistent data visibility
- The function is intentionally lightweight to minimize overhead in high-throughput replication scenarios
- Proper pairing with begin_replication_step() is essential for maintaining snapshot stack integrity